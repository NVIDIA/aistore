#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#

from datetime import datetime, timedelta
from typing import List, Optional
import time
from dateutil.parser import isoparse
from aistore.sdk.bucket import Bucket
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_PUT,
    QPARAM_WHAT,
    QPARAM_FORCE,
    DEFAULT_JOB_WAIT_TIMEOUT,
    WHAT_ONE_XACT_STATUS,
    URL_PATH_CLUSTER,
    ACT_START,
    WHAT_QUERY_XACT_STATS,
)
from aistore.sdk.errors import Timeout, JobInfoNotFound
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import (
    JobStatus,
    JobArgs,
    ActionMsg,
    JobSnap,
    BucketModel,
    AggregatedJobSnap,
)
from aistore.sdk.utils import probing_frequency, get_logger

logger = get_logger(__name__)


class Job:
    """
    A class containing job-related functions.

    Args:
        client (RequestClient): Client for interfacing with AIS cluster
        job_id (str, optional): ID of a specific job, empty for all jobs
        job_kind (str, optional): Specific kind of job, empty for all kinds
    """

    def __init__(self, client: RequestClient, job_id: str = "", job_kind: str = ""):
        self._client = client
        self._job_id = job_id
        self._job_kind = job_kind

    @property
    def job_id(self):
        """
        Return job id
        """
        return self._job_id

    @property
    def job_kind(self):
        """
        Return job kind
        """
        return self._job_kind

    def status(
        self,
    ) -> JobStatus:
        """
        Return status of a job

        Returns:
            The job status including id, finish time, and error info.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        if not self._job_id:
            raise ValueError("Cannot query status on a job without an assigned ID")
        return self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=JobStatus,
            json=JobArgs(id=self._job_id, kind=self._job_kind).as_dict(),
            params={QPARAM_WHAT: WHAT_ONE_XACT_STATUS},
        )

    def wait(
        self,
        timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
        verbose: bool = True,
    ):
        """
        Wait for a job to finish

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

        Returns:
            None

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
        """
        logger.disabled = not verbose
        passed = 0
        sleep_time = probing_frequency(timeout)
        while True:
            if passed > timeout:
                raise Timeout("job to finish")
            status = self.status()
            if status.end_time == 0:
                time.sleep(sleep_time)
                passed += sleep_time
                logger.info("Waiting on job '%s'...", status.uuid)
                continue
            end_time = datetime.fromtimestamp(status.end_time / 1e9).time()
            if status.err:
                logger.error(
                    "Job '%s' failed at time '%s' with error: %s",
                    status.uuid,
                    end_time,
                    status.err,
                )
            elif status.aborted:
                logger.error("Job '%s' aborted at time '%s'", status.uuid, end_time)
            else:
                logger.info("Job '%s' finished at time '%s'", status.uuid, end_time)
            break

    def wait_for_idle(
        self,
        timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
        verbose: bool = True,
    ):
        """
        Wait for a job to reach an idle state

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

        Returns:
            None

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
            errors.JobInfoNotFound: Raised when information on a job's status could not be found on the AIS cluster
        """
        action = f"job '{self._job_id}' to reach idle state"
        logger.disabled = not verbose
        self._wait_for_condition(self._check_job_idle, action, timeout)

    def wait_single_node(
        self,
        timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
        verbose: bool = True,
    ):
        """
        Wait for a job running on a single node

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

        Returns:
            None

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
            errors.JobInfoNotFound: Raised when information on a job's status could not be found on the AIS cluster
        """
        action = f"job '{self._job_id}' to finish"
        logger.disabled = not verbose
        self._wait_for_condition(self._check_snapshot_finished, action, timeout)

    def start(
        self,
        daemon_id: str = "",
        force: bool = False,
        buckets: List[Bucket] = None,
    ) -> str:
        """
        Start a job and return its ID.

        Args:
            daemon_id (str, optional): For running a job that must run on a specific target node (e.g. resilvering).
            force (bool, optional): Override existing restrictions for a bucket (e.g., run LRU eviction even if the
                bucket has LRU disabled).
            buckets (List[Bucket], optional): List of one or more buckets; applicable only for jobs that have bucket
                scope (for details on job types, see `Table` in xact/api.go).

        Returns:
            The running job ID.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        job_args = JobArgs(kind=self._job_kind, daemon_id=daemon_id)
        if buckets and len(buckets) > 0:
            bucket_models = [
                BucketModel(
                    name=bck.name, provider=bck.provider.value, namespace=bck.namespace
                )
                for bck in buckets
            ]
            if len(bucket_models) == 1:
                job_args.bucket = bucket_models[0]
            else:
                job_args.buckets = bucket_models
        params = {QPARAM_FORCE: "true"} if force else {}
        action = ActionMsg(action=ACT_START, value=job_args.as_dict()).dict()

        resp = self._client.request(
            HTTP_METHOD_PUT, path=URL_PATH_CLUSTER, json=action, params=params
        )
        return resp.text

    def get_within_timeframe(
        self, start_time: datetime, end_time: Optional[datetime] = None
    ) -> List[JobSnap]:
        """
        Retrieves jobs that started after a specified start_time and optionally ended before a specified end_time.

        Args:
            start_time (datetime): The start of the timeframe for monitoring jobs.
            end_time (datetime, optional): The end of the timeframe for monitoring jobs.

        Returns:
            List[JobSnapshot]: A list of jobs that meet the specified timeframe criteria.

        Raises:
            JobInfoNotFound: Raised when no relevant job info is found.
        """
        snapshots = self._get_all_snapshots()
        jobs_found = []
        for snapshot in snapshots:
            if snapshot.id == self.job_id or snapshot.kind == self.job_kind:
                snapshot_start_time = isoparse(snapshot.start_time)
                if snapshot_start_time >= start_time:
                    if end_time is None:
                        jobs_found.append(snapshot)
                    else:
                        snapshot_end_time = isoparse(snapshot.end_time)
                        if snapshot_end_time <= end_time:
                            jobs_found.append(snapshot)
        if not jobs_found:
            raise JobInfoNotFound("No relevant job info found")
        return jobs_found

    def get_details(self) -> AggregatedJobSnap:
        """
        Retrieve detailed job snapshot information across all targets.

        Returns:
            AggregatedJobSnapshots: A snapshot containing detailed metrics for the job.
        """
        job_args = JobArgs(id=self._job_id, kind=self._job_kind).as_dict()
        query_params = {QPARAM_WHAT: WHAT_QUERY_XACT_STATS}
        return self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            json=job_args,
            params=query_params,
            res_model=AggregatedJobSnap,
        )

    def get_total_time(self) -> Optional[timedelta]:
        """
        Calculates the total job duration as the difference between the earliest start time
        and the latest end time among all job snapshots. If any snapshot is missing an end_time,
        returns None to indicate the job is incomplete.

        Returns:
            Optional[timedelta]: The total duration of the job, or None if incomplete.
        """
        snapshots = self._get_all_snapshots()

        try:
            if not snapshots:
                return None

            earliest_start = None
            latest_end = None

            for s in snapshots:
                # First check for incomplete jobs
                if s.end_time is None:
                    return None

                current_end = isoparse(s.end_time)
                latest_end = (
                    current_end if latest_end is None else max(latest_end, current_end)
                )

                if s.start_time:
                    current_start = isoparse(s.start_time)
                    earliest_start = (
                        current_start
                        if earliest_start is None
                        else min(earliest_start, current_start)
                    )

                if earliest_start is None:  # No valid start times
                    return None

            return latest_end - earliest_start
        except (ValueError, TypeError) as e:
            logger.error("Invalid timestamp format: %s", e)
            return None
        except IndexError:
            return None  # No valid timestamps

    def _get_all_snapshots(self) -> List[JobSnap]:
        """
        Returns a flat list of all job snapshots across all target nodes.

        Returns:
            List[JobSnapshot]: A combined list of snapshots.
        """
        details = self.get_details()
        # Access the __root__ dictionary and chain its values
        return details.list_snapshots()

    def _check_snapshot_finished(self, snapshots):
        job_found = False
        snapshot = None
        for snap in snapshots:
            if snap.id != self._job_id:
                continue
            job_found = True
            snapshot = snap
            break

        if not job_found:
            raise JobInfoNotFound(f"No info found for job {self._job_id}")

        end_time = isoparse(snapshot.end_time).time()
        if snapshot.end_time != "0001-01-01T00:00:00Z" and not snapshot.aborted:
            logger.info("Job '%s' finished at time '%s'", self.job_id, end_time)
            return True
        if snapshot.aborted:
            logger.error("Job '%s' aborted at time '%s'", self.job_id, end_time)
            return True
        return False

    def _check_job_idle(self, snapshots):
        job_found = False
        for snap in snapshots:
            if snap.id != self._job_id:
                continue
            # If any targets are reporting the job not idle, continue waiting
            if not snap.is_idle:
                return False
            job_found = True
        if not job_found:
            raise JobInfoNotFound(f"No info found for job {self._job_id}")
        logger.info("Job '%s' reached idle state", self._job_id)
        return True

    def _wait_for_condition(self, condition_fn, action, timeout):
        passed = 0
        sleep_time = probing_frequency(timeout)

        while True:
            snapshots = self._get_all_snapshots()
            try:
                if condition_fn(snapshots):
                    return
            except JobInfoNotFound:
                logger.info("No information found for job %s, retrying", self._job_id)

            if passed > timeout:
                if len(snapshots) == 0:
                    raise Timeout(action, "No job information found.")
                raise Timeout(action)
            time.sleep(sleep_time)
            passed += sleep_time
            logger.info("Waiting for %s", action)
