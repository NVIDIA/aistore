#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

import itertools
from datetime import datetime, timezone
from typing import List, Dict
import time

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
from aistore.sdk.types import JobStatus, JobArgs, ActionMsg, JobSnapshot, BucketModel
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

    def _parse_iso_datetime(self, dt_str):
        # Remove 'Z' timezone designator if present
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1]
        # Handle fractional seconds (nanoseconds) by truncating to microseconds
        if "." in dt_str:
            date_part, frac_part = dt_str.split(".")
            # Truncate or pad the fractional part to 6 digits (microseconds)
            frac_part = (frac_part + "000000")[:6]
            dt_str = f"{date_part}.{frac_part}"
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")

        # Always return the datetime as UTC-aware
        return dt.replace(tzinfo=timezone.utc)

    def get_within_timeframe(
        self, start_time: datetime, end_time: datetime
    ) -> List[JobSnapshot]:
        """
        Checks for jobs that started and finished within a specified timeframe.

        Args:
            start_time (datetime.datetime): The start of the timeframe for monitoring jobs.
            end_time (datetime.datetime): The end of the timeframe for monitoring jobs.

        Returns:
            List[JobSnapshot]: A list of jobs that have finished within the specified timeframe.

        Raises:
            JobInfoNotFound: Raised when information on a job's status could not be found.
        """
        snapshots = self._query_job_snapshots()
        jobs_found = []
        for snapshot in snapshots:
            if snapshot.id == self.job_id or snapshot.kind == self.job_kind:
                snapshot_start_time = self._parse_iso_datetime(snapshot.start_time)
                snapshot_end_time = self._parse_iso_datetime(snapshot.end_time)
                if snapshot_start_time >= start_time and snapshot_end_time <= end_time:
                    jobs_found.append(snapshot)
        if len(jobs_found) == 0:
            raise JobInfoNotFound("No relevant job info found")
        return jobs_found

    def _query_job_snapshots(self) -> List[JobSnapshot]:
        value = JobArgs(id=self._job_id, kind=self._job_kind).as_dict()
        params = {QPARAM_WHAT: WHAT_QUERY_XACT_STATS}
        snapshot_lists = self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            json=value,
            params=params,
            res_model=Dict[str, List[JobSnapshot]],
        ).values()
        snapshots = list(itertools.chain.from_iterable(snapshot_lists))
        return snapshots

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

        end_time = datetime.fromisoformat(snapshot.end_time.rstrip("Z")).time()
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
            snapshots = self._query_job_snapshots()
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
