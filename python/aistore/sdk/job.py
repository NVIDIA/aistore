#
# Copyright (c) 2022-2026, NVIDIA CORPORATION. All rights reserved.
#

from datetime import datetime, timedelta, timezone
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
    ACT_XACT_STOP,
    WHAT_QUERY_XACT_STATS,
)
from aistore.sdk.errors import JobInfoNotFound, Timeout
from aistore.sdk.wait_result import WaitResult
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
from aistore.sdk.xact_const import idles_before_finishing, is_valid_kind

logger = get_logger(__name__)

# Number of consecutive idle polls required before an idle-kind job is
# considered done -- mirrors Go's `xact.numConsecutiveIdle` (xact/api_wait.go).
NUM_CONSECUTIVE_IDLE = 2


class _IdleStreak:  # pylint: disable=too-few-public-methods
    """
    Idle-wait done-condition mirroring Go's `xact.snapsIdle`: the job must
    report idle on `NUM_CONSECUTIVE_IDLE` consecutive polls before completing;
    an abort on any target completes immediately.
    """

    def __init__(self):
        self._count = 0

    def __call__(self, details: AggregatedJobSnap) -> bool:
        if details.any_aborted():
            return True
        if details.all_idle():
            self._count += 1
            return self._count >= NUM_CONSECUTIVE_IDLE
        self._count = 0
        return False


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
            JobStatus: The job status including id, finish time, and error info

        Raises:
            ValueError: If the job does not have an assigned ID
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
    ) -> WaitResult:
        """
        Wait for a job to reach the appropriate "done" state for its kind.

        Mirrors the Go-side `api/xaction.go: WaitForXaction(bp, args)`
        descriptor-aware dispatch:
          - if `self.job_kind` is an idle kind (e.g. `download`, `get-batch`,
            `copy-listrange`, `etl-listrange`, `archive`, `list`,
            `put-copies`, `ec-get`/`ec-put`/`ec-resp`), wait for the job to
            reach an idle state across all targets (see `wait_for_idle`).
          - otherwise (including unknown or empty kind), wait for the job to
            reach a terminal state (its `end_time` becomes set).

        If `job_kind` is empty but `job_id` is set, the kind is first
        resolved from the cluster so the job dispatches correctly (raising
        `JobInfoNotFound` if the id does not exist).

        Idle is confirmed over `NUM_CONSECUTIVE_IDLE` consecutive polls; an
        abort on any target returns immediately with `success=False`.

        For finer control (single-node fan-out, explicit idle/terminal),
        call `wait_for_idle` or `wait_single_node` directly.

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

        Returns:
            WaitResult: Outcome of the wait operation

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
            errors.JobInfoNotFound: If `job_kind` is empty and `job_id` is not found
        """
        kind = self._job_kind
        if not kind and self._job_id:
            kind = self._resolve_kind()
        if kind and idles_before_finishing(kind):
            return self._wait_for_condition(
                _IdleStreak(),
                timeout,
                verbose,
                "reached idle state",
            )
        if kind and not is_valid_kind(kind):
            # not an error: SDK kind table may lag server; default to terminal
            logger.debug("unknown xaction kind %r; defaulting to terminal wait", kind)
        return self._wait_terminal(timeout, verbose)

    def _resolve_kind(self) -> str:
        """
        Look up this job's kind from the cluster by id and cache it, so
        `wait()` can dispatch correctly when only a job id was provided.

        Raises:
            JobInfoNotFound: if no snapshot for `job_id` is found
        """
        # get_details() already scopes the query to this job's id
        for snap in self.get_details().list_snapshots():
            if snap.kind:
                self._job_kind = snap.kind
                return snap.kind
        raise JobInfoNotFound(f"No job info found for '{self._job_id}'")

    def _wait_terminal(
        self,
        timeout: int,
        verbose: bool,
    ) -> WaitResult:
        """Poll `status()` until the job reaches a terminal state."""
        logger.disabled = not verbose
        passed = 0
        sleep_time = probing_frequency(timeout)

        while True:
            status = self.status()

            if passed > timeout:
                logger.error(
                    "Timeout waiting for job '%s' after %ds. Job status: %s",
                    status.uuid,
                    timeout,
                    status,
                )
                raise Timeout(f"job '{status.uuid}'", f"after {timeout}s")

            if status.end_time == 0:
                time.sleep(sleep_time)
                passed += sleep_time
                logger.info("Waiting on job '%s'...", status.uuid)
                continue

            end_time = (
                datetime.fromtimestamp(status.end_time / 1e9, tz=timezone.utc)
                if status.end_time > 0
                else None
            )
            error = status.err or "aborted" if status.aborted else status.err
            success = not status.err and not status.aborted
            result = WaitResult(
                job_id=status.uuid,
                success=success,
                error=error,
                end_time=end_time,
            )

            if success:
                logger.info("Job '%s' finished successfully", status.uuid)
            else:
                logger.error("Job '%s' failed: %s", status.uuid, result.error)

            return result

    def wait_for_idle(
        self,
        timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
        verbose: bool = True,
    ) -> WaitResult:
        """
        Wait for a job to reach an idle state

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

        Returns:
            WaitResult: Outcome of the wait operation

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
        """
        return self._wait_for_condition(
            _IdleStreak(), timeout, verbose, "reached idle state"
        )

    def wait_single_node(
        self,
        timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
        verbose: bool = True,
    ) -> WaitResult:
        """
        Wait for a job running on a single node

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

        Returns:
            WaitResult: Outcome of the wait operation

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
        """
        return self._wait_for_condition(
            AggregatedJobSnap.any_finished, timeout, verbose, "finished"
        )

    def start(
        self,
        daemon_id: str = "",
        force: bool = False,
        buckets: Optional[List[Bucket]] = None,
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
        action = ActionMsg(action=ACT_START, value=job_args.as_dict()).model_dump()

        resp = self._client.request(
            HTTP_METHOD_PUT, path=URL_PATH_CLUSTER, json=action, params=params
        )
        return resp.text

    def abort(self) -> None:
        """
        Abort (stop) this job.

        After aborting, `wait()` (or `wait_for_idle`) will return a
        `WaitResult` with `success=False` and the abort error rather than
        blocking until timeout.

        Raises:
            ValueError: If neither job_id nor job_kind is set
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        if not self._job_id and not self._job_kind:
            raise ValueError("Cannot abort a job without an id or kind")
        job_args = JobArgs(id=self._job_id, kind=self._job_kind)
        action = ActionMsg(action=ACT_XACT_STOP, value=job_args.as_dict()).model_dump()
        self._client.request(HTTP_METHOD_PUT, path=URL_PATH_CLUSTER, json=action)

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
        snapshots = self.get_details().list_snapshots()
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
        snapshots = self.get_details().list_snapshots()

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

    def _wait_for_condition(
        self, condition_fn, timeout: int, verbose: bool, state: str
    ) -> WaitResult:
        logger.disabled = not verbose
        passed = 0
        sleep_time = probing_frequency(timeout)

        while True:
            details = self.get_details()
            snaps = details.list_snapshots()

            if passed > timeout:
                logger.error(
                    "Timeout waiting for job '%s' after %ds. Job snapshots: %s",
                    self._job_id,
                    timeout,
                    snaps,
                )
                raise Timeout(f"job '{self._job_id}'", f"after {timeout}s")

            if not snaps:
                logger.info("No info for job '%s', retrying", self._job_id)
            else:
                if condition_fn(details):
                    result = WaitResult.from_snapshots(self._job_id, snaps)
                    if result.success:
                        logger.info("Job '%s' %s", self._job_id, state)
                    else:
                        logger.error("Job '%s' failed: %s", self._job_id, result.error)
                    return result

                logger.info("Waiting on job '%s'...", self._job_id)

            time.sleep(sleep_time)
            passed += sleep_time
