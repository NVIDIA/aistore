#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable

import itertools
import logging
from datetime import datetime
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
from aistore.sdk.errors import Timeout
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import JobStatus, JobArgs, ActionMsg, JobSnapshot, BucketModel
from aistore.sdk.utils import probing_frequency


# pylint: disable=unused-variable
class Job:
    """
    A class containing job-related functions.

    Args:
        client (RequestClient): Client for interfacing with AIS cluster
        job_id (str, optional): ID of a specific job, empty for all jobs
        job_kind (str, optional): Specific kind of job, empty for all kinds
    """

    # pylint: disable=duplicate-code
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
        daemon_id: str = "",
        only_running: bool = False,
    ) -> JobStatus:
        """
        Return status of a job

        Args:
            daemon_id (str, optional): Return jobs only running on the daemon_id.
            only_running (bool, optional):
                True - return only currently running jobs
                False - include finished and aborted jobs

        Returns:
            The job description.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        return self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=JobStatus,
            json=JobArgs(
                id=self._job_id,
                kind=self._job_kind,
                only_running=only_running,
                daemon_id=daemon_id,
            ).get_json(),
            params={QPARAM_WHAT: WHAT_ONE_XACT_STATUS},
        )

    def wait(
        self,
        daemon_id: str = "",
        timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
        verbose: bool = True,
    ):
        """
        Wait for a job to finish

        Args:
            daemon_id (str, optional): Return jobs only running on the daemon_id.
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
        logger = logging.getLogger(f"{__name__}.wait")
        logger.disabled = not verbose
        passed = 0
        sleep_time = probing_frequency(timeout)
        while True:
            if passed > timeout:
                raise Timeout("wait for job to finish")
            status = self.status(daemon_id=daemon_id)
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
        daemon_id: str = "",
        timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
        verbose: bool = True,
    ):
        """
        Wait for a job to reach an idle state

        Args:
            daemon_id (str, optional): Only check this specific target node for the idle job
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
        logger = logging.getLogger(f"{__name__}.wait_for_idle")
        logger.disabled = not verbose
        passed = 0
        sleep_time = probing_frequency(timeout)
        while True:
            if passed > timeout:
                raise Timeout("wait for job to reach idle")
            snapshot_lists = self._query_job_snapshots(daemon_id=daemon_id).values()
            snapshots = itertools.chain.from_iterable(snapshot_lists)
            # If any targets are reporting the job not idle, continue waiting
            if any(not snap.is_idle for snap in snapshots if snap.id == self._job_id):
                time.sleep(sleep_time)
                passed += sleep_time
                logger.info("Waiting on job '%s' to reach idle state...", self._job_id)
                continue
            logger.info("Job '%s' reached idle state", self._job_id)
            break

    def start(
        self,
        daemon_id: str = "",
        force: bool = False,
        buckets: List[Bucket] = None,
    ) -> str:
        """
        Start a job and return its ID.

        Args:
            daemon_id (str, optional): Return jobs only running on the daemon_id.
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
                    name=bck.name, provider=bck.provider, namespace=bck.namespace
                )
                for bck in buckets
            ]
            if len(bucket_models) == 1:
                job_args.bucket = bucket_models[0]
            else:
                job_args.buckets = bucket_models
        params = {QPARAM_FORCE: "true"} if force else {}
        action = ActionMsg(action=ACT_START, value=job_args.get_json()).dict()

        resp = self._client.request(
            HTTP_METHOD_PUT, path=URL_PATH_CLUSTER, json=action, params=params
        )
        return resp.text

    def _query_job_snapshots(self, daemon_id: str = "") -> Dict[str, List[JobSnapshot]]:
        value = JobArgs(
            id=self._job_id, kind=self._job_kind, daemon_id=daemon_id
        ).get_json()
        params = {QPARAM_WHAT: WHAT_QUERY_XACT_STATS}
        return self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            json=value,
            params=params,
            res_model=Dict[str, List[JobSnapshot]],
        )
