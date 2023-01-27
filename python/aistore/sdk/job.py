#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import List
import time

from aistore.sdk.const import HTTP_METHOD_GET, HTTP_METHOD_PUT, QParamWhat, QParamForce
from aistore.sdk.errors import Timeout
from aistore.sdk.types import Bck, JobStatus, JobArgs
from aistore.sdk.utils import probing_frequency


# pylint: disable=unused-variable
class Job:
    """
    A class containing job-related functions.

    Args:
        None
    """

    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        """The client bound to this job object."""
        return self._client

    def status(
        self,
        job_id: str = "",
        job_kind: str = "",
        daemon_id: str = "",
        only_running: bool = False,
    ) -> JobStatus:
        """
        Return status of a job

        Args:
            job_id (str, optional): UUID of the job. Empty - all jobs.
            job_kind (str, optional): Kind of the job. Empty - all kinds.
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
        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path="cluster",
            res_model=JobStatus,
            json=JobArgs(
                id=job_id, kind=job_kind, only_running=only_running, daemon_id=daemon_id
            ).get_json(),
            params={QParamWhat: "status"},
        )

    def wait_for_job(
        self,
        job_id: str = "",
        job_kind: str = "",
        daemon_id: str = "",
        timeout: int = 300,
    ):
        """
        Wait for a job to finish

        Args:
            job_id (str, optional): UUID of the job. Empty - all jobs.
            job_kind (str, optional): Kind of the job. Empty - all kinds.
            daemon_id (str, optional): Return jobs only running on the daemon_id.
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.

        Returns:
            None

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
        """
        passed = 0
        sleep_time = probing_frequency(timeout)
        while True:
            if passed > timeout:
                raise Timeout("wait for job to finish")
            status = self.status(job_id=job_id, job_kind=job_kind, daemon_id=daemon_id)
            if status.end_time != 0:
                break
            time.sleep(sleep_time)
            passed += sleep_time
            print(status)

    def start(
        self,
        job_kind: str = "",
        daemon_id: str = "",
        force: bool = False,
        buckets: List[Bck] = None,
    ) -> str:
        """
        Start a job and return its ID.

        Args:
            job_kind (str, optional): Kind of the job (for supported kinds, see api/apc/const.go). Empty - all kinds.
            daemon_id (str, optional): Return jobs only running on the daemon_id.
            force (bool, optional): Override existing restrictions for a bucket (e.g., run LRU eviction even if the bucket has LRU disabled).
            buckets (List[Bck], optional): List of one or more buckets; applicable only for jobs that have bucket scope (for details and full enumeration, see xact/table.go).

        Returns:
            The running job ID.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        value = JobArgs(kind=job_kind, daemon_id=daemon_id, buckets=buckets).get_json()
        params = {QParamForce: "true"} if force else {}
        action = {"action": "start", "value": value}

        resp = self.client.request(
            HTTP_METHOD_PUT, path="cluster", json=action, params=params
        )
        return resp.text
