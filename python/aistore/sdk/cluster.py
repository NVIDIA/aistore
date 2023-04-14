#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import List, Optional

from aistore.sdk.const import (
    HTTP_METHOD_GET,
    ACT_LIST,
    PROVIDER_AIS,
    QPARAM_WHAT,
    QPARAM_PRIMARY_READY_REB,
    QPARAM_PROVIDER,
    WHAT_SMAP,
    URL_PATH_BUCKETS,
    URL_PATH_HEALTH,
    URL_PATH_DAEMON,
    URL_PATH_CLUSTER,
    WHAT_ALL_XACT_STATUS,
    WHAT_ALL_RUNNING_STATUS,
    URL_PATH_ETL,
)

from aistore.sdk.types import BucketModel, JobStatus, JobQuery, ETLInfo
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import ActionMsg, Smap


# pylint: disable=unused-variable
class Cluster:
    """
    A class representing a cluster bound to an AIS client.
    """

    # pylint: disable=duplicate-code
    def __init__(self, client: RequestClient):
        self._client = client

    @property
    def client(self):
        """Client this cluster uses to make requests"""
        return self._client

    def get_info(self) -> Smap:
        """
        Returns state of AIS cluster, including the detailed information about its nodes.

        Returns:
            aistore.sdk.types.Smap: Smap containing cluster information

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_DAEMON,
            res_model=Smap,
            params={QPARAM_WHAT: WHAT_SMAP},
        )

    def list_buckets(self, provider: str = PROVIDER_AIS):
        """
        Returns list of buckets in AIStore cluster.

        Args:
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
            Defaults to "ais". Empty provider returns buckets of all providers.

        Returns:
            List[BucketModel]: A list of buckets

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = {QPARAM_PROVIDER: provider}
        action = ActionMsg(action=ACT_LIST).dict()

        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_BUCKETS,
            res_model=List[BucketModel],
            json=action,
            params=params,
        )

    def list_jobs_status(self, job_kind="", target_id="") -> List[JobStatus]:
        """
        List the status of jobs on the cluster

        Args:
            job_kind (str, optional): Only show jobs of a particular type
            target_id (str, optional): Limit to jobs on a specific target node

        Returns:
            List of JobStatus objects
        """
        res = self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=Optional[List[JobStatus]],
            json=JobQuery(kind=job_kind, target=target_id).as_dict(),
            params={QPARAM_WHAT: WHAT_ALL_XACT_STATUS},
        )
        if res is None:
            return []
        return res

    def list_running_jobs(self, job_kind="", target_id="") -> List[str]:
        """
        List the currently running jobs on the cluster

        Args:
            job_kind (str, optional): Only show jobs of a particular type
            target_id (str, optional): Limit to jobs on a specific target node

        Returns:
            List of jobs in the format job_kind[job_id]
        """
        return self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=List[str],
            json=JobQuery(kind=job_kind, target=target_id, active=True).as_dict(),
            params={QPARAM_WHAT: WHAT_ALL_RUNNING_STATUS},
        )

    def list_running_etls(self) -> List[ETLInfo]:
        """
        Lists all running ETLs.

        Note: Does not list ETLs that have been stopped or deleted.

        Returns:
            List[ETLInfo]: A list of details on running ETLs
        """
        return self._client.request_deserialize(
            HTTP_METHOD_GET, path=URL_PATH_ETL, res_model=List[ETLInfo]
        )

    def is_aistore_running(self) -> bool:
        """
        Checks if cluster is ready or still setting up.

        Returns:
            bool: True if cluster is ready, or false if cluster is still setting up
        """

        # compare with AIS Go API (api/cluster.go) for additional supported options
        params = {QPARAM_PRIMARY_READY_REB: "true"}
        try:
            resp = self.client.request(
                HTTP_METHOD_GET, path=URL_PATH_HEALTH, params=params
            )
            return resp.ok
        except Exception:
            return False
