#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable

import logging
from typing import Dict, List, Optional, Union

from aistore.sdk.const import (
    HTTP_METHOD_GET,
    ACT_LIST,
    QPARAM_WHAT,
    QPARAM_PRIMARY_READY_REB,
    QPARAM_PROVIDER,
    HEADER_NODE_ID,
    URL_PATH_ETL,
    URL_PATH_REVERSE,
    URL_PATH_BUCKETS,
    URL_PATH_HEALTH,
    URL_PATH_DAEMON,
    URL_PATH_CLUSTER,
    WHAT_SMAP,
    WHAT_ALL_XACT_STATUS,
    WHAT_ALL_RUNNING_STATUS,
    WHAT_NODE_STATS_AND_STATUS,
)
from aistore.sdk.etl.etl_const import ETL_STAGE_RUNNING
from aistore.sdk.provider import Provider

from aistore.sdk.types import (
    BucketModel,
    JobStatus,
    JobQuery,
    ETLInfo,
    ActionMsg,
    Smap,
)
from aistore.sdk.request_client import RequestClient

logger = logging.getLogger("cluster")


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
        return self._get_smap()

    def get_primary_url(self) -> str:
        """
        Returns: URL of primary proxy
        """
        return self._get_smap().proxy_si.public_net.direct_url

    def list_buckets(self, provider: Union[str, Provider] = Provider.AIS):
        """
        Returns list of buckets in AIStore cluster.

        Args:
            provider (str or Provider, optional): Provider of bucket (one of "ais", "aws", "gcp", ...).
                Defaults to "ais". Empty provider returns buckets of all providers.

        Returns:
            List[BucketModel]: A list of buckets

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = {QPARAM_PROVIDER: Provider.parse(provider).value}
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
        res = self._client.request_deserialize(
            HTTP_METHOD_GET, path=URL_PATH_ETL, res_model=List[ETLInfo]
        )
        return [info for info in res if info.stage == ETL_STAGE_RUNNING]

    def is_ready(self) -> bool:
        """
        Checks if cluster is ready or still setting up.

        Returns:
            bool: True if cluster is ready, or false if cluster is still setting up
        """
        # compare with AIS Go API (api/cluster.go) for additional supported options
        params = {QPARAM_PRIMARY_READY_REB: "true"}
        try:
            resp = self._client.request(
                HTTP_METHOD_GET,
                path=URL_PATH_HEALTH,
                endpoint=self.get_primary_url(),
                params=params,
            )
            return resp.ok
        except Exception as err:
            logger.debug(err)
            return False

    def get_performance(self) -> Dict:
        """
        Retrieves the raw performance and status data from each target node in the AIStore cluster.

        Returns:
            Dict: A dictionary where each key is the ID of a target node and each value is the
                    raw AIS performance/status JSON returned by that node (for more information,
                    see https://aistore.nvidia.com/docs/metrics-reference#target-metrics).

        Raises:
            requests.RequestException: If there's an ambiguous exception while processing the request
            requests.ConnectionError: If there's a connection error with the cluster
            requests.ConnectionTimeout: If the connection to the cluster times out
            requests.ReadTimeout: If the timeout is reached while awaiting a response from the cluster
        """
        performance_data = {}
        targets = self._get_targets()
        params = {QPARAM_WHAT: WHAT_NODE_STATS_AND_STATUS}

        for target_id in targets:
            headers = {HEADER_NODE_ID: target_id}
            resp = self.client.request(
                HTTP_METHOD_GET,
                path=f"{URL_PATH_REVERSE}/{URL_PATH_DAEMON}",
                params=params,
                headers=headers,
            )
            resp.raise_for_status()
            performance_data[target_id] = resp.json()

        return performance_data

    def _get_smap(self):
        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_DAEMON,
            res_model=Smap,
            params={QPARAM_WHAT: WHAT_SMAP},
        )

    def _get_targets(self):
        tmap = self._get_smap().tmap
        return list(tmap.keys())

    def get_uuid(self) -> str:
        """
        Returns: UUID of AIStore Cluster
        """
        return self._get_smap().uuid
