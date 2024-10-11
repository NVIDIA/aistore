#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable

import logging
from typing import List, Optional, Union

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
    WHAT_NODE_STATS_AND_STATUS_V322,
)
from aistore.sdk.provider import Provider

from aistore.sdk.types import (
    BucketModel,
    JobStatus,
    JobQuery,
    ETLInfo,
    ActionMsg,
    Smap,
    NodeStats,
    NodeStatsV322,
    ClusterPerformance,
    NodeCounter,
    NodeLatency,
    NodeThroughput,
)
from aistore.sdk.request_client import RequestClient
from aistore.sdk.errors import AISError

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
            provider (str or Provider, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az" or "ht".
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
        return self._client.request_deserialize(
            HTTP_METHOD_GET, path=URL_PATH_ETL, res_model=List[ETLInfo]
        )

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

    # pylint: disable=too-many-locals
    def get_performance(
        self,
        get_throughput: bool = True,
        get_latency: bool = True,
        get_counters: bool = True,
    ) -> ClusterPerformance:
        """
        Retrieves and calculates the performance metrics for each target node in the AIStore cluster.
        It compiles throughput, latency, and various operational counters from each target node,
        providing a comprehensive view of the cluster's overall performance

        Args:
            get_throughput (bool, optional): get cluster throughput
            get_latency (bool, optional): get cluster latency
            get_counters (bool, optional): get cluster counters

        Returns:
            ClusterPerformance: An object encapsulating the detailed performance metrics of the cluster,
                including throughput, latency, and counters for each node

        Raises:
            requests.RequestException: If there's an ambiguous exception while processing the request
            requests.ConnectionError: If there's a connection error with the cluster
            requests.ConnectionTimeout: If the connection to the cluster times out
            requests.ReadTimeout: If the timeout is reached while awaiting a response from the cluster
        """

        targets = self._get_targets()
        target_stats = {}
        params = {QPARAM_WHAT: WHAT_NODE_STATS_AND_STATUS}
        res_model = NodeStats
        for target_id in targets:
            headers = {HEADER_NODE_ID: target_id}
            try:
                res = self.client.request_deserialize(
                    HTTP_METHOD_GET,
                    path=f"{URL_PATH_REVERSE}/{URL_PATH_DAEMON}",
                    res_model=res_model,
                    headers=headers,
                    params=params,
                )
            except AISError as err:
                if "unrecognized what=node_status" in err.message:
                    params = {QPARAM_WHAT: WHAT_NODE_STATS_AND_STATUS_V322}
                    res_model = NodeStatsV322
                    res = self.client.request_deserialize(
                        HTTP_METHOD_GET,
                        path=f"{URL_PATH_REVERSE}/{URL_PATH_DAEMON}",
                        res_model=res_model,
                        headers=headers,
                        params=params,
                    )
                else:
                    raise err
            target_stats[target_id] = res
        throughputs = {}
        latencies = {}
        counters = {}
        for target_id, val in target_stats.items():
            if get_throughput:
                throughputs[target_id] = NodeThroughput(val.tracker)
            if get_latency:
                latencies[target_id] = NodeLatency(val.tracker)
            if get_counters:
                counters[target_id] = NodeCounter(val.tracker)

        return ClusterPerformance(
            throughput=throughputs, latency=latencies, counters=counters
        )

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
