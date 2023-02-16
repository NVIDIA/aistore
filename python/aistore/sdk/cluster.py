#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import List

from aistore.sdk.const import (
    HTTP_METHOD_GET,
    ACT_LIST,
    ProviderAIS,
    QParamWhat,
    QparamPrimaryReadyReb,
    QParamProvider,
    QParamSmap,
    URL_PATH_BUCKETS,
    URL_PATH_HEALTH,
    URL_PATH_DAEMON,
)

from aistore.sdk.types import BucketModel
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
            params={QParamWhat: QParamSmap},
        )

    def list_buckets(self, provider: str = ProviderAIS):
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
        params = {QParamProvider: provider}
        action = ActionMsg(action=ACT_LIST).dict()

        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_BUCKETS,
            res_model=List[BucketModel],
            json=action,
            params=params,
        )

    def is_aistore_running(self) -> bool:
        """
        Checks if cluster is ready or still setting up.

        Returns:
            bool: True if cluster is ready, or false if cluster is still setting up
        """

        # compare with AIS Go API (api/cluster.go) for additional supported options
        params = {QparamPrimaryReadyReb: "true"}
        try:
            resp = self.client.request(
                HTTP_METHOD_GET, path=URL_PATH_HEALTH, params=params
            )
            return resp.ok
        except Exception:
            return False
