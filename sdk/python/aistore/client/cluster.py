#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import List

from aistore.client.const import (HTTP_METHOD_GET, ACT_LIST, ProviderAIS, QParamWhat, QparamPrimaryReadyReb, QParamProvider)

from aistore.client.types import ActionMsg, Bck, Smap


# pylint: disable=unused-variable
class Cluster:
    """
    A class representing a cluster bound to an AIS client.

    Args:
        None
    """
    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        """The client object bound to this cluster."""
        return self._client

    def get_info(self) -> Smap:
        """
        Returns state of AIS cluster, including the detailed information about its nodes.

        Args:
            None

        Returns:
            aistore.msg.Smap: Smap containing cluster information

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path="daemon",
            res_model=Smap,
            params={QParamWhat: "smap"},
        )

    def list_buckets(self, provider: str = ProviderAIS):
        """
        Returns list of buckets in AIStore cluster.

        Args:
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
            Defaults to "ais". Empty provider returns buckets of all providers.

        Returns:
            List[Bck]: A list of buckets

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
            path="buckets",
            res_model=List[Bck],
            json=action,
            params=params,
        )

    def is_aistore_running(self) -> bool:
        """
        Returns True if cluster is ready, or false if cluster is still setting up.
        
        Args:
            None
        
        Returns: 
            bool: True if cluster is ready, or false if cluster is still setting up
        """

        # compare with AIS Go API (api/cluster.go) for additional supported options
        params = {QparamPrimaryReadyReb: "true"}
        try:
            resp = self.client.request(HTTP_METHOD_GET, path="health", params=params)
            return resp.ok
        except Exception:
            return False
