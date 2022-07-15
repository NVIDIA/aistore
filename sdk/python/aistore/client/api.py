#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import TypeVar, Type
import requests
from urllib.parse import urljoin
from pydantic.tools import parse_raw_as

from aistore.client.const import ProviderAIS
from aistore.client.bucket import Bucket
from aistore.client.cluster import Cluster
from aistore.client.utils import handle_errors
from aistore.client.xaction import Xaction

T = TypeVar("T")


# pylint: disable=unused-variable
class Client:
    """
    AIStore client for managing buckets, objects, ETL jobs

    Args:
        endpoint (str): AIStore endpoint
    """
    def __init__(self, endpoint: str):
        self._endpoint = endpoint
        self._base_url = urljoin(self._endpoint, "v1")
        self._session = requests.session()

    @property
    def base_url(self):
        return self._base_url

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def session(self):
        return self._session

    def request_deserialize(self, method: str, path: str, res_model: Type[T], **kwargs) -> T:
        resp = self.request(method, path, **kwargs)
        return parse_raw_as(res_model, resp.text)

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{ self.base_url }/{ path.lstrip('/') }"
        resp = self.session.request(method, url, headers={"Accept": "application/json"}, **kwargs)
        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp)
        return resp

    def bucket(self, bck_name: str, provider: str = ProviderAIS, ns: str = ""):
        """
        Factory constructor for bucket object. 
        Does not make any HTTP request, only instantiates a bucket object owned by the client.

        Args:
            bck_name (str): Name of bucket (optional, defaults to "ais").
            provider (str): Provider of bucket (one of "ais", "aws", "gcp", ...).
        
        Returns:
            The bucket object created.
        """
        return Bucket(client=self, bck_name=bck_name, provider=provider, ns=ns)

    def cluster(self):
        """
        Factory constructor for cluster object. 
        Does not make any HTTP request, only instantiates a cluster object owned by the client.

        Args:
            None
        
        Returns:
            The cluster object created.
        """
        return Cluster(client=self)

    def xaction(self):
        """
        Factory constructor for xaction object, which contains xaction-related functions. 
        Does not make any HTTP request, only instantiates an xaction object bound to the client.

        Args:
            None
        
        Returns:
            The xaction object created.
        """
        return Xaction(client=self)
