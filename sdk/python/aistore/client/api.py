#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import TypeVar, Type
import requests
from urllib.parse import urljoin
from pydantic.tools import parse_raw_as

from aistore.client.bucket import Bucket
from aistore.client.const import HTTP_METHOD_GET, ProviderAIS, QParamArchpath, QParamProvider
from aistore.client.cluster import Cluster
from aistore.client.types import BucketLister, ObjStream
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

    # TODO: Remove once pytorch/data dependency on previous version is resolved
    def list_objects_iter(
        self,
        bck_name: str,
        provider: str = ProviderAIS,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
    ) -> BucketLister:
        """
        Returns an iterator for all objects in a bucket
        Args:
            bck_name (str): Name of a bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
                Defaults to "ais". Empty provider returns buckets of all providers.
            prefix (str, optional): return only objects that start with the prefix
            props (str, optional): comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
        Returns:
            BucketLister: object iterator
        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        return BucketLister(self, bck_name=bck_name, provider=provider, prefix=prefix, props=props, page_size=page_size)

    # TODO: Remove once pytorch/data dependency on previous version is resolved
    def get_object(self, bck_name: str, obj_name: str, provider: str = ProviderAIS, archpath: str = "", chunk_size: int = 1) -> ObjStream:
        """
        Reads an object
        Args:
            bck_name (str): Name of a bucket
            obj_name (str): Name of an object in the bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file from the archive
            chunk_size (int, optional): chunk_size to use while reading from stream
        Returns:
            The stream of bytes to read an object or a file inside an archive.
        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = {QParamProvider: provider, QParamArchpath: archpath}
        resp = self.request(HTTP_METHOD_GET, path=f"objects/{ bck_name }/{ obj_name }", params=params, stream=True)
        length = int(resp.headers.get("content-length", 0))
        e_tag = resp.headers.get("ais-checksum-value", "")
        e_tag_type = resp.headers.get("ais-checksum-type", "")
        return ObjStream(content_length=length, e_tag=e_tag, e_tag_type=e_tag_type, stream=resp, chunk_size=chunk_size)
