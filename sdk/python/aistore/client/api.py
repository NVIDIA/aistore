#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

from typing import TypeVar, Type, List, NewType
import requests
from urllib.parse import urljoin
from pydantic.tools import parse_raw_as

from .const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_HEAD,
    QParamArchpath,
    QParamProvider,
    ProviderAIS,
    QParamWhat,
)
from .msg import ActionMsg, Bck, BucketList, Smap

T = TypeVar("T")
Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=unused-variable
class Client:
    """
    AIStore client for managing buckets, objects, ETL jobs

    Args:
        endpoint (str): AIStore server endpoint
    """
    def __init__(self, endpoint: str):
        self._endpoint = endpoint
        self._base_url = urljoin(self._endpoint, "v1")

    @property
    def base_url(self):
        return self._base_url

    @property
    def endpoint(self):
        return self._endpoint

    def _request_deserialize(self, method: str, path: str, res_model: Type[T], **kwargs) -> T:
        resp = self._request(method, path, **kwargs)
        return parse_raw_as(res_model, resp.text)

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{ self.base_url }/{ path.lstrip('/') }"
        resp = requests.request(method, url, headers={"Accept": "application/json"}, **kwargs)
        resp.raise_for_status()
        return resp

    def _request_raw(self, method: str, path: str, **kwargs) -> List[bytes]:
        resp = self._request(method, path, **kwargs)
        return resp.content

    def list_buckets(self, provider: str = ProviderAIS):
        """
        Returns list of buckets in AIStore cluster

        Args:
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp" or "az".
            Defaults to "ais". Empty provider returns buckets of all providers.

        Returns:
            List[Bck]: A list of buckets

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        params = {QParamProvider: provider}
        action = ActionMsg(action="list").dict()
        return self._request_deserialize(
            HTTP_METHOD_GET,
            path="buckets",
            res_model=List[Bck],
            json=action,
            params=params,
        )

    def create_bucket(self, bck_name, **kwargs):
        """
        Creates a bucket in AIStore cluster

        Args:
            bck_name (str): Name of the new bucket

        Returns:
            Nothing

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
            requests.exceptions.HTTPError(409): Bucket already exists
        """
        bck = Bck(name=bck_name, **kwargs)
        params = {QParamProvider: bck.provider}
        action = ActionMsg(action="create-bck").dict()
        self._request(
            HTTP_METHOD_POST,
            path=f"buckets/{ bck_name }",
            json=action,
            params=params,
        )

    def destroy_bucket(self, bck_name, **kwargs):
        """
        Destroys a bucket in AIStore cluster

        Args:
            bck_name (str): Name of the existing bucket

        Returns:
            Nothing

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        bck = Bck(name=bck_name, **kwargs)
        params = {QParamProvider: bck.provider}
        action = ActionMsg(action="destroy-bck").dict()
        self._request(
            HTTP_METHOD_DELETE,
            path=f"buckets/{ bck_name }",
            json=action,
            params=params,
        )

    def head_bucket(self, bck_name: str, **kwargs) -> Header:
        """
        Tests if a bucket exists in AIStore cluster

        Args:
            bck_name (str): Name of the new bucket

        Keyword Args:
            provider (str): Name of bucket provider, one of "ais", "aws", "gcp" or "az". Defaults to "ais"

        Returns:
            Nothing

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
            requests.exeptions.HTTPError(404): The bucket does not exist
        """
        bck = Bck(name=bck_name, **kwargs)
        params = {QParamProvider: bck.provider}
        return self._request(
            HTTP_METHOD_HEAD,
            path=f"buckets/{ bck_name }",
            params=params,
        ).headers

    def list_objects(self, bck_name, **kwargs):
        """
        Returns list of objects in a bucket

        Args:
            bck_name (str): Name of a bucket

        Keyword Args:
            provider (str): Name of bucket provider, one of "ais", "aws", "gcp" or "az". Defaults to "ais"
            prefix (str): return only objects that starts with the prefix

        Returns:
            BucketList: next page of objects in the bucket

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        bck = Bck(name=bck_name, **kwargs)
        value = {"prefix": kwargs["prefix"]} if "prefix" in kwargs else None
        action = ActionMsg(action="list", value=value).dict()
        params = {QParamProvider: bck.provider}

        return self._request_deserialize(
            HTTP_METHOD_GET,
            path=f"buckets/{ bck_name }",
            res_model=BucketList,
            json=action,
            params=params,
        )

    def get_object(self, bck_name, object_name, **kwargs):
        """
        Reads an object content

        Args:
            bck_name (str): Name of a bucket
            object_name (str): Name of an object in the bucket

        Keyword Args:
            provider (str): Name of bucket provider, one of "ais", "aws", "gcp" or "az". Defaults to "ais"
            archpath (str): If the object is an archive, use `archpath` to extract a single file from the archive
            transform_id (str): UUID of ETL transformation worker

        Returns:
            List[byte] - the content of an object or a file inside an archive

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        bck = Bck(name=bck_name, **kwargs)
        params = {QParamProvider: bck.provider}
        if "archpath" in kwargs:
            params[QParamArchpath] = kwargs["archpath"]
        if "transform_id" in kwargs:
            params["uuid"] = kwargs["transform_id"]
        return self._request_raw(HTTP_METHOD_GET, path=f"objects/{ bck_name }/{ object_name }", params=params)

    def put_object(self, bck_name: str, object_name: str, path: str, **kwargs) -> Header:
        """
        Puts a local file as an object to a bucket in AIS storage

        Args:
            bck_name (str): Name of a bucket
            object_name (str): Name of an object in the bucket
            path (str): path to local file

        Keyword Args:
            provider (str): Name of bucket provider, one of "ais", "aws", "gcp" or "az". Defaults to "ais"

        Returns:
            Nothing

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        url = f"/objects/{ bck_name }/{ object_name }"
        bck = Bck(name=bck_name, **kwargs)
        params = {QParamProvider: bck.provider}
        with open(path, "rb") as data:
            return self._request(
                HTTP_METHOD_PUT,
                path=url,
                params=params,
                data=data,
            ).headers

    def get_cluster_info(self) -> Smap:
        """
        Returns state of AIS cluster, including the detailed information about its nodes

        Args:
            No arguments

        Returns:
            aistore.msg.Smap

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        return self._request_deserialize(
            HTTP_METHOD_GET,
            path="daemon",
            res_model=Smap,
            params={QParamWhat: "smap"},
        )
