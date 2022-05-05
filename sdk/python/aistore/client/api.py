#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

from typing import TypeVar, Type, List, NewType, BinaryIO
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
    QParamKeepBckMD,
)
from .msg import ActionMsg, Bck, BucketList, BucketEntry, Smap
from .errors import InvalidBckProvider

T = TypeVar("T")
Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=unused-variable
class ObjStream:
    def __init__(self, length: int = 0, e_tag: str = "", e_tag_type: str = "", stream: BinaryIO = None):
        self._contentLength = length
        self._stream = stream
        self._e_tag = e_tag
        self._e_tag_type = e_tag_type

    @property
    def contentLength(self) -> int:
        return self._contentLength

    @property
    def e_tag(self) -> str:
        return self._e_tag

    @property
    def e_tag_type(self) -> str:
        return self._e_tag_type

    def iter_content(self, chunk_size: int = 1) -> List[bytes]:
        return self._stream.iter_content(chunk_size=chunk_size)


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

    def list_buckets(self, provider: str = ProviderAIS):
        """
        Returns list of buckets in AIStore cluster

        Args:
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
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

    def create_bucket(self, bck_name: str):
        """
        Creates a bucket in AIStore cluster.
        Always creates a bucket for AIS provider. Other providers do not support bucket creation.

        Args:
            bck_name (str): Name of the new bucket.

        Returns:
            Nothing

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
            requests.exceptions.HTTPError(409): Bucket already exists
        """
        params = {QParamProvider: ProviderAIS}
        action = ActionMsg(action="create-bck").dict()
        self._request(
            HTTP_METHOD_POST,
            path=f"buckets/{ bck_name }",
            json=action,
            params=params,
        )

    def destroy_bucket(self, bck_name: str):
        """
        Destroys a bucket in AIStore cluster.
        Can delete only AIS buckets. Other providers do not support bucket deletion.

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
        params = {QParamProvider: ProviderAIS}
        action = ActionMsg(action="destroy-bck").dict()
        self._request(
            HTTP_METHOD_DELETE,
            path=f"buckets/{ bck_name }",
            json=action,
            params=params,
        )

    def evict_bucket(self, bck_name: str, provider: str, keepMD: bool = True):
        """
        Evicts a bucket in AIStore cluster.
        NOTE: only Cloud buckets can be evicted

        Args:
            bck_name (str): Name of the existing bucket
            provider (str): Name of bucket provider, one of "aws", "gcp", "az", "hdfs" or "ht"
            keepMD (bool, optional): if true, it evicts objects but keeps bucket metadata

        Returns:
            Nothing

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
            InvalidBckProvider: Evicting AIS bucket
        """
        if provider == ProviderAIS:
            raise InvalidBckProvider(provider)
        params = {QParamProvider: provider}
        if keepMD:
            params[QParamKeepBckMD] = "true"
        action = ActionMsg(action="evict-remote-bck").dict()
        self._request(
            HTTP_METHOD_DELETE,
            path=f"buckets/{ bck_name }",
            json=action,
            params=params,
        )

    def head_bucket(self, bck_name: str, provider: str = ProviderAIS) -> Header:
        """
        Requests bucket properties.

        Args:
            bck_name (str): Name of the new bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
                Defaults to "ais". Empty provider returns buckets of all providers.

        Returns:
            Response header with the bucket properties

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
            requests.exeptions.HTTPError(404): The bucket does not exist
        """
        params = {QParamProvider: provider}
        return self._request(
            HTTP_METHOD_HEAD,
            path=f"buckets/{ bck_name }",
            params=params,
        ).headers

    def list_objects(self,
                     bck_name: str,
                     provider: str = ProviderAIS,
                     prefix: str = "",
                     props: str = "",
                     count: int = 0,
                     page_size: int = 0) -> List[BucketEntry]:
        """
        Returns list of objects in a bucket

        Args:
            bck_name (str): Name of a bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
                Defaults to "ais". Empty provider returns buckets of all providers.
            prefix (str, optional): return only objects that start with the prefix
            props (str, optional): comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
            count (int, optional): return first "count" objects, default is "0" - return all objects in the bucket
            page_size (int, optional): read by page_size objects at a time

        Returns:
            List[BucketEntry]: the list of objects in the bucket

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        page_size = count if page_size == 0 else page_size
        value = {"prefix": prefix, "pagesize": page_size, "uuid": "", "props": props}
        params = {QParamProvider: provider}
        obj_list = None
        to_read = count
        list_all = count == 0

        while list_all or to_read > 0:
            action = ActionMsg(action="list", value=value).dict()
            curr = self._request_deserialize(
                HTTP_METHOD_GET,
                path=f"buckets/{ bck_name }",
                res_model=BucketList,
                json=action,
                params=params,
            )
            if obj_list:
                obj_list.entries = obj_list.entries + curr.entries
                # TODO: report flags somehow
                obj_list.flags = obj_list.flags or curr.flags
            obj_list = obj_list or curr
            if curr.continuation_token == "":
                break
            value["uuid"] = curr.uuid
            value["continuation_token"] = curr.continuation_token
            if not list_all:
                to_read -= len(curr.entries)
                value["pagesize"] = min(to_read, page_size)

        return obj_list.entries

    def head_object(self, bck_name: str, obj_name: str, provider: str = ProviderAIS) -> Header:
        """
        Requests object properties.

        Args:
            bck_name (str): Name of the new bucket
            obj_name (str): Name of an object in the bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
                Defaults to "ais". Empty provider returns buckets of all providers.

        Returns:
            Response header with the object properties.

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
            requests.exeptions.HTTPError(404): The object does not exist
        """
        params = {QParamProvider: provider}
        return self._request(
            HTTP_METHOD_HEAD,
            path=f"objects/{ bck_name }/{ obj_name }",
            params=params,
        ).headers

    def get_object(self, bck_name: str, obj_name: str, provider: str = ProviderAIS, archpath: str = "") -> ObjStream:
        """
        Reads an object

        Args:
            bck_name (str): Name of a bucket
            obj_name (str): Name of an object in the bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file from the archive

        Returns:
            The stream of bytes to read an object or a file inside an archive.

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        params = {QParamProvider: provider, QParamArchpath: archpath}
        resp = self._request(HTTP_METHOD_GET, path=f"objects/{ bck_name }/{ obj_name }", params=params, stream=True)
        length = int(resp.headers.get("content-length", 0))
        e_tag = resp.headers.get("ais-checksum-value", "")
        e_tag_type = resp.headers.get("ais-checksum-type", "")
        return ObjStream(length, e_tag, e_tag_type, resp)

    def put_object(self, bck_name: str, obj_name: str, path: str, provider: str = ProviderAIS) -> Header:
        """
        Puts a local file as an object to a bucket in AIS storage

        Args:
            bck_name (str): Name of a bucket
            obj_name (str): Name of an object in the bucket
            path (str): path to local file
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".

        Returns:
            Object properties

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
        """
        url = f"/objects/{ bck_name }/{ obj_name }"
        params = {QParamProvider: provider}
        with open(path, "rb") as data:
            return self._request(
                HTTP_METHOD_PUT,
                path=url,
                params=params,
                data=data,
            ).headers

    def delete_object(self, bck_name: str, obj_name: str, provider: str = ProviderAIS):
        """
        Delete an object from a bucket.

        Args:
            bck_name (str): Name of the new bucket
            obj_name (str): Name of an object in the bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
                Defaults to "ais".

        Returns:
            None

        Raises:
            requests.RequestException: Ambiguous while handling request
            requests.ConnectionError: A connection error occurred
            requests.ConnectionTimeout: Timed out while connecting to AIStore server
            requests.ReadTimeout: Timeout receiving response from server
            requests.exeptions.HTTPError(404): The object does not exist
        """
        params = {QParamProvider: provider}
        self._request(
            HTTP_METHOD_DELETE,
            path=f"objects/{ bck_name }/{ obj_name }",
            params=params,
        )

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
