#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import TypeVar, Type, List, NewType
import requests
import time
from urllib.parse import urljoin
from pydantic.tools import parse_raw_as

from aistore.client.const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_HEAD,
    ProviderAIS,
    QParamArchpath,
    QParamProvider,
    QParamWhat,
    QParamKeepBckMD,
    QParamBucketTo,
    QparamPrimaryReadyReb
)
from aistore.client.types import (ActionMsg, Bck, BucketList, BucketEntry, ObjStream, Smap, XactStatus, BucketLister)
from aistore.client.errors import (InvalidBckProvider, Timeout)
from aistore.client.utils import handle_errors, probing_frequency

T = TypeVar("T")
Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=unused-variable
# pylint: disable=R0904
class Client:
    """
    AIStore client for managing buckets, objects, ETL jobs

    Args:
        endpoint (str): AIStore endpoint
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
        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp)
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
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
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

    def is_aistore_running(self) -> bool:
        """
        Returns True if cluster is ready and false if cluster is still setting up
        
        Args:
            None
        
        Returns: 
            bool: True if cluster is ready and false if cluster is still setting up
        """

        # compare with AIS Go API (api/cluster.go) for additional supported options
        params = {QparamPrimaryReadyReb: "true"}
        try:
            resp = self._request(HTTP_METHOD_GET, path="health", params=params)
            return resp.ok
        except Exception:
            return False

    def create_bucket(self, bck_name: str):
        """
        Creates a bucket in AIStore cluster.
        Always creates a bucket for AIS provider. Other providers do not support bucket creation.

        Args:
            bck_name (str): Name of the new bucket.

        Returns:
            Nothing

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
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
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = {QParamProvider: ProviderAIS}
        action = ActionMsg(action="destroy-bck").dict()
        self._request(
            HTTP_METHOD_DELETE,
            path=f"buckets/{ bck_name }",
            json=action,
            params=params,
        )

    def rename_bucket(self, from_bck: str, to_bck: str) -> str:
        """
        Renames/moves from_bck to to_bck. Only works on AIS buckets. Returns xaction id that can be used later to check the status of the
        asynchronous operation.

        Args:
            from_bck (str): Bucket to be renamed/moved
            to_bck (str): New bucket name
        
        Returns:
            Xaction id (as str) that can be used to check the status of the operation.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        params = {QParamProvider: ProviderAIS, QParamBucketTo: ProviderAIS + '/@#/' + to_bck + '/'}
        action = ActionMsg(action="move-bck").dict()
        resp = self._request(HTTP_METHOD_POST, path=f"buckets/{ from_bck }", json=action, params=params)
        return resp.text

    def evict_bucket(self, bck_name: str, provider: str, keep_md: bool = True):
        """
        Evicts a bucket in AIStore cluster.
        NOTE: only Cloud buckets can be evicted

        Args:
            bck_name (str): Name of the existing bucket
            provider (str): Name of bucket provider, one of "aws", "gcp", "az", "hdfs" or "ht"
            keep_md (bool, optional): if true, it evicts objects but keeps bucket metadata

        Returns:
            Nothing

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            InvalidBckProvider: Evicting AIS bucket
        """
        if provider == ProviderAIS:
            raise InvalidBckProvider(provider)
        params = {QParamProvider: provider}
        if keep_md:
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
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exeptions.HTTPError(404): The bucket does not exist
        """
        params = {QParamProvider: provider}
        return self._request(
            HTTP_METHOD_HEAD,
            path=f"buckets/{ bck_name }",
            params=params,
        ).headers

    def copy_bucket(
        self,
        from_bck_name: str,
        to_bck_name: str,
        prefix: str = "",
        dry_run: bool = False,
        force: bool = False,
        from_provider: str = ProviderAIS,
        to_provider: str = ProviderAIS,
    ) -> str:
        """
        Returns xaction id that can be used later to check the status of the
        asynchronous operation.

        Args:
            from_bck_name (str): Name of the source bucket.
            to_bck_name (str): Name of the destination bucket.
            prefix (str, optional): If set, only the objects starting with
                provider prefix will be copied.
            dry_run (bool, optional): Determines if the copy should actually
                happen or not.
            force (bool, optional): Override existing destination bucket.
            from_provider (str, optional): Name of source bucket provider.
            to_provider (str, optional): Name of destination bucket provider.

        Returns:
            Xaction id (as str) that can be used to check the status of the operation.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out receiving response from AIStore
        """

        value = {"prefix": prefix, "dry_run": dry_run, "force": force}
        action = ActionMsg(action="copy-bck", value=value).dict()
        params = {QParamProvider: from_provider, QParamBucketTo: to_provider + '/@#/' + to_bck_name + '/'}
        return self._request(
            HTTP_METHOD_POST,
            path=f"buckets/{ from_bck_name }",
            json=action,
            params=params,
        ).text

    def list_objects(
        self,
        bck_name: str,
        provider: str = ProviderAIS,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
        uuid: str = "",
        continuation_token: str = ""
    ) -> BucketList:
        """
        Returns a structure that contains a page of objects, xaction UUID, and continuation token (to read the next page, if available).

        Args:
            bck_name (str): Name of a bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
                Defaults to "ais". Empty provider returns buckets of all providers.
            prefix (str, optional): return only objects that start with the prefix
            props (str, optional): comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
            page_size (int, optional): return at most "page_size" objects.
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number objects.
            uuid (str, optional): job UUID, required to get the next page of objects
            continuation_token (str, optional): marks the object to start reading the next page

        Returns:
            BucketList: the page of objects in the bucket and the continuation token to get the next page.
            Empty continuation token marks the final page of the object list.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        value = {"prefix": prefix, "pagesize": page_size, "uuid": uuid, "props": props, "continuation_token": continuation_token}
        params = {QParamProvider: provider}
        action = ActionMsg(action="list", value=value).dict()

        return self._request_deserialize(
            HTTP_METHOD_GET,
            path=f"buckets/{ bck_name }",
            res_model=BucketList,
            json=action,
            params=params,
        )

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

    def list_all_objects(
        self,
        bck_name: str,
        provider: str = ProviderAIS,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
    ) -> List[BucketEntry]:
        """
        Returns a list of all objects in a bucket

        Args:
            bck_name (str): Name of a bucket
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
                Defaults to "ais". Empty provider returns buckets of all providers.
            prefix (str, optional): return only objects that start with the prefix
            props (str, optional): comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
            page_size (int, optional): return at most "page_size" objects.
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number objects.

        Returns:
            List[BucketEntry]: list of objects in a bucket

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        value = {"prefix": prefix, "uuid": "", "props": props, "continuation_token": "", "pagesize": page_size}
        obj_list = None

        while True:
            resp = self.list_objects(
                bck_name=bck_name, provider=provider, prefix=prefix, props=props, uuid=value["uuid"], continuation_token=value["continuation_token"]
            )
            if obj_list:
                obj_list = obj_list + resp.entries
            obj_list = obj_list or resp.entries
            if resp.continuation_token == "":
                break
            value["continuation_token"] = resp.continuation_token
            value["uuid"] = resp.uuid
        return obj_list

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
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exeptions.HTTPError(404): The object does not exist
        """
        params = {QParamProvider: provider}
        return self._request(
            HTTP_METHOD_HEAD,
            path=f"objects/{ bck_name }/{ obj_name }",
            params=params,
        ).headers

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
        resp = self._request(HTTP_METHOD_GET, path=f"objects/{ bck_name }/{ obj_name }", params=params, stream=True)
        length = int(resp.headers.get("content-length", 0))
        e_tag = resp.headers.get("ais-checksum-value", "")
        e_tag_type = resp.headers.get("ais-checksum-type", "")
        return ObjStream(content_length=length, e_tag=e_tag, e_tag_type=e_tag_type, stream=resp, chunk_size=chunk_size)

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
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
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
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
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
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        return self._request_deserialize(
            HTTP_METHOD_GET,
            path="daemon",
            res_model=Smap,
            params={QParamWhat: "smap"},
        )

    def xact_status(self, xact_id: str = "", xact_kind: str = "", daemon_id: str = "", only_running: bool = False) -> XactStatus:
        """
        Return status of an eXtended Action (xaction)

        Args:
            xact_id (str, optional): UUID of the xaction. Empty - all xactions
            xact_kind (str, optional): kind of the xaction. Empty - all kinds
            daemon_id (str, optional): return xactions only running on the daemon_id
            only_running (bool, optional): True - return only currently running xactions, False - include in the list also finished and aborted ones

        Returns:
            The xaction description

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        value = {"id": xact_id, "kind": xact_kind, "show_active": only_running, "node": daemon_id}
        params = {QParamWhat: "status"}

        return self._request_deserialize(
            HTTP_METHOD_GET,
            path="cluster",
            res_model=XactStatus,
            json=value,
            params=params,
        )

    def wait_for_xaction_finished(self, xact_id: str = "", xact_kind: str = "", daemon_id: str = "", timeout: int = 300):
        """
        Wait for an eXtended Action (xaction) to finish

        Args:
            xact_id (str, optional): UUID of the xaction. Empty - all xactions
            xact_kind (str, optional): kind of the xaction. Empty - all kinds
            daemon_id (str, optional): return xactions only running on the daemon_id
            timeout (int, optional): the maximum time to wait for the xaction, in seconds. Default timeout is 5 minutes

        Returns:
            None

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the xaction to finish
        """
        passed = 0
        sleep_time = probing_frequency(timeout)
        while True:
            if passed > timeout:
                raise Timeout("wait for xaction to finish")
            status = self.xact_status(xact_id=xact_id, xact_kind=xact_kind, daemon_id=daemon_id)
            if status.end_time != 0:
                break
            time.sleep(sleep_time)
            passed += sleep_time
            print(status)

    def xact_start(self, xact_kind: str = "", daemon_id: str = "", force: bool = False, buckets: List[Bck] = None) -> str:
        """
        Start an eXtended Action (xaction) and return its UUID

        Args:
            xact_kind (str, optional): `kind` of the xaction (for supported kinds, see api/apc/const.go). Empty - all kinds.
            daemon_id (str, optional): return xactions only running on the daemon_id
            force (bool, optional): override existing restrictions for a bucket (e.g., run LRU eviction even if the bucket has LRU disabled)
            buckets (List[Bck], optional): list of one or more buckets; applicable only for xactions that have bucket scope (for details and full enumeration, see xact/table.go)

        Returns:
            The running xaction UUID

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        value = {"kind": xact_kind, "node": daemon_id, "buckets": buckets}
        if force:
            value["ext"] = {"force": True}
        action = {"action": "start", "value": value}

        resp = self._request(
            HTTP_METHOD_PUT,
            path="cluster",
            json=action,
        )
        return resp.text
