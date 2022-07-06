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
    ACT_LIST,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_PUT,
    ProviderAIS,
    QParamArchpath,
    QparamPrimaryReadyReb,
    QParamProvider,
    QParamWhat
)
from aistore.client.bucket import Bucket
from aistore.client.errors import Timeout
from aistore.client.types import (ActionMsg, Bck, ObjStream, Smap, XactStatus)
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

    def request_deserialize(self, method: str, path: str, res_model: Type[T], **kwargs) -> T:
        resp = self.request(method, path, **kwargs)
        return parse_raw_as(res_model, resp.text)

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{ self.base_url }/{ path.lstrip('/') }"
        resp = requests.request(method, url, headers={"Accept": "application/json"}, **kwargs)
        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp)
        return resp

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

        return self.request_deserialize(
            HTTP_METHOD_GET,
            path="buckets",
            res_model=List[Bck],
            json=action,
            params=params,
        )

    def is_aistore_running(self) -> bool:
        """
        Returns cluster status.
        
        Args:
            None
        
        Returns: 
            True if cluster is ready or False if cluster is still setting up.
        """

        # compare with AIS Go API (api/cluster.go) for additional supported options
        params = {QparamPrimaryReadyReb: "true"}
        try:
            resp = self.request(HTTP_METHOD_GET, path="health", params=params)
            return resp.ok
        except Exception:
            return False

    def head_object(self, bck_name: str, obj_name: str, provider: str = ProviderAIS) -> Header:
        """
        Requests object properties.

        Args:
            bck_name (str): Name of the new bucket.
            obj_name (str): Name of an object in the bucket.
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
        return self.request(
            HTTP_METHOD_HEAD,
            path=f"objects/{ bck_name }/{ obj_name }",
            params=params,
        ).headers

    def get_object(self, bck_name: str, obj_name: str, provider: str = ProviderAIS, archpath: str = "", chunk_size: int = 1) -> ObjStream:
        """
        Reads an object.

        Args:
            bck_name (str): Name of a bucket.
            obj_name (str): Name of an object in the bucket.
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file from the archive.
            chunk_size (int, optional): Chunk size to use while reading from stream.

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

    def put_object(self, bck_name: str, obj_name: str, path: str, provider: str = ProviderAIS) -> Header:
        """
        Puts a local file as an object to a bucket in AIS storage.

        Args:
            bck_name (str): Name of a bucket.
            obj_name (str): Name of an object in the bucket.
            path (str): path to local file.
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
            return self.request(
                HTTP_METHOD_PUT,
                path=url,
                params=params,
                data=data,
            ).headers

    def delete_object(self, bck_name: str, obj_name: str, provider: str = ProviderAIS):
        """
        Delete an object from a bucket.

        Args:
            bck_name (str): Name of the new bucket.
            obj_name (str): Name of an object in the bucket.
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
        self.request(
            HTTP_METHOD_DELETE,
            path=f"objects/{ bck_name }/{ obj_name }",
            params=params,
        )

    def get_cluster_info(self) -> Smap:
        """
        Returns state of AIS cluster, including the detailed information about its nodes

        Args:
            None

        Returns:
            aistore.msg.Smap representing cluster information.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        return self.request_deserialize(
            HTTP_METHOD_GET,
            path="daemon",
            res_model=Smap,
            params={QParamWhat: "smap"},
        )

    def xact_status(self, xact_id: str = "", xact_kind: str = "", daemon_id: str = "", only_running: bool = False) -> XactStatus:
        """
        Return status of an eXtended Action (xaction)

        Args:
            xact_id (str, optional): UUID of the xaction. Empty - all xactions.
            xact_kind (str, optional): Kind of the xaction. Empty - all kinds.
            daemon_id (str, optional): Return xactions only running on the daemon_id.
            only_running (bool, optional): True - return only currently running xactions, False - include in the list also finished and aborted ones.

        Returns:
            The xaction description.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        value = {"id": xact_id, "kind": xact_kind, "show_active": only_running, "node": daemon_id}
        params = {QParamWhat: "status"}

        return self.request_deserialize(
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
            xact_id (str, optional): UUID of the xaction. Empty - all xactions.
            xact_kind (str, optional): Kind of the xaction. Empty - all kinds.
            daemon_id (str, optional): Return xactions only running on the daemon_id.
            timeout (int, optional): The maximum time to wait for the xaction, in seconds. Default timeout is 5 minutes.

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
        Start an eXtended Action (xaction) and return its UUID.

        Args:
            xact_kind (str, optional): Kind of the xaction (for supported kinds, see api/apc/const.go). Empty - all kinds.
            daemon_id (str, optional): Return xactions only running on the daemon_id.
            force (bool, optional): Override existing restrictions for a bucket (e.g., run LRU eviction even if the bucket has LRU disabled).
            buckets (List[Bck], optional): List of one or more buckets; applicable only for xactions that have bucket scope (for details and full enumeration, see xact/table.go).

        Returns:
            The running xaction UUID.

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

        resp = self.request(
            HTTP_METHOD_PUT,
            path="cluster",
            json=action,
        )
        return resp.text

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
