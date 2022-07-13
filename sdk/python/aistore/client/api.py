#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import TypeVar, Type, List
import requests
import time
from urllib.parse import urljoin
from pydantic.tools import parse_raw_as

from aistore.client.const import (HTTP_METHOD_GET, HTTP_METHOD_PUT, ProviderAIS, QParamWhat)
from aistore.client.bucket import Bucket
from aistore.client.cluster import Cluster
from aistore.client.errors import Timeout
from aistore.client.types import (Bck, XactStatus)
from aistore.client.utils import handle_errors, probing_frequency

T = TypeVar("T")


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

    def bucket(self, name: str, provider: str = ProviderAIS, ns: str = ""):
        """
        Factory constructor for bucket object. 
        Does not make any HTTP request, only instantiates a bucket object owned by the client.

        Args:
            name (str): Name of bucket (optional, defaults to "ais").
            provider (str): Provider of bucket (one of "ais", "aws", "gcp", ...).
        
        Returns:
            The bucket object created.
        """
        return Bucket(client=self, bck_name=name, provider=provider, ns=ns)

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
