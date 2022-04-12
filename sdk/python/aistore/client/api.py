#
# Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
#

from typing import TypeVar, Type, List
import requests
from urllib.parse import urljoin
from pydantic.tools import parse_raw_as

from .const import (
    HTTP_METHOD_GET,
    QParamArchpath,
    QParamProvider,
    ProviderAIS,
)
from .msg import ActionMsg, Bck

T = TypeVar("T")


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
        url = f"{ self.base_url }/{ path.lstrip('/') }"
        resp = requests.request(method, url, headers={"Accept": "application/json"}, **kwargs)
        resp.raise_for_status()
        return parse_raw_as(res_model, resp.text)

    def list_buckets(self, provider: str = ProviderAIS):
        """
        Returns list of buckets in AIStore cluster

        Args:
            provider (str, optional): Name of bucket provider, one of "ais", "aws", "gcp" or "az". Defaults to "" => all providers.

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
        url = "{}/buckets/{}".format(self.base_url, bck_name)
        bck = Bck(name=bck_name, **kwargs)
        params = {QParamProvider: bck.provider}
        action = ActionMsg(action="create-bck").dict()
        return requests.post(url=url, json=action, params=params)

    def destroy_bucket(self, bck_name, **kwargs):
        url = "{}/buckets/{}".format(self.base_url, bck_name)
        bck = Bck(name=bck_name, **kwargs)
        params = {QParamProvider: bck.provider}
        action = ActionMsg(action="destroy-bck").dict()
        return requests.delete(url=url, json=action, params=params)

    def list_objects(self, bck_name, **kwargs):
        url = "{}/buckets/{}".format(self.base_url, bck_name)
        bck = Bck(name=bck_name, **kwargs)

        value = None
        if "prefix" in kwargs:
            value = {"prefix": kwargs.get("prefix")}
        params = ActionMsg(action="list", value=value)

        resp = requests.get(
            url=url,
            data=params.json(),
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 200:
            entries = resp.json()["entries"]
            return entries

        return resp.json()

    def get_object(self, bck_name, object_name, **kwargs):
        url = "{}/objects/{}/{}".format(self.base_url, bck_name, object_name)
        bck = Bck(name=bck_name, **kwargs)
        params = {}
        if bck.provider != "":
            params[QParamProvider] = bck.provider
        if "archpath" in kwargs:
            params[QParamArchpath] = kwargs.get("archpath")
        if "transform_id" in kwargs:
            params["uuid"] = kwargs.get("transform_id")
        return requests.get(url=url, params=params)

    def put_object(self, bck_name, object_name, path, **kwargs):
        url = "{}/objects/{}/{}".format(self.base_url, bck_name, object_name)
        bck = Bck(name=bck_name, **kwargs)
        params = {}
        if bck.provider != "":
            params[QParamProvider] = bck.provider
        with open(path, "rb") as data:
            return requests.put(url=url, params=params, data=data)

    def get_cluster_info(self):
        url = "{}/daemon".format(self.base_url)
        return requests.get(url, params={"what": "smap"})
