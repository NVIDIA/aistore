#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from urllib.parse import urljoin

from pydantic.tools import parse_raw_as
from typing import TypeVar, Type
import requests

from aistore.sdk.utils import handle_errors

T = TypeVar("T")


# pylint: disable=unused-variable
class RequestClient:
    """
    Internal client for buckets, objects, jobs, etc. to use for making requests to an AIS cluster

    Args:
        endpoint (str): AIStore endpoint
    """

    def __init__(self, endpoint: str):
        self._endpoint = endpoint
        self._base_url = urljoin(endpoint, "v1")
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

    def request_deserialize(
        self, method: str, path: str, res_model: Type[T], **kwargs
    ) -> T:
        resp = self.request(method, path, **kwargs)
        return parse_raw_as(res_model, resp.text)

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self._base_url}/{path.lstrip('/')}"
        resp = self._session.request(
            method, url, headers={"Accept": "application/json"}, **kwargs
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp)
        return resp
