#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from urllib.parse import urljoin
from typing import TypeVar, Type

from pydantic.tools import parse_raw_as
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
        """
        Returns: AIS cluster base url
        """
        return self._base_url

    @property
    def endpoint(self):
        """
        Returns: AIS cluster endpoint
        """
        return self._endpoint

    @property
    def session(self):
        """
        Returns: Active request session
        """
        return self._session

    def request_deserialize(
        self, method: str, path: str, res_model: Type[T], **kwargs
    ) -> T:
        """
        Make a request to the AIS cluster and deserialize the response to a defined type
        Args:
            method (str): HTTP method, e.g. POST, GET, PUT, DELETE
            path (str): URL path to call
            res_model Type[T]: Resulting type to which the response should be deserialized
            **kwargs (optional): Optional keyword arguments to pass with the call to request

        Returns:
            Parsed result of the call to the API, as res_model
        """
        resp = self.request(method, path, **kwargs)
        return parse_raw_as(res_model, resp.text)

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        """
        Make a request to the AIS cluster
        Args:
            method (str): HTTP method, e.g. POST, GET, PUT, DELETE
            path (str): URL path to call
            **kwargs (optional): Optional keyword arguments to pass with the call to request

        Returns:
            Raw response from the API
        """
        url = f"{self._base_url}/{path.lstrip('/')}"
        resp = self._session.request(
            method, url, headers={"Accept": "application/json"}, **kwargs
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp)
        return resp
