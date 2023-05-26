#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
from urllib.parse import urljoin, urlencode
from typing import TypeVar, Type, Any, Dict

import requests

from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
    HEADERS_KW,
)
from aistore.sdk.utils import handle_errors, decode_response
from aistore.version import __version__ as sdk_version

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
            res_model (Type[T]): Resulting type to which the response should be deserialized
            **kwargs (optional): Optional keyword arguments to pass with the call to request

        Returns:
            Parsed result of the call to the API, as res_model
        """
        resp = self.request(method, path, **kwargs)
        return decode_response(res_model, resp)

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
        if HEADERS_KW not in kwargs:
            kwargs[HEADERS_KW] = {}
        headers = kwargs.get(HEADERS_KW, {})
        headers[HEADER_CONTENT_TYPE] = JSON_CONTENT_TYPE
        headers[HEADER_USER_AGENT] = f"{USER_AGENT_BASE}/{sdk_version}"
        resp = self._session.request(
            method,
            url,
            **kwargs,
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp)
        return resp

    def get_full_url(self, path: str, params: Dict[str, Any]):
        """
        Get the full URL to the path on the cluster with the parameters given

        Args:
            path: Path on the cluster
            params: Query parameters to include

        Returns:
            URL including cluster base url and parameters

        """
        return f"{self._base_url}/{path.lstrip('/')}?{urlencode(params)}"
