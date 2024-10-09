#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
from typing import Optional, Dict

import requests

from aistore.sdk.const import HTTP_METHOD_GET, HTTP_METHOD_HEAD, HEADER_RANGE
from aistore.sdk.obj.object_attributes import ObjectAttributes
from aistore.sdk.request_client import RequestClient


class ObjectClient:
    """
    ObjectClient is a simple wrapper around a given RequestClient that makes requests to an individual object.

    Args:
        request_client (RequestClient): The RequestClient used to make HTTP requests
        path (str): URL Path to the object
        params (List[str]): Query parameters for the request
        headers (Optional[Dict[str, str]]): HTTP request headers
    """

    def __init__(
        self,
        request_client: RequestClient,
        path: str,
        params: Dict[str, str],
        headers: Optional[Dict[str, str]] = None,
    ):
        self._request_client = request_client
        self._request_path = path
        self._request_params = params
        self._request_headers = headers

    def get(self, stream: bool, start_position: int) -> requests.Response:
        """
        Make a request to AIS to get the object content, optionally starting at a specific byte position.

        Args:
            stream (bool): If True, stream the response content.
            start_position (int): The byte position to start reading from.

        Returns:
            requests.Response: The response object from the request.
        """
        headers = self._request_headers.copy() if self._request_headers else {}
        if start_position != 0:
            headers[HEADER_RANGE] = f"bytes={start_position}-"

        resp = self._request_client.request(
            HTTP_METHOD_GET,
            path=self._request_path,
            params=self._request_params,
            stream=stream,
            headers=headers,
        )
        resp.raise_for_status()
        return resp

    def head(self) -> ObjectAttributes:
        """
        Make a head request to AIS to update and return only object attributes.

        Returns:
            `ObjectAttributes` containing metadata for this object.

        """
        resp = self._request_client.request(
            HTTP_METHOD_HEAD, path=self._request_path, params=self._request_params
        )
        return ObjectAttributes(resp.headers)
