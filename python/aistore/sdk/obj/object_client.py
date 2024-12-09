#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Tuple, Dict

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
        params (Dict[str, str]): Query parameters for the request
        headers (Optional[Dict[str, str]]): HTTP request headers
        byte_range (Optional[Tuple[Optional[int], Optional[int]]): Tuple representing the byte range
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        request_client: RequestClient,
        path: str,
        params: Dict[str, str],
        headers: Optional[Dict[str, str]] = None,
        byte_range: Optional[Tuple[Optional[int], Optional[int]]] = (None, None),
    ):
        self._request_client = request_client
        self._request_path = path
        self._request_params = params
        self._request_headers = headers
        self._byte_range = byte_range

    def get(self, stream: bool, offset: Optional[int] = None) -> requests.Response:
        """
        Make a request to AIS to get the object content, applying an optional offset.

        Args:
            stream (bool): If True, stream the response content.
            offset (int, optional): The offset in bytes to apply. If not provided, no offset
                                    is applied.

        Returns:
            requests.Response: The response object from the request.
        """
        headers = self._request_headers.copy() if self._request_headers else {}

        if offset:
            l, r = self._byte_range
            if l is not None:
                l = l + offset
            elif r is not None:
                r = r - offset
            else:
                l = offset

            headers[HEADER_RANGE] = f"bytes={l or ''}-{r or ''}"

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
