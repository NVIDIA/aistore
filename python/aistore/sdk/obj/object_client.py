#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Tuple, Dict

import requests

from aistore.sdk.const import HTTP_METHOD_GET, HTTP_METHOD_HEAD, HEADER_RANGE
from aistore.sdk.obj.object_attributes import ObjectAttributes
from aistore.sdk.request_client import RequestClient
from aistore.sdk.errors import ErrObjNotFound


class ObjectClient:
    """
    ObjectClient is a simple wrapper around a given RequestClient that makes requests to an individual object.

    Args:
        request_client (RequestClient): The RequestClient used to make HTTP requests
        path (str): URL Path to the object
        params (Dict[str, str]): Query parameters for the request
        headers (Optional[Dict[str, str]]): HTTP request headers
        byte_range (Optional[Tuple[Optional[int], Optional[int]]): Tuple representing the byte range
        uname (Optional[str]): Unique (namespaced) name of the object (used for determining the target node)
    """

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        request_client: RequestClient,
        path: str,
        params: Dict[str, str],
        headers: Optional[Dict[str, str]] = None,
        byte_range: Optional[Tuple[Optional[int], Optional[int]]] = (None, None),
        uname: Optional[str] = None,
    ):
        self._request_client = request_client
        self._request_path = path
        self._request_params = params
        self._request_headers = headers
        self._byte_range = byte_range
        self._uname = uname
        if uname:
            self._initialize_target_client()

    @property
    def path(self) -> str:
        """
        Get the URL path for the object.

        Returns:
            str: The URL path for the object.
        """
        return self._request_path

    def _initialize_target_client(self, force: bool = False):
        """
        Initialize a new RequestClient pointing to the target node for the object.
        """
        smap = self._request_client.get_smap(force)
        target_node = smap.get_target_for_object(self._uname)
        new_client = self._request_client.clone(
            base_url=target_node.public_net.direct_url
        )
        self._request_client = new_client

    def _retry_with_new_smap(self, method: str, **kwargs) -> requests.Response:
        """
        Retry the request with the latest `smap` if a 404 error is encountered.

        Args:
            method (str): HTTP method (e.g., GET, HEAD).
            **kwargs: Additional arguments to pass to the request.

        Returns:
            requests.Response: The response object from the retried request.
        """
        if self._uname:
            # Force update the smap
            self._initialize_target_client(force=True)

        # Retry the request
        return self._request_client.request(method, **kwargs)

    def _make_get_request(
        self, headers: Dict[str, str], stream: bool
    ) -> requests.Response:
        """
        Make a GET request with retry on object not found.

        Args:
            headers (Dict[str, str]): Request headers including any Range header.
            stream (bool): Whether to stream the response.

        Returns:
            requests.Response: The response object.

        Raises:
            ErrObjNotFound: If the object is not found and cannot be retried.
        """
        try:
            resp = self._request_client.request(
                HTTP_METHOD_GET,
                path=self._request_path,
                params=self._request_params,
                stream=stream,
                headers=headers,
            )
            resp.raise_for_status()
            return resp

        except ErrObjNotFound:
            if self._uname:
                return self._retry_with_new_smap(
                    HTTP_METHOD_GET,
                    path=self._request_path,
                    params=self._request_params,
                    stream=stream,
                    headers=headers,
                )
            raise

    def get(self, stream: bool, offset: Optional[int] = None) -> requests.Response:
        """
        Fetch object content from AIS, applying an optional offset.

        Args:
            stream (bool): If True, stream the response content.
            offset (int, optional): Byte offset for reading the object. Defaults to None.

        Returns:
            requests.Response: The response object containing the content.

        Raises:
            ErrObjNotFound: If the object is not found and cannot be retried.
            requests.RequestException: For network-related errors.
        """
        headers = self._request_headers.copy() if self._request_headers else {}

        if offset:
            l, r = self._byte_range
            if l is not None:
                l += offset
            elif r is not None:
                r -= offset
            else:
                l = offset

            headers[HEADER_RANGE] = f"bytes={l or ''}-{r or ''}"

        return self._make_get_request(headers, stream)

    def get_chunk(self, start: int, end: int) -> bytes:
        """
        Fetch a specific byte range of the object as a chunk.

        Args:
            start (int): Start byte offset (inclusive).
            end (int): End byte offset (exclusive).

        Returns:
            bytes: The chunk content.

        Raises:
            ErrObjNotFound: If the object is not found and cannot be retried.
            requests.RequestException: For network-related errors.
        """
        headers = self._request_headers.copy() if self._request_headers else {}
        headers[HEADER_RANGE] = f"bytes={start}-{end - 1}"
        return self._make_get_request(headers, stream=False).content

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
