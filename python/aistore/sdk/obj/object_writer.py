#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from pathlib import Path
from typing import Dict
from requests import Response
from aistore.sdk.obj.obj_file.object_file import ObjectFileWriter
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import ActionMsg
from aistore.sdk.utils import validate_file
from aistore.sdk.const import (
    HTTP_METHOD_PATCH,
    HTTP_METHOD_PUT,
    QPARAM_NEW_CUSTOM,
    QPARAM_OBJ_APPEND,
    QPARAM_OBJ_APPEND_HANDLE,
    HEADER_OBJECT_APPEND_HANDLE,
)


class ObjectWriter:
    """
    Provide a way to write an object's contents and attributes.
    """

    def __init__(
        self, client: RequestClient, object_path: str, query_params: Dict[str, str]
    ):
        self._client = client
        self._object_path = object_path
        self.query_params = query_params

    def put_content(self, content: bytes) -> Response:
        """
        Puts bytes as an object to a bucket in AIS storage.

        Args:
            content (bytes): Bytes to put as an object.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        return self._put_data(data=content)

    def put_file(self, path: str or Path) -> Response:
        """
        Puts a local file as an object to a bucket in AIS storage.

        Args:
            path (str or Path): Path to local file

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            ValueError: The path provided is not a valid file
        """
        validate_file(path)
        with open(path, "rb") as f:
            return self._put_data(f)

    def _put_data(self, data) -> Response:
        return self._client.request(
            HTTP_METHOD_PUT,
            path=self._object_path,
            params=self.query_params,
            data=data,
        )

    def append_content(
        self, content: bytes, handle: str = "", flush: bool = False
    ) -> str:
        """
        Append bytes as an object to a bucket in AIS storage.

        Args:
            content (bytes): Bytes to append to the object.
            handle (str): Handle string to use for subsequent appends or flush (empty for the first append).
            flush (bool): Whether to flush and finalize the append operation, making the object accessible.

        Returns:
            handle (str): Handle string to pass for subsequent appends or flush.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exceptions.HTTPError(404): The object does not exist
        """
        params = self.query_params.copy()
        params[QPARAM_OBJ_APPEND] = "append" if not flush else "flush"
        params[QPARAM_OBJ_APPEND_HANDLE] = handle

        resp_headers = self._client.request(
            HTTP_METHOD_PUT,
            path=self._object_path,
            params=params,
            data=content,
        ).headers

        return resp_headers.get(HEADER_OBJECT_APPEND_HANDLE, "")

    def set_custom_props(
        self, custom_metadata: Dict[str, str], replace_existing: bool = False
    ) -> Response:
        """
        Set custom properties for the object.

        Args:
            custom_metadata (Dict[str, str]): Custom metadata key-value pairs.
            replace_existing (bool, optional): Whether to replace existing metadata. Defaults to False.
        """
        params = self.query_params.copy()
        if replace_existing:
            params[QPARAM_NEW_CUSTOM] = "true"

        json_val = ActionMsg(action="", value=custom_metadata).dict()

        return self._client.request(
            HTTP_METHOD_PATCH, path=self._object_path, params=params, json=json_val
        )

    def as_file(self, mode: str = "a") -> ObjectFileWriter:
        """
        Return a file-like object for writing object data.

        Args:
            mode (str): Specifies the mode in which the file is opened (defaults to 'a').
            - `'w'`: Write mode. Opens the object for writing, truncating any existing content.
                     Writing starts from the beginning of the object.
            - `'a'`: Append mode. Opens the object for appending. Existing content is preserved,
                     and writing starts from the end of the object.

        Returns:
            A file-like object for writing object data.

        Raises:
            ValueError: Invalid mode provided.
        """
        if mode not in {"w", "a"}:
            raise ValueError(f"Invalid mode: {mode}")

        return ObjectFileWriter(self, mode)
