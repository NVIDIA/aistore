#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
from io import BufferedWriter
from typing import NewType
import requests

from aistore.sdk.const import (
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_PUT,
    QParamArchpath,
    QParamETLName,
    ACT_PROMOTE,
    HTTP_METHOD_POST,
    URL_PATH_OBJECTS,
)

from aistore.sdk.types import ObjStream, ActionMsg, PromoteAPIArgs
from aistore.sdk.utils import read_file_bytes, validate_file

Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=consider-using-with,unused-variable
class Object:
    """
    A class representing an object of a bucket bound to a client.

    Args:
        bucket (Bucket): Bucket to which this object belongs
        obj_name (str): name of object

    """

    def __init__(self, bucket: "Bucket", name: str):
        self._bucket = bucket
        self._client = bucket.client
        self._bck_name = bucket.name
        self._qparams = bucket.qparam
        self._name = name

    @property
    def bucket(self):
        """Bucket to which this object belongs"""
        return self._bucket

    @property
    def name(self):
        """Name of this object"""
        return self._name

    def head(self) -> Header:
        """
        Requests object properties.

        Returns:
            Response header with the object properties.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exceptions.HTTPError(404): The object does not exist
        """
        return self._client.request(
            HTTP_METHOD_HEAD,
            path=f"{URL_PATH_OBJECTS}/{ self._bck_name}/{ self.name }",
            params=self._qparams,
        ).headers

    def get(
        self,
        archpath: str = "",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_name: str = None,
        writer: BufferedWriter = None,
    ) -> ObjStream:
        """
        Reads an object

        Args:
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file
                from the archive
            chunk_size (int, optional): chunk_size to use while reading from stream
            etl_name (str, optional): Transforms an object based on ETL with etl_name
            writer (BufferedWriter, optional): User-provided writer for writing content output.
                User is responsible for closing the writer

        Returns:
            The stream of bytes to read an object or a file inside an archive.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = self._qparams.copy()
        params[QParamArchpath] = archpath
        if etl_name:
            params[QParamETLName] = etl_name
        resp = self._client.request(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_OBJECTS}/{ self._bck_name }/{ self.name }",
            params=params,
            stream=True,
        )
        obj_stream = ObjStream(
            stream=resp,
            response_headers=resp.headers,
            chunk_size=chunk_size,
        )
        if writer:
            writer.writelines(obj_stream)
        return obj_stream

    def put_content(self, content: bytes) -> Header:
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
        self._put_data(self.name, content)

    def put_file(self, path: str = None):
        """
        Puts a local file as an object to a bucket in AIS storage.

        Args:
            path (str): Path to local file

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            ValueError: The path provided is not a valid file
        """
        validate_file(path)
        self._put_data(self.name, read_file_bytes(path))

    def _put_data(self, obj_name: str, data: bytes):
        url = f"{URL_PATH_OBJECTS}/{ self._bck_name }/{ obj_name }"
        self._client.request(
            HTTP_METHOD_PUT,
            path=url,
            params=self._qparams,
            data=data,
        )

    # pylint: disable=too-many-arguments
    def promote(
        self,
        path: str,
        target_id: str = "",
        recursive: bool = False,
        overwrite_dest: bool = False,
        delete_source: bool = False,
        src_not_file_share: bool = False,
    ) -> Header:
        """
        Promotes a file or folder an AIS target can access to a bucket in AIS storage.
        These files can be either on the physical disk of an AIS target itself or on a network file system
        the cluster can access.
        See more info here: https://aiatscale.org/blog/2022/03/17/promote

        Args:
            path (str): Path to file or folder the AIS cluster can reach
            target_id (str, optional): Promote files from a specific target node
            recursive (bool, optional): Recursively promote objects from files in directories inside the path
            overwrite_dest (bool, optional): Overwrite objects already on AIS
            delete_source (bool, optional): Delete the source files when done promoting
            src_not_file_share (bool, optional): Optimize if the source is guaranteed to not be on a file share

        Returns:
            Object properties

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            AISError: Path does not exist on the AIS cluster storage
        """
        url = f"{URL_PATH_OBJECTS}/{ self._bck_name }"
        value = PromoteAPIArgs(
            source_path=path,
            object_name=self.name,
            target_id=target_id,
            recursive=recursive,
            overwrite_dest=overwrite_dest,
            delete_source=delete_source,
            src_not_file_share=src_not_file_share,
        ).get_json()
        json_val = ActionMsg(action=ACT_PROMOTE, name=path, value=value).dict()

        return self._client.request(
            HTTP_METHOD_POST, path=url, params=self._qparams, json=json_val
        ).headers

    def delete(self):
        """
        Delete an object from a bucket.

        Returns:
            None

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exceptions.HTTPError(404): The object does not exist
        """
        self._client.request(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_OBJECTS}/{ self._bck_name }/{ self.name }",
            params=self._qparams,
        )
