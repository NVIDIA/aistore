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
    QPARAM_ARCHPATH,
    QPARAM_ETL_NAME,
    QPARAM_LATEST,
    ACT_PROMOTE,
    HTTP_METHOD_POST,
    URL_PATH_OBJECTS,
    HEADER_RANGE,
    ACT_BLOB_DOWNLOAD,
    HEADER_OBJECT_BLOB_DOWNLOAD,
    HEADER_OBJECT_BLOB_WORKERS,
    HEADER_OBJECT_BLOB_CHUNK_SIZE,
)
from aistore.sdk.object_reader import ObjectReader
from aistore.sdk.types import ActionMsg, PromoteAPIArgs, BlobMsg
from aistore.sdk.utils import read_file_bytes, validate_file

Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=consider-using-with,unused-variable
class Object:
    """
    A class representing an object of a bucket bound to a client.

    Args:
        bucket (Bucket): Bucket to which this object belongs
        name (str): name of object

    """

    def __init__(self, bucket: "Bucket", name: str):
        self._bucket = bucket
        self._client = bucket.client
        self._bck_name = bucket.name
        self._qparams = bucket.qparam
        self._name = name
        self._object_path = f"{URL_PATH_OBJECTS}/{ self._bck_name}/{ self.name }"

    @property
    def bucket(self):
        """Bucket containing this object"""
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
            path=self._object_path,
            params=self._qparams,
        ).headers

    # pylint: disable=too-many-arguments
    def get(
        self,
        archpath: str = "",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_name: str = None,
        writer: BufferedWriter = None,
        latest: bool = False,
        byte_range: str = None,
        blob_chunk_size: str = None,
        blob_num_workers: str = None,
    ) -> ObjectReader:
        """
        Reads an object

        Args:
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file
                from the archive
            chunk_size (int, optional): chunk_size to use while reading from stream
            etl_name (str, optional): Transforms an object based on ETL with etl_name
            writer (BufferedWriter, optional): User-provided writer for writing content output
                User is responsible for closing the writer
            latest (bool, optional): GET the latest object version from the associated remote bucket
            byte_range (str, optional): Specify a specific data segment of the object for transfer, including
                both the start and end of the range (e.g. "bytes=0-499" to request the first 500 bytes)
            blob_chunk_size (str, optional):  Utilize built-in blob-downloader with the given chunk size in
                IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k;)
            blob_num_workers (str, optional): Utilize built-in blob-downloader with the given number of
                concurrent blob-downloading workers (readers)

        Returns:
            The stream of bytes to read an object or a file inside an archive.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = self._qparams.copy()
        params[QPARAM_ARCHPATH] = archpath
        if etl_name:
            params[QPARAM_ETL_NAME] = etl_name
        if latest:
            params[QPARAM_LATEST] = "true"

        if byte_range and (blob_chunk_size or blob_num_workers):
            raise ValueError("Cannot use Byte Range with Blob Download")
        headers = {}
        if blob_chunk_size or blob_num_workers:
            headers[HEADER_OBJECT_BLOB_DOWNLOAD] = "true"
        if blob_chunk_size:
            headers[HEADER_OBJECT_BLOB_CHUNK_SIZE] = blob_chunk_size
        if blob_num_workers:
            headers[HEADER_OBJECT_BLOB_WORKERS] = blob_num_workers
        if byte_range:
            # For range formatting, see the spec:
            # https://www.rfc-editor.org/rfc/rfc7233#section-2.1
            headers = {HEADER_RANGE: byte_range}

        resp = self._client.request(
            HTTP_METHOD_GET,
            path=self._object_path,
            params=params,
            stream=True,
            headers=headers,
        )
        obj_reader = ObjectReader(
            stream=resp,
            response_headers=resp.headers,
            chunk_size=chunk_size,
        )
        if writer:
            writer.writelines(obj_reader)
        return obj_reader

    def get_semantic_url(self):
        """
        Get the semantic URL to the object

        Returns:
            Semantic URL to get object
        """

        return f"{self.bucket.provider}://{self._bck_name}/{self._name}"

    def get_url(self, archpath: str = "", etl_name: str = None):
        """
        Get the full url to the object including base url and any query parameters

        Args:
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file
                from the archive
            etl_name (str, optional): Transforms an object based on ETL with etl_name

        Returns:
            Full URL to get object

        """
        params = self._qparams.copy()
        if archpath:
            params[QPARAM_ARCHPATH] = archpath
        if etl_name:
            params[QPARAM_ETL_NAME] = etl_name
        return self._client.get_full_url(self._object_path, params)

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
        ).as_dict()
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
            path=self._object_path,
            params=self._qparams,
        )

    def blob_download(
        self,
        chunk_size: int = None,
        num_workers: int = None,
        latest: bool = False,
    ) -> str:
        """
        A special facility to download very large remote objects a.k.a. BLOBs
        Returns job ID that for the blob download operation.

        Args:
            chunk_size (int): chunk size in bytes
            num_workers (int): number of concurrent blob-downloading workers (readers)
            latest (bool): GET the latest object version from the associated remote bucket

        Returns:
            Job ID (as str) that can be used to check the status of the operation

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
        """
        params = self._qparams.copy()
        value = BlobMsg(
            chunk_size=chunk_size,
            num_workers=num_workers,
            latest=latest,
        ).as_dict()
        json_val = ActionMsg(
            action=ACT_BLOB_DOWNLOAD, value=value, name=self.name
        ).dict()
        url = f"{URL_PATH_OBJECTS}/{ self._bck_name }"
        return self._client.request(
            HTTP_METHOD_POST, path=url, params=params, json=json_val
        ).text
