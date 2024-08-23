#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
from io import BufferedWriter
from typing import Dict, NewType

import requests

from aistore.sdk.const import (
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_PUT,
    QPARAM_ARCHPATH,
    QPARAM_ARCHREGX,
    QPARAM_ARCHMODE,
    QPARAM_OBJ_APPEND,
    QPARAM_OBJ_APPEND_HANDLE,
    QPARAM_ETL_NAME,
    QPARAM_LATEST,
    QPARAM_NEW_CUSTOM,
    ACT_PROMOTE,
    HTTP_METHOD_PATCH,
    HTTP_METHOD_POST,
    URL_PATH_OBJECTS,
    HEADER_RANGE,
    HEADER_OBJECT_APPEND_HANDLE,
    ACT_BLOB_DOWNLOAD,
    HEADER_OBJECT_BLOB_DOWNLOAD,
    HEADER_OBJECT_BLOB_WORKERS,
    HEADER_OBJECT_BLOB_CHUNK_SIZE,
)
from aistore.sdk.object_file import ObjectFile
from aistore.sdk.object_reader import ObjectReader
from aistore.sdk.types import (
    ActionMsg,
    PromoteAPIArgs,
    BlobMsg,
    ArchiveSettings,
    BlobDownloadSettings,
)
from aistore.sdk.utils import read_file_bytes, validate_file
from aistore.sdk.object_props import ObjectProps

Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=consider-using-with,unused-variable
class Object:
    """
    A class representing an object of a bucket bound to a client.

    Args:
        bucket (Bucket): Bucket to which this object belongs
        name (str): name of object
        size (int, optional): size of object in bytes
        props (ObjectProps, optional): Properties of object
    """

    def __init__(self, bucket: "Bucket", name: str, props: ObjectProps = None):
        self._bucket = bucket
        self._client = bucket.client
        self._bck_name = bucket.name
        self._qparams = bucket.qparam
        self._name = name
        self._object_path = f"{URL_PATH_OBJECTS}/{ self._bck_name}/{ self.name }"
        self._props = props

    @property
    def bucket(self):
        """Bucket containing this object."""
        return self._bucket

    @property
    def name(self) -> str:
        """Name of this object."""
        return self._name

    @property
    def props(self) -> ObjectProps:
        """Properties of this object."""
        return self._props

    def head(self) -> Header:
        """
        Requests object properties and returns headers. Updates props.

        Returns:
            Response header with the object properties.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exceptions.HTTPError(404): The object does not exist
        """
        headers = self._client.request(
            HTTP_METHOD_HEAD,
            path=self._object_path,
            params=self._qparams,
        ).headers
        self._props = ObjectProps(headers)
        return headers

    # pylint: disable=too-many-arguments
    def get(
        self,
        archive_settings: ArchiveSettings = None,
        blob_download_settings: BlobDownloadSettings = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_name: str = None,
        writer: BufferedWriter = None,
        latest: bool = False,
        byte_range: str = None,
    ) -> ObjectReader:
        """
        Creates and returns an ObjectReader with access to object contents and optionally writes to a provided writer.

        Args:
            archive_settings (ArchiveSettings, optional): Settings for archive extraction
            blob_download_settings (BlobDownloadSettings, optional): Settings for using blob download
            chunk_size (int, optional): chunk_size to use while reading from stream
            etl_name (str, optional): Transforms an object based on ETL with etl_name
            writer (BufferedWriter, optional): User-provided writer for writing content output
                User is responsible for closing the writer
            latest (bool, optional): GET the latest object version from the associated remote bucket
            byte_range (str, optional): Specify a specific data segment of the object for transfer, including
                both the start and end of the range (e.g. "bytes=0-499" to request the first 500 bytes)

        Returns:
            An ObjectReader which can be iterated over to stream chunks of object content or used to read all content
            directly.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = self._qparams.copy()
        headers = {}
        if archive_settings:
            if archive_settings.mode:
                archive_settings.mode = archive_settings.mode.value
            params[QPARAM_ARCHPATH] = archive_settings.archpath
            params[QPARAM_ARCHREGX] = archive_settings.regex
            params[QPARAM_ARCHMODE] = archive_settings.mode

        if blob_download_settings:
            headers[HEADER_OBJECT_BLOB_DOWNLOAD] = "true"
            headers[HEADER_OBJECT_BLOB_CHUNK_SIZE] = blob_download_settings.chunk_size
            headers[HEADER_OBJECT_BLOB_WORKERS] = blob_download_settings.num_workers
        if etl_name:
            params[QPARAM_ETL_NAME] = etl_name
        if latest:
            params[QPARAM_LATEST] = "true"

        if byte_range and blob_download_settings:
            raise ValueError("Cannot use Byte Range with Blob Download")

        if byte_range:
            # For range formatting, see the spec:
            # https://www.rfc-editor.org/rfc/rfc7233#section-2.1
            headers = {HEADER_RANGE: byte_range}

        obj_reader = ObjectReader(
            client=self._client,
            path=self._object_path,
            params=params,
            headers=headers,
            chunk_size=chunk_size,
        )
        if writer:
            writer.writelines(obj_reader)
        return obj_reader

    def as_file(
        self,
        max_resume: int = 5,
        archive_settings: ArchiveSettings = None,
        blob_download_settings: BlobDownloadSettings = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_name: str = None,
        writer: BufferedWriter = None,
        latest: bool = False,
        byte_range: str = None,
    ) -> ObjectFile:  # pylint: disable=too-many-arguments
        """
        Creates an `ObjectFile` for reading object data in chunks with support for
        resuming and retrying from the last known position in the case the object stream
        is prematurely closed due to an unexpected error.

        Args:
            max_resume (int, optional): If streaming object contents is interrupted, this
                defines the maximum number of attempts to resume the connection before
                raising an exception.
            archive_settings (ArchiveSettings, optional): Settings for archive extraction.
            blob_download_settings (BlobDownloadSettings, optional): Settings for using blob
                download (e.g., chunk size, workers).
            chunk_size (int, optional): The size of chunks to use while reading from the stream.
            etl_name (str, optional): Name of the ETL (Extract, Transform, Load) transformation
                to apply during the get operation.
            writer (BufferedWriter, optional): A writer for writing content output. User is
                responsible for closing the writer.
            latest (bool, optional): Whether to get the latest version of the object from
                a remote bucket (if applicable).
            byte_range (str, optional): Specify a byte range to fetch a segment of the object
                (e.g., "bytes=0-499" for the first 500 bytes).

        Returns:
            ObjectFile: A file-like object that can be used to read the object content.

        Raises:
            requests.RequestException: An ambiguous exception occurred while handling the request.
            requests.ConnectionError: A connection error occurred.
            requests.ConnectionTimeout: The connection to AIStore timed out.
            requests.ReadTimeout: Waiting for a response from AIStore timed out.
            requests.exceptions.HTTPError(404): The object does not exist.
        """
        object_reader = self.get(
            archive_settings=archive_settings,
            blob_download_settings=blob_download_settings,
            chunk_size=chunk_size,
            etl_name=etl_name,
            writer=writer,
            latest=latest,
            byte_range=byte_range,
        )
        return ObjectFile(object_reader, max_resume=max_resume)

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
    ) -> str:
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
            Job ID (as str) that can be used to check the status of the operation, or empty if job is done synchronously

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
        ).text

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

        url = f"{URL_PATH_OBJECTS}/{ self._bck_name }/{ self.name }"
        params = self._qparams.copy()
        params[QPARAM_OBJ_APPEND] = "append" if not flush else "flush"
        params[QPARAM_OBJ_APPEND_HANDLE] = handle

        resp_headers = self._client.request(
            HTTP_METHOD_PUT,
            path=url,
            params=params,
            data=content,
        ).headers

        return resp_headers.get(HEADER_OBJECT_APPEND_HANDLE, "")

    def set_custom_props(
        self, custom_metadata: Dict[str, str], replace_existing: bool = False
    ):
        """
        Set custom properties for the object.

        Args:
            custom_metadata (Dict[str, str]): Custom metadata key-value pairs.
            replace_existing (bool, optional): Whether to replace existing metadata. Defaults to False.
        """
        params = self._qparams.copy()
        if replace_existing:
            params[QPARAM_NEW_CUSTOM] = "true"

        url = f"{URL_PATH_OBJECTS}/{self._bck_name}/{self.name}"

        json_val = ActionMsg(action="", value=custom_metadata).dict()

        self._client.request(HTTP_METHOD_PATCH, path=url, params=params, json=json_val)
