#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
from dataclasses import dataclass
from io import BufferedWriter
from pathlib import Path
from typing import Dict

from requests import Response
from requests.structures import CaseInsensitiveDict

from aistore.sdk.archive_config import ArchiveConfig
from aistore.sdk.blob_download_config import BlobDownloadConfig
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
from aistore.sdk.provider import Provider
from aistore.sdk.obj.object_client import ObjectClient
from aistore.sdk.obj.object_reader import ObjectReader
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import (
    ActionMsg,
    PromoteAPIArgs,
    BlobMsg,
)
from aistore.sdk.utils import validate_file
from aistore.sdk.obj.object_props import ObjectProps


@dataclass
class BucketDetails:
    """
    Metadata about a bucket, used by objects within that bucket.
    """

    name: str
    provider: Provider
    qparams: Dict[str, str]


class Object:
    """
    Provides methods for interacting with an object in AIS.

    Args:
        client (RequestClient): Client used for all http requests.
        bck_details (BucketDetails): Metadata about the bucket to which this object belongs.
        name (str): Name of the object.
        props (ObjectProps, optional): Properties of the object, as updated by head(), optionally pre-initialized.
    """

    def __init__(
        self,
        client: RequestClient,
        bck_details: BucketDetails,
        name: str,
        props: ObjectProps = None,
    ):
        self._client = client
        self._bck_details = bck_details
        self._bck_path = f"{URL_PATH_OBJECTS}/{ bck_details.name}"
        self._object_path = f"{self._bck_path}/{name}"
        self._name = name
        self._props = props

    @property
    def bucket_name(self) -> str:
        """Name of the bucket where this object resides."""
        return self._bck_details.name

    @property
    def bucket_provider(self) -> Provider:
        """Provider of the bucket where this object resides (e.g. ais, s3, gcp)."""
        return self._bck_details.provider

    @property
    def query_params(self) -> Dict[str, str]:
        """Query params used as a base for constructing all requests for this object."""
        return self._bck_details.qparams

    @property
    def name(self) -> str:
        """Name of this object."""
        return self._name

    @property
    def props(self) -> ObjectProps:
        """Properties of this object."""
        return self._props

    def head(self) -> CaseInsensitiveDict:
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
            params=self.query_params,
        ).headers
        self._props = ObjectProps(headers)
        return headers

    # pylint: disable=too-many-arguments
    def get(
        self,
        archive_config: ArchiveConfig = None,
        blob_download_config: BlobDownloadConfig = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_name: str = None,
        writer: BufferedWriter = None,
        latest: bool = False,
        byte_range: str = None,
    ) -> ObjectReader:
        """
        Creates and returns an ObjectReader with access to object contents and optionally writes to a provided writer.

        Args:
            archive_config (ArchiveConfig, optional): Settings for archive extraction
            blob_download_config (BlobDownloadConfig, optional): Settings for using blob download
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
        params = self.query_params.copy()
        headers = {}
        if archive_config:
            if archive_config.mode:
                params[QPARAM_ARCHMODE] = archive_config.mode.value
            params[QPARAM_ARCHPATH] = archive_config.archpath
            params[QPARAM_ARCHREGX] = archive_config.regex

        if blob_download_config:
            headers[HEADER_OBJECT_BLOB_DOWNLOAD] = "true"
            headers[HEADER_OBJECT_BLOB_CHUNK_SIZE] = blob_download_config.chunk_size
            headers[HEADER_OBJECT_BLOB_WORKERS] = blob_download_config.num_workers
        if etl_name:
            params[QPARAM_ETL_NAME] = etl_name
        if latest:
            params[QPARAM_LATEST] = "true"

        if byte_range and blob_download_config:
            raise ValueError("Cannot use Byte Range with Blob Download")

        if byte_range:
            # For range formatting, see the spec:
            # https://www.rfc-editor.org/rfc/rfc7233#section-2.1
            headers = {HEADER_RANGE: byte_range}

        obj_client = ObjectClient(
            request_client=self._client,
            path=self._object_path,
            params=params,
            headers=headers,
        )

        obj_reader = ObjectReader(
            object_client=obj_client,
            chunk_size=chunk_size,
        )
        if writer:
            writer.writelines(obj_reader)
        return obj_reader

    def get_semantic_url(self) -> str:
        """
        Get the semantic URL to the object

        Returns:
            Semantic URL to get object
        """

        return f"{self.bucket_provider.value}://{self.bucket_name}/{self._name}"

    def get_url(self, archpath: str = "", etl_name: str = None) -> str:
        """
        Get the full url to the object including base url and any query parameters

        Args:
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file
                from the archive
            etl_name (str, optional): Transforms an object based on ETL with etl_name

        Returns:
            Full URL to get object

        """
        params = self.query_params.copy()
        if archpath:
            params[QPARAM_ARCHPATH] = archpath
        if etl_name:
            params[QPARAM_ETL_NAME] = etl_name
        return self._client.get_full_url(self._object_path, params)

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
            HTTP_METHOD_POST,
            path=self._bck_path,
            params=self.query_params,
            json=json_val,
        ).text

    def delete(self) -> Response:
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
        return self._client.request(
            HTTP_METHOD_DELETE,
            path=self._object_path,
            params=self.query_params,
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
        params = self.query_params.copy()
        value = BlobMsg(
            chunk_size=chunk_size,
            num_workers=num_workers,
            latest=latest,
        ).as_dict()
        json_val = ActionMsg(
            action=ACT_BLOB_DOWNLOAD, value=value, name=self.name
        ).dict()
        return self._client.request(
            HTTP_METHOD_POST, path=self._bck_path, params=params, json=json_val
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
