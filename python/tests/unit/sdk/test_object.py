import unittest
from unittest.mock import Mock, patch, mock_open

from requests import Response
from requests.structures import CaseInsensitiveDict


from aistore.sdk.const import (
    HTTP_METHOD_HEAD,
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_GET,
    QPARAM_ARCHPATH,
    QPARAM_ARCHREGX,
    QPARAM_ARCHMODE,
    QPARAM_ETL_NAME,
    QPARAM_OBJ_APPEND,
    QPARAM_OBJ_APPEND_HANDLE,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    HEADER_CONTENT_LENGTH,
    HEADER_OBJECT_APPEND_HANDLE,
    AIS_CHECKSUM_VALUE,
    AIS_CHECKSUM_TYPE,
    AIS_ACCESS_TIME,
    AIS_VERSION,
    AIS_CUSTOM_MD,
    HTTP_METHOD_POST,
    ACT_PROMOTE,
    ACT_BLOB_DOWNLOAD,
    URL_PATH_OBJECTS,
    HEADER_OBJECT_BLOB_DOWNLOAD,
    HEADER_OBJECT_BLOB_CHUNK_SIZE,
    HEADER_OBJECT_BLOB_WORKERS,
)
from aistore.sdk.object import Object
from aistore.sdk.object_reader import ObjectReader
from aistore.sdk.archive_mode import ArchiveMode
from aistore.sdk.types import (
    ActionMsg,
    BlobMsg,
    PromoteAPIArgs,
    ArchiveSettings,
    BlobDownloadSettings,
)
from tests.const import SMALL_FILE_SIZE, ETL_NAME

BCK_NAME = "bucket_name"
OBJ_NAME = "object_name"
REQUEST_PATH = f"{URL_PATH_OBJECTS}/{BCK_NAME}/{OBJ_NAME}"


# pylint: disable=unused-variable, too-many-locals
class TestObject(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock()
        self.mock_bucket = Mock()
        self.mock_bucket.client = self.mock_client
        self.mock_bucket.name = BCK_NAME
        self.mock_writer = Mock()
        self.mock_bucket.qparam = {}
        self.expected_params = {}
        self.object = Object(self.mock_bucket, OBJ_NAME)

    def test_properties(self):
        self.assertEqual(self.mock_bucket, self.object.bucket)
        self.assertEqual(OBJ_NAME, self.object.name)

    def test_head(self):
        self.object.head()

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_HEAD,
            path=REQUEST_PATH,
            params=self.expected_params,
        )

    def test_get_default_params(self):
        self.get_exec_assert()

    def test_get(self):
        archpath_param = "archpath"
        chunk_size = "4mb"
        num_workers = 10
        self.expected_params[QPARAM_ARCHPATH] = archpath_param
        self.expected_params[QPARAM_ARCHREGX] = ""
        self.expected_params[QPARAM_ARCHMODE] = None
        self.expected_params[QPARAM_ETL_NAME] = ETL_NAME
        archive_settings = ArchiveSettings(archpath=archpath_param)
        blob_download_settings = BlobDownloadSettings(
            chunk_size=chunk_size,
            num_workers=num_workers,
        )
        self.get_exec_assert(
            archive_settings=archive_settings,
            chunk_size=3,
            etl_name=ETL_NAME,
            writer=self.mock_writer,
            blob_download_settings=blob_download_settings,
        )

    def test_get_archregex(self):
        regex = "regex"
        mode = ArchiveMode.PREFIX
        self.expected_params[QPARAM_ARCHPATH] = ""
        self.expected_params[QPARAM_ARCHREGX] = regex
        self.expected_params[QPARAM_ARCHMODE] = mode.value
        archive_settings = ArchiveSettings(regex=regex, mode=mode)
        self.get_exec_assert(archive_settings=archive_settings)

    def get_exec_assert(self, **kwargs):
        content = b"123456789"
        content_length = 9
        ais_check_val = "xyz"
        ais_check_type = "md5"
        ais_atime = "time string"
        ais_version = "3"
        custom_metadata_dict = {"key1": "val1", "key2": "val2"}
        custom_metadata = ", ".join(
            ["=".join(kv) for kv in custom_metadata_dict.items()]
        )
        resp_headers = CaseInsensitiveDict(
            {
                HEADER_CONTENT_LENGTH: content_length,
                AIS_CHECKSUM_VALUE: ais_check_val,
                AIS_CHECKSUM_TYPE: ais_check_type,
                AIS_ACCESS_TIME: ais_atime,
                AIS_VERSION: ais_version,
                AIS_CUSTOM_MD: custom_metadata,
            }
        )
        mock_response = Mock(Response)
        mock_response.headers = resp_headers
        mock_response.iter_content.return_value = content
        mock_response.raw = content
        expected_obj = ObjectReader(
            response_headers=resp_headers,
            stream=mock_response,
        )
        self.mock_client.request.return_value = mock_response

        res = self.object.get(**kwargs)
        blob_download_settings = kwargs.get(
            "blob_download_settings", BlobDownloadSettings()
        )
        chunk_size = blob_download_settings.chunk_size
        num_workers = blob_download_settings.num_workers
        headers = {}
        if chunk_size or num_workers:
            headers[HEADER_OBJECT_BLOB_DOWNLOAD] = "true"
        if chunk_size:
            headers[HEADER_OBJECT_BLOB_CHUNK_SIZE] = chunk_size
        if num_workers:
            headers[HEADER_OBJECT_BLOB_WORKERS] = num_workers

        self.assertEqual(expected_obj.raw(), res.raw())
        self.assertEqual(content_length, res.attributes.size)
        self.assertEqual(ais_check_type, res.attributes.checksum_type)
        self.assertEqual(ais_check_val, res.attributes.checksum_value)
        self.assertEqual(ais_atime, res.attributes.access_time)
        self.assertEqual(ais_version, res.attributes.obj_version)
        self.assertEqual(custom_metadata_dict, res.attributes.custom_metadata)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_GET,
            path=REQUEST_PATH,
            params=self.expected_params,
            stream=True,
            headers=headers,
        )

        # Use the object reader iterator to call the stream with the chunk size
        for _ in res:
            continue
        mock_response.iter_content.assert_called_with(
            chunk_size=kwargs.get("chunk_size", DEFAULT_CHUNK_SIZE)
        )

        if "writer" in kwargs:
            self.mock_writer.writelines.assert_called_with(res)

    def test_get_url(self):
        expected_res = "full url"
        archpath = "arch"
        self.mock_client.get_full_url.return_value = expected_res
        res = self.object.get_url(archpath=archpath, etl_name=ETL_NAME)
        self.assertEqual(expected_res, res)
        self.mock_client.get_full_url.assert_called_with(
            REQUEST_PATH, {QPARAM_ARCHPATH: archpath, QPARAM_ETL_NAME: ETL_NAME}
        )

    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.exists")
    def test_put_file(self, mock_exists, mock_is_file):
        mock_exists.return_value = True
        mock_is_file.return_value = True
        path = "any/filepath"
        data = b"bytes in the file"

        with patch("builtins.open", mock_open(read_data=data)):
            self.object.put_file(path)

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=REQUEST_PATH,
            params=self.expected_params,
            data=data,
        )

    def test_put_content(self):
        content = b"user-supplied-bytes"
        self.object.put_content(content)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=REQUEST_PATH,
            params=self.expected_params,
            data=content,
        )

    def test_append_content(self):
        content = b"content-to-append"
        expected_handle = "TEST_HANDLE"
        self.expected_params[QPARAM_OBJ_APPEND] = "append"
        self.expected_params[QPARAM_OBJ_APPEND_HANDLE] = ""
        resp_headers = CaseInsensitiveDict(
            {HEADER_OBJECT_APPEND_HANDLE: expected_handle}
        )
        mock_response = Mock(Response)
        mock_response.headers = resp_headers
        self.mock_client.request.return_value = mock_response

        next_handle = self.object.append_content(content)
        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_PUT,
            path=REQUEST_PATH,
            params=self.expected_params,
            data=content,
        )
        self.assertEqual(next_handle, expected_handle)

    def test_append_flush(self):
        expected_handle = ""
        prev_handle = "prev_handle"
        self.expected_params[QPARAM_OBJ_APPEND] = "flush"
        self.expected_params[QPARAM_OBJ_APPEND_HANDLE] = prev_handle
        resp_headers = CaseInsensitiveDict({})
        mock_response = Mock(Response)
        mock_response.headers = resp_headers
        self.mock_client.request.return_value = mock_response

        next_handle = self.object.append_content(b"", prev_handle, True)
        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_PUT,
            path=REQUEST_PATH,
            params=self.expected_params,
            data=b"",
        )
        self.assertEqual(next_handle, expected_handle)

    def test_promote_default_args(self):
        filename = "promoted file"
        expected_value = PromoteAPIArgs(source_path=filename, object_name=OBJ_NAME)
        self.promote_exec_assert(filename, expected_value)

    def test_promote(self):
        filename = "promoted file"
        target_id = "target node"
        recursive = True
        overwrite_dest = True
        delete_source = True
        src_not_file_share = True
        expected_value = PromoteAPIArgs(
            source_path=filename,
            object_name=OBJ_NAME,
            target_id=target_id,
            recursive=recursive,
            overwrite_dest=overwrite_dest,
            delete_source=delete_source,
            src_not_file_share=src_not_file_share,
        )
        self.promote_exec_assert(
            filename,
            expected_value,
            target_id=target_id,
            recursive=recursive,
            overwrite_dest=overwrite_dest,
            delete_source=delete_source,
            src_not_file_share=src_not_file_share,
        )

    def promote_exec_assert(self, filename, expected_value, **kwargs):
        request_path = f"{URL_PATH_OBJECTS}/{BCK_NAME}"
        expected_json = ActionMsg(
            action=ACT_PROMOTE, name=filename, value=expected_value.as_dict()
        ).dict()
        self.object.promote(filename, **kwargs)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=request_path,
            params=self.expected_params,
            json=expected_json,
        )

    def test_delete(self):
        self.object.delete()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE, path=REQUEST_PATH, params=self.expected_params
        )

    def test_blob_download_default_args(self):
        request_path = f"{URL_PATH_OBJECTS}/{BCK_NAME}"
        expected_blob_msg = BlobMsg(
            chunk_size=None,
            num_workers=None,
            latest=False,
        ).as_dict()
        expected_json = ActionMsg(
            action=ACT_BLOB_DOWNLOAD, name=OBJ_NAME, value=expected_blob_msg
        ).dict()
        self.object.blob_download()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=request_path,
            params=self.expected_params,
            json=expected_json,
        )

    def test_blob_download(self):
        request_path = f"{URL_PATH_OBJECTS}/{BCK_NAME}"
        chunk_size = SMALL_FILE_SIZE
        num_workers = 10
        latest = True
        expected_blob_msg = BlobMsg(
            chunk_size=chunk_size,
            num_workers=num_workers,
            latest=latest,
        ).as_dict()
        expected_json = ActionMsg(
            action=ACT_BLOB_DOWNLOAD, name=OBJ_NAME, value=expected_blob_msg
        ).dict()
        self.object.blob_download(
            num_workers=num_workers, chunk_size=chunk_size, latest=latest
        )
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=request_path,
            params=self.expected_params,
            json=expected_json,
        )
