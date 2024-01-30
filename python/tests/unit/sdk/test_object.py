import unittest
from unittest.mock import Mock, patch, mock_open

from requests import Response
from requests.structures import CaseInsensitiveDict

from aistore.sdk.ais_source import AISSource
from aistore.sdk.const import (
    HTTP_METHOD_HEAD,
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_GET,
    QPARAM_ARCHPATH,
    QPARAM_ETL_NAME,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    HEADER_CONTENT_LENGTH,
    AIS_CHECKSUM_VALUE,
    AIS_CHECKSUM_TYPE,
    AIS_ACCESS_TIME,
    AIS_VERSION,
    AIS_CUSTOM_MD,
    HTTP_METHOD_POST,
    ACT_PROMOTE,
    URL_PATH_OBJECTS,
)
from aistore.sdk.object import Object
from aistore.sdk.object_reader import ObjectReader
from aistore.sdk.types import ActionMsg, PromoteAPIArgs

BCK_NAME = "bucket_name"
OBJ_NAME = "object_name"
REQUEST_PATH = f"{URL_PATH_OBJECTS}/{BCK_NAME}/{OBJ_NAME}"


# pylint: disable=unused-variable
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

    def test_ais_source(self):
        self.assertIsInstance(self.object, AISSource)

    def test_head(self):
        self.object.head()

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_HEAD,
            path=REQUEST_PATH,
            params=self.expected_params,
        )

    def test_get_default_params(self):
        self.expected_params[QPARAM_ARCHPATH] = ""
        self.get_exec_assert()

    def test_get(self):
        archpath_param = "archpath"
        etl_name = "etl"
        self.expected_params[QPARAM_ARCHPATH] = archpath_param
        self.expected_params[QPARAM_ETL_NAME] = etl_name
        self.get_exec_assert(
            archpath=archpath_param,
            chunk_size=3,
            etl_name=etl_name,
            writer=self.mock_writer,
        )

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
            headers={},
        )

        # Use the object reader iterator to call the stream with the chunk size
        for _ in res:
            continue
        mock_response.iter_content.assert_called_with(
            chunk_size=kwargs.get("chunk_size", DEFAULT_CHUNK_SIZE)
        )

        if "writer" in kwargs:
            self.mock_writer.writelines.assert_called_with(res)

    @patch("aistore.sdk.object.Object.get_url")
    def test_list_urls(self, mock_get_url):
        object_url = "single-object-url"
        mock_get_url.return_value = object_url
        etl_name = "test-etl"
        res = self.object.list_urls(etl_name=etl_name)
        self.assertEqual([object_url], list(res))
        mock_get_url.assert_called_with(etl_name=etl_name)

    def test_get_url(self):
        expected_res = "full url"
        archpath = "arch"
        etl = "test-etl"
        self.mock_client.get_full_url.return_value = expected_res
        res = self.object.get_url(archpath=archpath, etl_name=etl)
        self.assertEqual(expected_res, res)
        self.mock_client.get_full_url.assert_called_with(
            REQUEST_PATH, {QPARAM_ARCHPATH: archpath, QPARAM_ETL_NAME: etl}
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
