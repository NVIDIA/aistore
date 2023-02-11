import unittest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, call

from requests import Response
from requests.structures import CaseInsensitiveDict

from aistore.sdk.const import (
    HTTP_METHOD_HEAD,
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_GET,
    QParamArchpath,
    QParamETLName,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    CONTENT_LENGTH,
    AIS_CHECKSUM_VALUE,
    AIS_CHECKSUM_TYPE,
    AIS_ACCESS_TIME,
    AIS_VERSION,
    AIS_CUSTOM_MD,
    HTTP_METHOD_POST,
    ACT_PROMOTE,
)
from aistore.sdk.object import Object
from aistore.sdk.types import ObjStream, ActionMsg, PromoteOptions, PromoteAPIArgs

BCK_NAME = "bucket name"
OBJ_NAME = "object name"
REQUEST_PATH = f"objects/{BCK_NAME}/{OBJ_NAME}"


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

    def test_head(self):
        self.object.head()

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_HEAD,
            path=REQUEST_PATH,
            params=self.expected_params,
        )

    def test_get_default_params(self):
        self.expected_params[QParamArchpath] = ""
        self.get_exec_assert()

    def test_get(self):
        archpath_param = "archpath"
        etl_name = "etl"
        self.expected_params[QParamArchpath] = archpath_param
        self.expected_params[QParamETLName] = etl_name
        self.get_exec_assert(
            archpath=archpath_param,
            chunk_size=DEFAULT_CHUNK_SIZE + 1,
            etl_name=etl_name,
            writer=self.mock_writer,
        )

    def get_exec_assert(self, **kwargs):
        content_length = 123
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
                CONTENT_LENGTH: content_length,
                AIS_CHECKSUM_VALUE: ais_check_val,
                AIS_CHECKSUM_TYPE: ais_check_type,
                AIS_ACCESS_TIME: ais_atime,
                AIS_VERSION: ais_version,
                AIS_CUSTOM_MD: custom_metadata,
            }
        )
        client_response = Response()
        client_response.headers = resp_headers
        expected_obj = ObjStream(
            response_headers=resp_headers,
            stream=client_response,
            chunk_size=kwargs.get("chunk_size", DEFAULT_CHUNK_SIZE),
        )
        self.mock_client.request.return_value = client_response

        res = self.object.get(**kwargs)

        self.assertEqual(expected_obj, res)
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
        )
        if "writer" in kwargs:
            self.mock_writer.writelines.assert_called_with(res)

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

    @patch("aistore.sdk.object.validate_directory")
    @patch("aistore.sdk.object.read_file_bytes")
    @patch("pathlib.Path.glob")
    def test_put_files(self, mock_glob, mock_read, mock_validate_dir):
        path = "directory"
        file_1_name = "file_1_name"
        file_2_name = "file_2_name"
        path_1 = Mock()
        path_1.is_file.return_value = True
        path_1.relative_to.return_value = file_1_name
        path_2 = Mock()
        path_2.relative_to.return_value = file_2_name
        path_2.is_file.return_value = True
        file_1_data = b"bytes in the first file"
        file_2_data = b"bytes in the second file"
        mock_glob.return_value = [path_1, path_2]
        expected_obj_names = [
            f"{self.object.name}/{file_1_name}",
            f"{self.object.name}/{file_2_name}",
        ]
        mock_read.side_effect = [file_1_data, file_2_data]

        res = self.object.put_files(path)

        mock_validate_dir.assert_called_with(path)
        self.assertEqual(expected_obj_names, res)
        expected_calls = [
            call(
                HTTP_METHOD_PUT,
                path=str(Path(REQUEST_PATH).joinpath(file_1_name)),
                params=self.expected_params,
                data=file_1_data,
            ),
            call(
                HTTP_METHOD_PUT,
                path=str(Path(REQUEST_PATH).joinpath(file_2_name)),
                params=self.expected_params,
                data=file_2_data,
            ),
        ]
        self.mock_client.request.assert_has_calls(expected_calls)

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
        options = PromoteOptions()
        expected_value = PromoteAPIArgs(source_path=filename, object_name=OBJ_NAME)
        self.promote_exec_assert(filename, options, expected_value)

    def test_promote(self):
        filename = "promoted file"
        target_id = "target node"
        recursive = True
        overwrite_dest = True
        delete_source = True
        src_not_file_share = True
        options = PromoteOptions(
            target_id=target_id,
            recursive=recursive,
            overwrite_dest=overwrite_dest,
            delete_source=delete_source,
            src_not_file_share=src_not_file_share,
        )
        expected_value = PromoteAPIArgs(
            source_path=filename,
            object_name=OBJ_NAME,
            target_id=target_id,
            recursive=recursive,
            overwrite_dest=overwrite_dest,
            delete_source=delete_source,
            src_not_file_share=src_not_file_share,
        )
        self.promote_exec_assert(filename, options, expected_value)

    def promote_exec_assert(self, filename, options, expected_value):
        request_path = f"/objects/{BCK_NAME}"
        expected_json = ActionMsg(
            action=ACT_PROMOTE, name=filename, value=expected_value.get_json()
        ).dict()
        self.object.promote(filename, options)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=request_path,
            params=self.expected_params,
            json=expected_json,
        )

    def test_delete(self):
        self.object.delete()
        path = f"objects/{BCK_NAME}/{OBJ_NAME}"
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE, path=path, params=self.expected_params
        )
