import unittest
from unittest.mock import Mock, patch, mock_open

from requests import Response

from aistore.sdk.const import (
    HTTP_METHOD_HEAD,
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_GET,
    QParamArchpath,
    QParamETLName,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
)
from aistore.sdk.object import Object
from aistore.sdk.types import ObjStream


class TestObject(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock()
        self.bck_name = "bucket name"
        self.obj_name = "object name"
        self.mock_bucket = Mock()
        self.mock_bucket.client = self.mock_client
        self.mock_bucket.name = self.bck_name
        self.mock_bucket.qparam = {}
        self.expected_params = {}
        self.object = Object(self.mock_bucket, self.obj_name)

    def test_properties(self):
        self.assertEqual(self.mock_bucket, self.object.bucket)
        self.assertEqual(self.obj_name, self.object.name)

    def test_head(self):
        self.object.head()

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_HEAD,
            path=f"objects/{self.bck_name}/{self.obj_name}",
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
        )

    def get_exec_assert(self, **kwargs):
        content_length = 123
        ais_check_val = "xyz"
        ais_check_type = "md5"
        resp_headers = {
            "content-length": content_length,
            "ais-checksum-value": ais_check_val,
            "ais-checksum-type": ais_check_type,
        }
        client_response = Response()
        client_response.headers = resp_headers
        expected_obj = ObjStream(
            content_length=content_length,
            e_tag=ais_check_val,
            e_tag_type=ais_check_type,
            stream=client_response,
            chunk_size=kwargs.get("chunk_size", DEFAULT_CHUNK_SIZE),
        )
        self.mock_client.request.return_value = client_response

        res = self.object.get(**kwargs)

        self.assertEqual(expected_obj, res)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_GET,
            path=f"objects/{self.bck_name}/{self.obj_name}",
            params=self.expected_params,
            stream=True,
        )

    def test_put_conflicting_args(self):
        self.assertRaises(
            ValueError, self.object.put, path="something", content=b"something else"
        )

    def test_put_default_args(self):
        self.put_exec_assert(expected_data=None)

    def test_put_path(self):
        path = "path/to/data"
        data = b"any-data-bytes"
        with patch("builtins.open", mock_open(read_data=data)):
            self.put_exec_assert(expected_data=data, path=path)

    def test_put_content(self):
        data = b"user-supplied-bytes"
        self.put_exec_assert(expected_data=data, content=data)

    def put_exec_assert(self, expected_data, **kwargs):
        request_path = f"/objects/{self.bck_name}/{self.obj_name}"
        expected_headers = "headers"
        mock_headers = Mock()
        mock_headers.headers = expected_headers
        self.mock_client.request.return_value = mock_headers
        res = self.object.put(**kwargs)
        self.assertEqual(expected_headers, res)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=request_path,
            params=self.expected_params,
            data=expected_data,
        )

    def test_delete(self):
        self.object.delete()
        path = f"objects/{self.bck_name}/{self.obj_name}"
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE, path=path, params=self.expected_params
        )
