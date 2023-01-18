import unittest
from unittest.mock import Mock, patch, mock_open

from requests import Response

from aistore.sdk.bucket import Bucket
from aistore.sdk.const import (
    HTTP_METHOD_HEAD,
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_GET,
    QParamArchpath,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
)
from aistore.sdk.object import Object
from aistore.sdk.types import ObjStream


class TestBucket(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock()
        self.bck_name = "bucket name"

        self.bck = Bucket(self.mock_client, self.bck_name)

        self.obj_name = "object name"
        self.object = Object(self.bck, self.obj_name)

    def test_properties(self):
        self.assertEqual(self.bck, self.object.bck)
        self.assertEqual(self.obj_name, self.object.obj_name)

    def test_head(self):
        self.object.head()

        self.bck.client.request.assert_called_with(
            HTTP_METHOD_HEAD,
            path=f"objects/{ self.bck.name }/{ self.obj_name }",
            params=self.bck.qparam,
        )

    def test_get_default_params(self):
        expected_request_params = self.bck.qparam
        expected_request_params[QParamArchpath] = ""
        self.get_exec_assert(expected_request_params)

    def test_get(self):
        archpath_param = "archpath"
        etl_name = "etl"
        expected_request_params = self.bck.qparam
        expected_request_params[QParamArchpath] = archpath_param
        expected_request_params["uuid"] = etl_name  # TODO -- FIXME: use QparamETLName
        self.get_exec_assert(
            expected_request_params,
            archpath=archpath_param,
            chunk_size=DEFAULT_CHUNK_SIZE + 1,
            etl_name=etl_name,
        )

    def get_exec_assert(self, expected_request_params, **kwargs):
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
            path=f"objects/{ self.bck_name }/{ self.obj_name }",
            params=expected_request_params,
            stream=True,
        )

    def test_put_conflicting_args(self):
        self.assertRaises(
            ValueError, self.object.put, path="something", content=b"something else"
        )

    def test_put_default_args(self):
        self.put_exec_assert(None)

    def test_put_path(self):
        path = "path/to/data"
        data = b"any-data-bytes"
        with patch("builtins.open", mock_open(read_data=data)):
            self.put_exec_assert(data, path=path)

    def test_put_content(self):
        data = b"user-supplied-bytes"
        self.put_exec_assert(data, content=data)

    def put_exec_assert(self, expected_data, **kwargs):
        request_path = f"/objects/{ self.bck_name }/{ self.obj_name }"
        expected_headers = "headers"
        mock_headers = Mock()
        mock_headers.headers = expected_headers
        self.mock_client.request.return_value = mock_headers
        res = self.object.put(**kwargs)
        self.assertEqual(expected_headers, res)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=request_path,
            params=self.bck.qparam,
            data=expected_data,
        )

    def test_delete(self):
        self.object.delete()
        path = f"objects/{self.bck_name}/{self.obj_name}"
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE, path=path, params=self.bck.qparam
        )
