import unittest
from unittest.mock import Mock, patch

import requests
from requests import PreparedRequest

from aistore.sdk.const import HTTP_METHOD_HEAD, HTTP_METHOD_GET, HEADER_RANGE
from aistore.sdk.request_client import RequestClient
from aistore.sdk.obj.object_client import ObjectClient
from aistore.sdk.errors import ErrObjNotFound

from tests.utils import cases


class TestObjectClient(unittest.TestCase):
    def setUp(self) -> None:
        self.request_client = Mock(spec=RequestClient)
        self.path = "/test/path"
        self.params = {"param1": "val1", "param2": "val2"}
        self.headers = {"header1": "req1", "header2": "req2"}
        self.response_headers = {"attr1": "resp1", "attr2": "resp2"}
        self.object_client = ObjectClient(
            self.request_client, self.path, self.params, self.headers
        )

    def test_get(self):
        self.get_exec_assert(
            self.object_client,
            stream=True,
            offset=0,
            expected_headers=self.headers,
        )

    def test_get_no_headers(self):
        object_client = ObjectClient(self.request_client, self.path, self.params)
        self.get_exec_assert(object_client, stream=True, offset=0, expected_headers={})

    @cases(
        {"byte_range_tuple": (None, None), "expected_range": "bytes=50-"},
        {"byte_range_tuple": (100, 200), "expected_range": "bytes=150-200"},
        {"byte_range_tuple": (100, None), "expected_range": "bytes=150-"},
        {"byte_range_tuple": (None, 200), "expected_range": "bytes=-150"},
    )
    def test_get_with_options(self, case):
        byte_range_tuple = case["byte_range_tuple"]
        expected_range = case["expected_range"]
        offset = 50

        object_client = ObjectClient(
            self.request_client,
            self.path,
            self.params,
            self.headers,
            byte_range_tuple,
        )

        expected_headers = self.headers.copy()
        if expected_range:
            expected_headers[HEADER_RANGE] = expected_range

        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        self.request_client.request.return_value = mock_response

        res = object_client.get(stream=False, offset=offset)

        self.assert_get(expected_stream=False, expected_headers=expected_headers)

        self.assertEqual(res, mock_response)
        mock_response.raise_for_status.assert_called_once()

        self.request_client.request.reset_mock()

    def get_exec_assert(self, object_client, stream, offset, expected_headers):
        mock_response = Mock(spec=requests.Response)
        self.request_client.request.return_value = mock_response

        res = object_client.get(stream=stream, offset=offset)

        self.assert_get(stream, expected_headers)
        self.assertEqual(res, mock_response)
        mock_response.raise_for_status.assert_called_once()

    def assert_get(self, expected_stream, expected_headers):
        self.request_client.request.assert_called_once_with(
            HTTP_METHOD_GET,
            path=self.path,
            params=self.params,
            stream=expected_stream,
            headers=expected_headers,
        )

    @patch("aistore.sdk.obj.object_client.ObjectAttributes", autospec=True)
    def test_head(self, mock_attr):
        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        self.request_client.request.return_value = mock_response

        res = self.object_client.head()

        self.assertEqual(mock_attr.return_value, res)
        self.request_client.request.assert_called_once_with(
            HTTP_METHOD_HEAD, path=self.path, params=self.params
        )
        mock_attr.assert_called_with(self.response_headers)

    @patch("aistore.sdk.obj.object_client.ObjectClient._retry_with_new_smap")
    def test_obj_not_found_retry(self, mock_retry_with_new_smap):
        """
        Test that the get method retries with a new smap when ErrObjNotFound is raised and caught.
        """
        self.request_client.request.side_effect = ErrObjNotFound(
            404, "Object not found", "object_url", Mock(PreparedRequest)
        )
        # pylint: disable=protected-access
        self.object_client._uname = (
            "test"  # will retry only if ObjectClient._uname is set
        )

        mock_retry_response = Mock(spec=requests.Response)
        mock_retry_with_new_smap.return_value = mock_retry_response

        res = self.object_client.get(stream=False)

        mock_retry_with_new_smap.assert_called_once_with(
            HTTP_METHOD_GET,
            path=self.path,
            params=self.params,
            stream=False,
            headers=self.headers,
        )

        self.assertEqual(res, mock_retry_response)

    def test_raises_err_obj_not_found(self):
        """
        Test that the get method raises ErrObjNotFound when no uname is provided.
        """
        object_client = ObjectClient(self.request_client, self.path, self.params)
        self.request_client.request.side_effect = ErrObjNotFound(
            404, "Object not found", "object_url", Mock(PreparedRequest)
        )

        with self.assertRaises(ErrObjNotFound):
            object_client.get(stream=False)
