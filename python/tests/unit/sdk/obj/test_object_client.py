import unittest
from unittest.mock import Mock, patch

import requests

from aistore.sdk.const import HTTP_METHOD_HEAD, HTTP_METHOD_GET, HEADER_RANGE
from aistore.sdk.request_client import RequestClient
from aistore.sdk.obj.object_client import ObjectClient


class TestObjectClient(unittest.TestCase):
    def setUp(self) -> None:
        self.request_client = Mock(spec=RequestClient)
        self.path = "/test/path"
        self.params = ["param1", "param2"]
        self.headers = {"header1": "req1", "header2": "req2"}
        self.response_headers = {"attr1": "resp1", "attr2": "resp2"}
        self.object_client = ObjectClient(
            self.request_client, self.path, self.params, self.headers
        )

    def test_get(self):
        self.get_exec_assert(
            self.object_client,
            stream=True,
            start_position=0,
            expected_headers=self.headers,
        )

    def test_get_no_headers(self):
        object_client = ObjectClient(self.request_client, self.path, self.params)
        self.get_exec_assert(
            object_client, stream=True, start_position=0, expected_headers={}
        )

    def test_get_with_options(self):
        # Test with stream=False, start position set
        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        self.request_client.request.return_value = mock_response
        # Any int
        start_position = 4
        self.headers[HEADER_RANGE] = f"bytes={start_position}-"

        res = self.object_client.get(stream=False, start_position=start_position)

        self.assert_get(expected_stream=False, expected_headers=self.headers)
        self.assertEqual(res, mock_response)
        mock_response.raise_for_status.assert_called_once()

    def get_exec_assert(self, object_client, stream, start_position, expected_headers):
        mock_response = Mock(spec=requests.Response)
        self.request_client.request.return_value = mock_response

        res = object_client.get(stream=stream, start_position=start_position)

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
