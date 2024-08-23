import unittest
from unittest.mock import MagicMock, patch, Mock
import requests

from aistore.sdk.object_reader import ObjectReader
from aistore.sdk.request_client import RequestClient
from aistore.sdk.const import HTTP_METHOD_GET, HTTP_METHOD_HEAD
from aistore.sdk.object_attributes import ObjectAttributes


class TestObjectReader(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock(spec=RequestClient)
        self.path = "/test/path"
        self.params = ["param1", "param2"]
        self.headers = {"header1": "req1", "header2": "req2"}
        self.response_headers = {"attr1": "resp1", "attr2": "resp2"}
        self.chunk_size = 1024
        self.object_reader = ObjectReader(
            self.client, self.path, self.params, self.headers, self.chunk_size
        )

    @patch("aistore.sdk.object_reader.ObjectAttributes", autospec=True)
    def test_attributes_head(self, mock_attr):
        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        self.client.request.return_value = mock_response

        res = self.object_reader.attributes

        self.assertEqual(mock_attr.return_value, res)
        self.client.request.assert_called_once_with(
            HTTP_METHOD_HEAD, path=self.path, params=self.params
        )
        mock_attr.assert_called_with(self.response_headers)

    @patch("aistore.sdk.object_reader.ObjectAttributes", autospec=True)
    def test_read_all(self, mock_attr):
        chunk1 = b"chunk1"
        chunk2 = b"chunk2"
        mock_response = Mock(
            spec=requests.Response,
            content=chunk1 + chunk2,
            headers=self.response_headers,
        )
        self.client.request.return_value = mock_response

        content = self.object_reader.read_all()

        self.assertEqual(chunk1 + chunk2, content)
        self.assert_make_request(mock_attr, stream=False)

    @patch("aistore.sdk.object_reader.ObjectAttributes", autospec=True)
    def test_raw(self, mock_attr):
        mock_response = Mock(
            spec=requests.Response, raw=b"bytestream", headers=self.response_headers
        )
        self.client.request.return_value = mock_response

        raw_stream = self.object_reader.raw()

        self.assertEqual(mock_response.raw, raw_stream)
        self.assert_make_request(mock_attr, stream=True)

    @patch("aistore.sdk.object_reader.ObjectAttributes", autospec=True)
    def test_iter(self, mock_attr):
        expected_chunks = [b"chunk1", b"chunk2"]
        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        mock_response.iter_content.return_value = expected_chunks
        self.client.request.return_value = mock_response

        chunks = list(self.object_reader)

        mock_response.iter_content.assert_called_once_with(chunk_size=self.chunk_size)
        mock_response.close.assert_called_once()
        self.assertEqual(expected_chunks, chunks)
        self.assert_make_request(mock_attr, stream=True)

    @patch("aistore.sdk.object_reader.ObjectAttributes", autospec=True)
    def test_iter_from_position(self, mock_attr):
        expected_chunks = [b"chunk1", b"chunk2"]
        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        mock_response.iter_content.return_value = expected_chunks
        self.client.request.return_value = mock_response

        start_position = 2048
        chunks = list(
            self.object_reader.iter_from_position(start_position=start_position)
        )

        self.assertEqual(chunks, expected_chunks)
        self.assert_make_request_with_range(
            mock_attr, stream=True, start_position=start_position
        )

        self.client.request.reset_mock()
        start_position = 0
        chunks = list(
            self.object_reader.iter_from_position(start_position=start_position)
        )

        self.assertEqual(chunks, expected_chunks)
        self.assert_make_request(mock_attr, stream=True)

    def assert_make_request(self, mock_attr, stream):
        self.client.request.assert_called_once_with(
            HTTP_METHOD_GET,
            path=self.path,
            params=self.params,
            stream=stream,
            headers=self.headers,
        )
        self.assertIsInstance(self.object_reader.attributes, ObjectAttributes)
        mock_attr.assert_called_with(self.response_headers)

    def assert_make_request_with_range(self, mock_attr, stream, start_position):
        self.client.request.assert_called_once_with(
            HTTP_METHOD_GET,
            path=self.path,
            params=self.params,
            stream=stream,
            headers={**self.headers, "Range": f"bytes={start_position}-"},
        )
        self.assertIsInstance(self.object_reader.attributes, ObjectAttributes)
        mock_attr.assert_called_with(self.response_headers)
