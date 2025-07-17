#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch
import requests

from aistore.sdk.batch.multipart_decoder import (
    MultipartDecoder,
    MultipartDecodeError,
)


# pylint: disable=too-many-public-methods
class TestMultipartDecoder(unittest.TestCase):
    """Test suite for MultipartDecoder class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.decoder = MultipartDecoder()
        self.test_boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"

    def test_init_default_encoding(self):
        """Test MultipartDecoder initialization with default encoding."""
        decoder = MultipartDecoder()
        self.assertEqual(decoder.encoding, "utf-8")

    def test_init_custom_encoding(self):
        """Test MultipartDecoder initialization with custom encoding."""
        decoder = MultipartDecoder(encoding="latin-1")
        self.assertEqual(decoder.encoding, "latin-1")

    def test_extract_boundary_success(self):
        """Test successful boundary extraction from Content-Type header."""
        content_type = (
            "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW"
        )
        # pylint: disable=protected-access
        boundary = self.decoder._extract_boundary(content_type)
        self.assertEqual(boundary, "----WebKitFormBoundary7MA4YWxkTrZu0gW")

    def test_extract_boundary_with_quotes(self):
        """Test boundary extraction with quoted boundary."""
        content_type = (
            'multipart/form-data; boundary="----WebKitFormBoundary7MA4YWxkTrZu0gW"'
        )
        # pylint: disable=protected-access
        boundary = self.decoder._extract_boundary(content_type)
        self.assertEqual(boundary, "----WebKitFormBoundary7MA4YWxkTrZu0gW")

    def test_extract_boundary_with_single_quotes(self):
        """Test boundary extraction with single-quoted boundary."""
        content_type = (
            "multipart/form-data; boundary='----WebKitFormBoundary7MA4YWxkTrZu0gW'"
        )
        # pylint: disable=protected-access
        boundary = self.decoder._extract_boundary(content_type)
        self.assertEqual(boundary, "----WebKitFormBoundary7MA4YWxkTrZu0gW")

    def test_extract_boundary_not_found(self):
        """Test boundary extraction failure when boundary not present."""
        content_type = "multipart/form-data"
        with self.assertRaises(ValueError) as context:
            # pylint: disable=protected-access
            self.decoder._extract_boundary(content_type)
        self.assertIn("Boundary not found", str(context.exception))

    def test_parse_part_unix_line_endings(self):
        """Test parsing part with Unix line endings."""
        part_content = b'Content-Disposition: form-data; name="field1"\n\ntest_value'
        # pylint: disable=protected-access
        result = self.decoder._parse_part(part_content)
        self.assertIsNotNone(result)
        headers, data = result
        self.assertEqual(headers, b'Content-Disposition: form-data; name="field1"')
        self.assertEqual(data, b"test_value")

    def test_parse_part_windows_line_endings(self):
        """Test parsing part with Windows line endings."""
        part_content = (
            b'Content-Disposition: form-data; name="field1"\r\n\r\ntest_value'
        )
        # pylint: disable=protected-access
        result = self.decoder._parse_part(part_content)
        self.assertIsNotNone(result)
        headers, data = result
        self.assertEqual(headers, b'Content-Disposition: form-data; name="field1"')
        self.assertEqual(data, b"test_value")

    def test_parse_part_no_valid_line_endings(self):
        """Test parsing part with no valid line endings."""
        part_content = b'Content-Disposition: form-data; name="field1"test_value'
        # pylint: disable=protected-access
        result = self.decoder._parse_part(part_content)
        self.assertIsNone(result)

    def test_parse_part_empty_content(self):
        """Test parsing empty part content."""
        part_content = b""
        # pylint: disable=protected-access
        result = self.decoder._parse_part(part_content)
        self.assertIsNone(result)

    def test_parse_content_single_part(self):
        """Test parsing multipart content with single part."""
        boundary = "boundary123"
        content = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field1"\r\n\r\n'
            b"value1"
            b"--boundary123--"
        )

        # pylint: disable=protected-access
        parts = list(self.decoder._parse_content(content, boundary))
        self.assertEqual(len(parts), 1)
        headers, data = parts[0]
        self.assertEqual(data, b"value1")
        self.assertEqual(headers, b'\r\nContent-Disposition: form-data; name="field1"')

    def test_parse_content_multiple_parts(self):
        """Test parsing multipart content with multiple parts."""
        boundary = "boundary123"
        content = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field1"\r\n\r\n'
            b"value1\r\n"
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field2"\r\n\r\n'
            b"value2\r\n"
            b"--boundary123--"
        )

        # pylint: disable=protected-access
        parts = list(self.decoder._parse_content(content, boundary))
        self.assertEqual(len(parts), 2)
        headers1, data1 = parts[0]
        headers2, data2 = parts[1]
        self.assertEqual(data1, b"value1\r\n")
        self.assertEqual(data2, b"value2\r\n")
        self.assertEqual(headers1, b'\r\nContent-Disposition: form-data; name="field1"')
        self.assertEqual(headers2, b'\r\nContent-Disposition: form-data; name="field2"')

    def test_parse_content_with_file(self):
        """Test parsing multipart content with file upload."""
        boundary = "boundary123"
        file_content = b"binary file content here"
        content = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="file"; filename="test.txt"\r\n'
            b"Content-Type: text/plain\r\n\r\n" + file_content + b"\r\n"
            b"--boundary123--"
        )

        # pylint: disable=protected-access
        parts = list(self.decoder._parse_content(content, boundary))
        self.assertEqual(len(parts), 1)
        headers, data = parts[0]
        self.assertEqual(data, file_content + b"\r\n")
        expected_headers = (
            b'\r\nContent-Disposition: form-data; name="file"; filename="test.txt"'
            b"\r\nContent-Type: text/plain"
        )
        self.assertEqual(headers, expected_headers)

    def test_decode_multipart_success(self):
        """Test successful multipart decoding from HTTP response."""
        # Create mock response
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": "multipart/form-data; boundary=boundary123"
        }
        mock_response.content = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field1"\r\n\r\n'
            b"value1\r\n"
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field2"\r\n\r\n'
            b"value2\r\n"
            b"--boundary123--"
        )

        parts = list(self.decoder.decode(mock_response))
        self.assertEqual(len(parts), 2)
        headers1, data1 = parts[0]
        headers2, data2 = parts[1]
        self.assertEqual(data1, b"value1\r\n")
        self.assertEqual(data2, b"value2\r\n")
        self.assertEqual(headers1, b'\r\nContent-Disposition: form-data; name="field1"')
        self.assertEqual(headers2, b'\r\nContent-Disposition: form-data; name="field2"')

    def test_decode_not_multipart_content_type(self):
        """Test decoding failure when response is not multipart."""
        mock_response = Mock()
        mock_response.headers = {"Content-Type": "application/json"}

        with self.assertRaises(MultipartDecodeError) as context:
            list(self.decoder.decode(mock_response))
        self.assertIn("not of multipart content type", str(context.exception))

    def test_decode_multipart_missing_content_type(self):
        """Test decoding when Content-Type header is missing."""
        mock_response = Mock()
        mock_response.headers = {}

        with self.assertRaises(MultipartDecodeError) as context:
            list(self.decoder.decode(mock_response))
        self.assertIn("not of multipart content type", str(context.exception))

    def test_parse_content_empty_parts(self):
        """Test parsing multipart content with empty parts."""
        boundary = "boundary123"
        content = (
            b"--boundary123\r\n"
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field1"\r\n\r\n'
            b"value1\r\n"
            b"--boundary123--"
        )

        # pylint: disable=protected-access
        parts = list(self.decoder._parse_content(content, boundary))
        self.assertEqual(len(parts), 1)
        headers, data = parts[0]
        self.assertEqual(data, b"value1\r\n")
        self.assertEqual(headers, b'\r\nContent-Disposition: form-data; name="field1"')

    def test_parse_content_with_preamble(self):
        """Test parsing multipart content with preamble text."""
        boundary = "boundary123"
        content = (
            b"This is preamble text\r\n"
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field1"\r\n\r\n'
            b"value1\r\n"
            b"--boundary123--"
        )

        # pylint: disable=protected-access
        parts = list(self.decoder._parse_content(content, boundary))
        self.assertEqual(len(parts), 1)
        headers, data = parts[0]
        self.assertEqual(data, b"value1\r\n")
        self.assertEqual(headers, b'\r\nContent-Disposition: form-data; name="field1"')

    def test_custom_encoding(self):
        """Test multipart decoding with custom encoding."""
        decoder = MultipartDecoder(encoding="latin-1")
        boundary = "boundary123"
        content = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="field1"\r\n\r\n'
            b"value1\r\n"
            b"--boundary123--"
        )

        # pylint: disable=protected-access
        parts = list(decoder._parse_content(content, boundary))
        self.assertEqual(len(parts), 1)
        headers, data = parts[0]
        self.assertEqual(data, b"value1\r\n")
        self.assertEqual(headers, b'\r\nContent-Disposition: form-data; name="field1"')

    def test_real_world_form_submission(self):
        """Test parsing a realistic form submission with text and file."""
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW"
        }

        # Realistic multipart content
        mock_response.content = (
            b"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\n"
            b'Content-Disposition: form-data; name="username"\r\n\r\n'
            b"john_doe\r\n"
            b"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\n"
            b'Content-Disposition: form-data; name="email"\r\n\r\n'
            b"john@example.com\r\n"
            b"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\n"
            b'Content-Disposition: form-data; name="avatar"; filename="profile.jpg"\r\n'
            b"Content-Type: image/jpeg\r\n\r\n"
            b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00\xff\xdb\r\n"
            b"------WebKitFormBoundary7MA4YWxkTrZu0gW--"
        )

        parts = list(self.decoder.decode(mock_response))

        self.assertEqual(len(parts), 3)
        headers1, data1 = parts[0]
        headers2, data2 = parts[1]
        headers3, data3 = parts[2]
        self.assertEqual(
            headers1, b'\r\nContent-Disposition: form-data; name="username"'
        )
        self.assertEqual(headers2, b'\r\nContent-Disposition: form-data; name="email"')

        expected_headers3 = (
            b'\r\nContent-Disposition: form-data; name="avatar"; filename="profile.jpg"'
            b"\r\nContent-Type: image/jpeg"
        )
        self.assertEqual(headers3, expected_headers3)

        self.assertEqual(data1, b"john_doe\r\n")
        self.assertEqual(data2, b"john@example.com\r\n")
        self.assertTrue(data3.startswith(b"\xff\xd8\xff\xe0"))  # JPEG header

    def test_aistore_batch_response_format(self):
        """Test parsing AIStore-style batch response format."""
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": "multipart/form-data; boundary=aistore-batch-boundary"
        }

        # AIStore batch response format
        batch_metadata = (
            b'{"out": [{"objname": "file1.txt", "size": 100}], "uuid": "123"}'
        )
        file_content = b"This is the content of file1.txt"

        mock_response.content = (
            b"--aistore-batch-boundary\r\n"
            b'Content-Disposition: form-data; name="batch_response"\r\n'
            b"Content-Type: application/json\r\n\r\n" + batch_metadata + b"\r\n"
            b"--aistore-batch-boundary\r\n"
            b'Content-Disposition: form-data; name="object"; filename="file1.txt"\r\n'
            b"Content-Type: text/plain\r\n\r\n" + file_content + b"\r\n"
            b"--aistore-batch-boundary--"
        )

        parts = list(self.decoder.decode(mock_response))

        self.assertEqual(len(parts), 2)
        headers1, data1 = parts[0]
        headers2, data2 = parts[1]

        self.assertEqual(data1, batch_metadata + b"\r\n")
        self.assertEqual(data2, file_content + b"\r\n")
        self.assertNotEqual(headers1, b"")
        self.assertNotEqual(headers2, b"")

    @patch("requests.get")
    def test_integration_with_requests(self, mock_get):
        """Test integration with actual requests library."""
        # Mock requests response
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": "multipart/form-data; boundary=test-boundary"
        }
        mock_response.content = (
            b"--test-boundary\r\n"
            b'Content-Disposition: form-data; name="data"\r\n\r\n'
            b"test_data\r\n"
            b"--test-boundary--"
        )
        mock_get.return_value = mock_response

        # Simulate making a request and parsing response
        response = requests.get("NOT REAL URL", timeout=10)
        parts = list(self.decoder.decode(response))

        self.assertEqual(len(parts), 1)
        headers, data = parts[0]
        self.assertEqual(data, b"test_data\r\n")
        self.assertNotEqual(headers, b"")
