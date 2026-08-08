#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch
import requests

from aistore.sdk.batch.multipart.multipart_decoder import (
    MultipartDecoder,
    MultipartDecodeError,
)
from aistore.sdk.const import DEFAULT_MAX_BUFFER_SIZE


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

    def test_streaming_preamble_slide_does_not_raise(self):
        """Preamble bytes sliding out during boundary scan must not raise MultipartDecodeError."""
        # With a small max_buffer_size, a preamble followed by a max-sized boundary chunk
        # causes the sliding window to drop preamble bytes during force_read. This is safe
        # since preamble is discarded anyway, and must not trigger the header overflow guard.
        small_max = 64
        boundary = "ZZ"
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": f'multipart/mixed; boundary="{boundary}"'
        }
        preamble = b"X" * small_max
        boundary_chunk = b"--ZZ\r\nContent-Type: text/plain\r\n\r\nbody"
        mock_response.iter_content = lambda chunk_size=small_max: iter(
            [preamble, boundary_chunk]
        )

        decoder = MultipartDecoder(parse_as_stream=True, max_buffer_size=small_max)
        parts = list(decoder.decode(mock_response))

        self.assertEqual(len(parts), 1)
        headers, _ = parts[0]
        self.assertIn(b"Content-Type: text/plain", headers)

    def test_streaming_boundary_found_after_large_preamble(self):
        """Streaming decoder must find boundary even when preamble exceeds initial buffer fill."""
        boundary = "ZZ"
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": f'multipart/mixed; boundary="{boundary}"'
        }
        # Preamble larger than the boundary safety margin (12 bytes) forces
        # _locate_first_boundary to loop past the first fill before finding --ZZ.
        preamble = b"X" * 100
        mock_response.iter_content = lambda chunk_size=8192: iter(
            [preamble, b"--ZZ\r\nContent-Type: text/plain\r\n\r\nbody content"]
        )

        decoder = MultipartDecoder(parse_as_stream=True)
        parts = list(decoder.decode(mock_response))

        self.assertEqual(len(parts), 1)
        headers, _ = parts[0]
        self.assertIn(b"Content-Type: text/plain", headers)

    def test_streaming_no_boundary_exits_cleanly(self):
        """Streaming decoder must exit cleanly when the boundary is never found."""
        boundary = "ZZ"
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": f'multipart/mixed; boundary="{boundary}"'
        }
        mock_response.iter_content = lambda chunk_size=8192: iter(
            [b"no-boundary-anywhere"]
        )

        decoder = MultipartDecoder(parse_as_stream=True)
        parts = list(decoder.decode(mock_response))

        self.assertEqual(len(parts), 0)

    def test_streaming_valid_header_with_large_body_chunk_does_not_raise(self):
        """A valid header followed by a large body in the same chunk must not raise."""
        boundary = "ZZ"
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": f'multipart/mixed; boundary="{boundary}"'
        }
        # Chunk 1: boundary only — forces _extract_headers to call force_read
        # Chunk 2: header ending + large body in one chunk, pushing buffer over max_buffer_size.
        # bytes_to_remove will be 0 (buffer was small), so no data is actually lost.
        large_body = b"Y" * (DEFAULT_MAX_BUFFER_SIZE + 1)
        mock_response.iter_content = lambda chunk_size=8192: iter(
            [b"--ZZ", b"Content-Type: text/plain\r\n\r\n" + large_body]
        )

        decoder = MultipartDecoder(parse_as_stream=True)
        parts = [(h, body.read()) for h, body in decoder.decode(mock_response)]

        self.assertEqual(len(parts), 1)
        headers, body = parts[0]
        self.assertEqual(body, large_body)
        self.assertIn(b"Content-Type: text/plain", headers)

    def test_streaming_oversized_header_error_message(self):
        """MultipartDecodeError from an oversized header must name the buffer size limit."""
        boundary = "ZZ"
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": f'multipart/mixed; boundary="{boundary}"'
        }
        chunk = b"X" * (DEFAULT_MAX_BUFFER_SIZE // 8)
        mock_response.iter_content = lambda chunk_size=8192: iter(
            [b"--ZZ"] + [chunk] * 10
        )

        decoder = MultipartDecoder(parse_as_stream=True)
        with self.assertRaises(MultipartDecodeError) as ctx:
            list(decoder.decode(mock_response))

        self.assertIn(str(DEFAULT_MAX_BUFFER_SIZE), str(ctx.exception))

    def test_streaming_oversized_header_raises(self):
        """Streaming decoder must raise MultipartDecodeError when a header exceeds max buffer size."""
        boundary = "ZZ"
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": f'multipart/mixed; boundary="{boundary}"'
        }
        # Deliver header data in 8KB chunks with no line ending
        chunk = b"X" * (DEFAULT_MAX_BUFFER_SIZE // 8)
        mock_response.iter_content = lambda chunk_size=8192: iter(
            [b"--ZZ"] + [chunk] * 10
        )

        decoder = MultipartDecoder(parse_as_stream=True)
        with self.assertRaises(MultipartDecodeError):
            list(decoder.decode(mock_response))

    def test_streaming_unterminated_headers_exits_cleanly(self):
        """
        Streaming decoder must not spin when part headers are
        never terminated by CRLF/LF before EOF.
        """
        boundary = "ZZ"
        mock_response = Mock()
        mock_response.headers = {
            "Content-Type": f'multipart/mixed; boundary="{boundary}"'
        }
        mock_response.iter_content = lambda chunk_size=8192: iter(
            [b"--ZZ", b"Content-Type: text/plain no-terminator-EOF"]
        )

        decoder = MultipartDecoder(parse_as_stream=True)
        parts = list(decoder.decode(mock_response))

        self.assertEqual(
            len(parts), 0, "Malformed stream with no line ending should yield no parts"
        )

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
