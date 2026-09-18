import unittest
from unittest.mock import Mock
from aistore.sdk.obj.object_client import ObjectClient
from aistore.sdk.const import (
    DEFAULT_CHUNK_SIZE,
    STATUS_OK,
    STATUS_PARTIAL_CONTENT,
)
from aistore.sdk.obj.content_iterator import ContentIterProvider, StreamBounds
from tests.utils import cases

byte_chunks = [b"chunk1", b"chunk2", b"chunk3"]


class TestContentIterProvider(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock(spec=ObjectClient)
        self.content_provider = ContentIterProvider(
            self.mock_client, DEFAULT_CHUNK_SIZE
        )

    @cases(None, 1234)
    def test_iter(self, chunk_size):
        mock_stream = Mock()
        mock_stream.iter_content = Mock(return_value=byte_chunks)
        self.mock_client.get.return_value = mock_stream

        if chunk_size:
            self.content_provider = ContentIterProvider(
                self.mock_client, chunk_size=chunk_size
            )

        offset = 100
        res = list(self.content_provider.create_iter(offset))

        self.assertEqual(byte_chunks, res)
        self.mock_client.get.assert_called_with(stream=True, offset=offset)
        if chunk_size:
            mock_stream.iter_content.assert_called_once_with(chunk_size=chunk_size)
        else:
            mock_stream.iter_content.assert_called_once_with(
                chunk_size=DEFAULT_CHUNK_SIZE
            )

        mock_stream.close.assert_called_once()

    def test_iter_exception_handling(self):
        mock_stream = Mock()
        mock_stream.iter_content.side_effect = Exception("Stream error")
        self.mock_client.get.return_value = mock_stream

        with self.assertRaises(Exception):
            list(self.content_provider.create_iter(0))

        mock_stream.close.assert_called_once()

    def test_iter_close_triggers_cleanup(self):
        """Test that calling close() on iterator closes the underlying stream."""
        mock_stream = Mock()
        mock_stream.iter_content.return_value = iter([b"chunk1", b"chunk2"])
        self.mock_client.get.return_value = mock_stream

        iterator = self.content_provider.create_iter()
        next(iterator)
        iterator.close()

        mock_stream.close.assert_called_once()

    def _mock_get(self, status_code, headers):
        mock_stream = Mock()
        mock_stream.status_code = status_code
        mock_stream.headers = headers
        mock_stream.iter_content.return_value = byte_chunks
        self.mock_client.get.return_value = mock_stream

    @cases((STATUS_PARTIAL_CONTENT, 100), (STATUS_OK, 0))
    def test_iter_tracks_start_and_expected_end_from_response(self, case):
        """The stream start comes from the response status, not the requested offset."""
        status_code, start = case
        self._mock_get(status_code, {"Content-Length": "42"})
        bounds = StreamBounds()

        res = list(self.content_provider.create_iter(100, bounds))

        self.assertEqual(byte_chunks, res)
        self.assertEqual(StreamBounds(start, start + 42), bounds)

    def test_iter_updates_bounds(self):
        """A response without a length must not keep the end of an earlier response."""
        bounds = StreamBounds()
        self._mock_get(STATUS_PARTIAL_CONTENT, {"Content-Length": "42"})
        list(self.content_provider.create_iter(100, bounds))

        self._mock_get(STATUS_OK, {})
        list(self.content_provider.create_iter(100, bounds))

        self.assertEqual(StreamBounds(0, None), bounds)

    def test_iter_skips_expected_end_when_content_encoded(self):
        """Content-Length is wire bytes when Content-Encoding is set; skip tracking."""
        self._mock_get(STATUS_OK, {"Content-Length": "42", "Content-Encoding": "gzip"})
        bounds = StreamBounds()

        list(self.content_provider.create_iter(0, bounds))

        self.assertIsNone(bounds.expected_end)
