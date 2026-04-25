import unittest
from unittest.mock import Mock
from aistore.sdk.obj.object_client import ObjectClient
from aistore.sdk.const import DEFAULT_CHUNK_SIZE
from aistore.sdk.obj.content_iterator import ContentIterProvider
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
        mock_stream.iter_content.return_value = byte_chunks
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

    def iter_exception_handling(self):
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

    def test_iter_tracks_expected_end_from_content_length(self):
        """Test that the iterator records expected EOF from the GET response."""
        mock_stream = Mock()
        mock_stream.headers = {"Content-Length": "42"}
        mock_stream.iter_content.return_value = byte_chunks
        self.mock_client.get.return_value = mock_stream

        offset = 100
        res = list(self.content_provider.create_iter(offset))

        self.assertEqual(byte_chunks, res)
        self.assertEqual(self.content_provider.expected_end_position, offset + 42)

    def test_iter_skips_expected_end_when_content_encoded(self):
        """Content-Length is wire bytes when Content-Encoding is set; skip tracking."""
        mock_stream = Mock()
        mock_stream.headers = {"Content-Length": "42", "Content-Encoding": "gzip"}
        mock_stream.iter_content.return_value = byte_chunks
        self.mock_client.get.return_value = mock_stream

        list(self.content_provider.create_iter(0))

        self.assertIsNone(self.content_provider.expected_end_position)
