import unittest
from unittest.mock import Mock
from aistore.sdk.obj.object_client import ObjectClient
from aistore.sdk.const import DEFAULT_CHUNK_SIZE
from aistore.sdk.obj.content_iterator import ContentIterator
from tests.utils import cases

byte_chunks = [b"chunk1", b"chunk2", b"chunk3"]


class TestContentIterator(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock(spec=ObjectClient)
        self.iterator = ContentIterator(self.mock_client, DEFAULT_CHUNK_SIZE)

    @cases(None, 1234)
    def test_iter_from_position(self, chunk_size):
        mock_stream = Mock()
        mock_stream.iter_content.return_value = byte_chunks
        self.mock_client.get.return_value = mock_stream

        if chunk_size:
            self.iterator = ContentIterator(self.mock_client, chunk_size=chunk_size)

        start_position = 100
        res = list(self.iterator.iter_from_position(start_position))

        self.assertEqual(byte_chunks, res)
        self.mock_client.get.assert_called_with(
            stream=True, start_position=start_position
        )
        if chunk_size:
            mock_stream.iter_content.assert_called_once_with(chunk_size=chunk_size)
        else:
            mock_stream.iter_content.assert_called_once_with(
                chunk_size=DEFAULT_CHUNK_SIZE
            )

        mock_stream.close.assert_called_once()

    def test_iter_from_position_exception_handling(self):
        mock_stream = Mock()
        mock_stream.iter_content.side_effect = Exception("Stream error")
        self.mock_client.get.return_value = mock_stream

        with self.assertRaises(Exception):
            list(self.iterator.iter_from_position(0))

        mock_stream.close.assert_called_once()
