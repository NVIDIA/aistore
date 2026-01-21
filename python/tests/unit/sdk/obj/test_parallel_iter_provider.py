#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch, call

from aistore.sdk.obj.content_iterator import ParallelContentIterProvider
from aistore.sdk.const import PROPS_CHUNKED


class TestParallelContentIterProvider(unittest.TestCase):
    """Unit tests for ParallelContentIterProvider."""

    def setUp(self):
        self.mock_client = Mock()
        self.chunk_size = 100
        self.num_workers = 4
        self.object_size = 350  # Will create 4 chunks: 0-99, 100-199, 200-299, 300-349

        # Mock head_v2() to return object size and chunk info
        mock_attrs = Mock()
        mock_attrs.size = self.object_size
        mock_attrs.chunks = None  # Monolithic object by default
        self.mock_client.head_v2.return_value = mock_attrs

    def test_init_fetches_object_size_via_head_v2(self):
        """Test that __init__ calls head_v2() to get object size and chunk info."""
        ParallelContentIterProvider(self.mock_client, self.chunk_size, self.num_workers)
        self.mock_client.head_v2.assert_called_once_with(PROPS_CHUNKED)

    def test_create_iter_empty_object(self):
        """Test iteration over empty object."""
        mock_attrs = Mock()
        mock_attrs.size = 0
        mock_attrs.chunks = None
        self.mock_client.head_v2.return_value = mock_attrs

        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        result = list(provider.create_iter())

        self.assertEqual(result, [])

    def test_create_iter_yields_chunks_in_order(self):
        """Test that chunks are yielded in correct order."""
        chunk_data = {
            (0, 100): b"chunk0",
            (100, 200): b"chunk1",
            (200, 300): b"chunk2",
            (300, 350): b"chunk3",
        }

        def mock_get_chunk(start, end):
            return chunk_data[(start, end)]

        self.mock_client.get_chunk.side_effect = mock_get_chunk

        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        result = list(provider.create_iter())

        self.assertEqual(self.mock_client.get_chunk.call_count, 4)
        self.assertEqual(len(result), 4)

    def test_create_iter_offset_beyond_size(self):
        """Test that offset beyond object size yields nothing."""
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        result = list(provider.create_iter(offset=400))

        self.assertEqual(result, [])

    def test_single_chunk_object(self):
        """Test object smaller than chunk size."""
        mock_attrs = Mock()
        mock_attrs.size = 50
        mock_attrs.chunks = None
        self.mock_client.head_v2.return_value = mock_attrs

        self.mock_client.get_chunk.return_value = b"small"

        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        result = list(provider.create_iter())

        self.assertEqual(result, [b"small"])
        # Verify get_chunk() was called with correct range
        self.mock_client.get_chunk.assert_called_once_with(0, 50)

    def test_client_property(self):
        """Test the client property returns the object client."""
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        self.assertEqual(provider.client, self.mock_client)

    @patch("aistore.sdk.obj.content_iterator.parallel.ThreadPoolExecutor")
    def test_create_iter_cancels_futures_on_error(self, mock_executor_cls):
        """Test that futures are canceled on chunk fetch error."""
        # Setup mock executor
        mock_executor = Mock()
        mock_executor_cls.return_value.__enter__ = Mock(return_value=mock_executor)
        mock_executor_cls.return_value.__exit__ = Mock(return_value=False)

        # Create futures - first succeeds, second fails
        mock_future_success = Mock()
        mock_future_success.result.return_value = (0, b"chunk0")

        mock_future_fail = Mock()
        fetch_error = ConnectionError("Network error")
        mock_future_fail.result.side_effect = fetch_error

        mock_executor.submit.side_effect = [mock_future_success, mock_future_fail]

        # Mock as_completed to return futures in order
        with patch(
            "aistore.sdk.obj.content_iterator.parallel.as_completed"
        ) as mock_as_completed:
            mock_as_completed.return_value = iter(
                [mock_future_success, mock_future_fail]
            )

            # Setup small object with 2 chunks
            mock_attrs = Mock()
            mock_attrs.size = 200
            mock_attrs.chunks = None
            self.mock_client.head_v2.return_value = mock_attrs

            provider = ParallelContentIterProvider(
                self.mock_client, self.chunk_size, self.num_workers
            )

            # Consume the iterator - should raise on second chunk
            with self.assertRaises(ConnectionError):
                list(provider.create_iter())

            # Verify shutdown was called with cancel_futures=True
            mock_executor.shutdown.assert_called_once_with(
                wait=False, cancel_futures=True
            )

    def test_create_iter_correct_byte_ranges(self):
        """Test that correct byte ranges are passed to get_chunk()."""
        self.mock_client.get_chunk.return_value = b"data"

        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        list(provider.create_iter())

        # Verify all byte ranges were requested correctly
        expected_calls = [
            call(0, 100),
            call(100, 200),
            call(200, 300),
            call(300, 350),
        ]
        self.mock_client.get_chunk.assert_has_calls(expected_calls, any_order=True)

    def test_uses_server_chunk_size_when_not_specified(self):
        """Test that chunk_size from head_v2() is used when chunk_size=None."""
        mock_attrs = Mock()
        mock_attrs.size = 800
        mock_chunks = Mock()
        mock_chunks.max_chunk_size = 200
        mock_attrs.chunks = mock_chunks
        self.mock_client.head_v2.return_value = mock_attrs
        self.mock_client.get_chunk.return_value = b"x" * 200

        provider = ParallelContentIterProvider(
            self.mock_client, chunk_size=None, num_workers=4
        )
        result = list(provider.create_iter())

        self.assertEqual(len(result), 4)  # 800 / 200 = 4 chunks

    def test_fallback_chunk_size_for_monolithic(self):
        """Test fallback to DEFAULT_PARALLEL_CHUNK_SIZE when chunks=None."""
        mock_attrs = Mock()
        mock_attrs.size = 16 * 1024 * 1024  # 16 MiB
        mock_attrs.chunks = None
        self.mock_client.head_v2.return_value = mock_attrs
        self.mock_client.get_chunk.return_value = b"x" * (8 * 1024 * 1024)

        provider = ParallelContentIterProvider(
            self.mock_client, chunk_size=None, num_workers=4
        )
        result = list(provider.create_iter())

        self.assertEqual(len(result), 2)  # 16 MiB / 8 MiB = 2 chunks
