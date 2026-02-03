#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

import os
import signal
import multiprocessing as mp
import unittest
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import Mock, patch

import requests

from aistore.sdk.obj.content_iterator import ParallelContentIterProvider
from aistore.sdk.obj.content_iterator.parallel import RingBuffer
from aistore.sdk.const import PROPS_CHUNKED


def _make_resp(data: bytes) -> Mock:
    """Minimal mock of requests.Response: iter_content yields data in one chunk."""
    resp = Mock()
    resp.iter_content.return_value = [data]
    return resp


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
            return _make_resp(chunk_data[(start, end)])

        self.mock_client.get_chunk.side_effect = mock_get_chunk

        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            result = list(provider.create_iter())
            self.assertTrue(spy_close.called)
        self.assertEqual(len(result), 4)
        self.assertEqual(mp.active_children(), [])

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

        self.mock_client.get_chunk.return_value = _make_resp(b"small")

        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            result = list(provider.create_iter())
            self.assertTrue(spy_close.called)
        self.assertEqual(result, [b"small"])
        self.assertEqual(mp.active_children(), [])

    def test_client_property(self):
        """Test the client property returns the object client."""
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        self.assertEqual(provider.client, self.mock_client)

    def test_create_iter_propagates_worker_exception(self):
        """Test that an exception raised in a worker propagates to the caller."""
        self.mock_client.get_chunk.side_effect = ConnectionError("Network error")
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            with self.assertRaises(Exception):
                list(provider.create_iter())
            self.assertTrue(spy_close.called)
        self.assertEqual(mp.active_children(), [])

    def test_worker_exception_mid_stream_propagates(self):
        """Worker failure mid-stream propagates the exception and frees all resources."""

        def get_chunk(start, end):
            if start >= 200:
                raise ConnectionError("Object not found")
            return _make_resp(b"X" * (end - start))

        self.mock_client.get_chunk.side_effect = get_chunk
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        # wraps= keeps the real cleanup (shm.close + shm.unlink) while recording the call.
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            gen = provider.create_iter()

            self.assertEqual(next(gen), b"X" * 100)  # chunk [0,   100) — ok
            self.assertEqual(next(gen), b"X" * 100)  # chunk [100, 200) — ok
            with self.assertRaises(Exception):
                next(gen)  # chunk [200, 300) — raises

            self.assertTrue(spy_close.called)  # ring buffer was actually closed
        self.assertEqual(mp.active_children(), [])  # all worker processes have exited

    def test_early_exit(self):
        """Breaking out early does not deadlock; workers drain and ring buffer is freed."""
        self.mock_client.get_chunk.side_effect = lambda s, e: _make_resp(b"X" * (e - s))
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        result = []
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            for chunk in provider.create_iter():
                result.append(chunk)
                break  # exit after the first chunk; remaining workers are still in-flight
            self.assertTrue(spy_close.called)
        self.assertEqual(len(result), 1)
        self.assertEqual(mp.active_children(), [])

    def test_generator_discarded_before_iteration_does_not_leak(self):
        """Generator closed before any iteration does not leak shared memory."""
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        gen = provider.create_iter()
        gen.close()  # discard immediately; ring buffer was never allocated

    def test_create_iter_correct_byte_ranges(self):
        """Test that correct byte ranges are passed to get_chunk()."""
        range_data = {
            (0, 100): b"A" * 100,
            (100, 200): b"B" * 100,
            (200, 300): b"C" * 100,
            (300, 350): b"D" * 50,
        }
        self.mock_client.get_chunk.side_effect = lambda s, e: _make_resp(
            range_data[(s, e)]
        )

        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            result = list(provider.create_iter())
            self.assertTrue(spy_close.called)
        self.assertEqual(result[0], b"A" * 100)
        self.assertEqual(result[1], b"B" * 100)
        self.assertEqual(result[2], b"C" * 100)
        self.assertEqual(result[3], b"D" * 50)
        self.assertEqual(mp.active_children(), [])

    def test_uses_server_chunk_size_when_not_specified(self):
        """Test that chunk_size from head_v2() is used when chunk_size=None."""
        mock_attrs = Mock()
        mock_attrs.size = 800
        mock_chunks = Mock()
        mock_chunks.max_chunk_size = 200
        mock_attrs.chunks = mock_chunks
        self.mock_client.head_v2.return_value = mock_attrs
        self.mock_client.get_chunk.return_value = _make_resp(b"x" * 200)

        provider = ParallelContentIterProvider(
            self.mock_client, chunk_size=None, num_workers=4
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            result = list(provider.create_iter())
            self.assertTrue(spy_close.called)
        self.assertEqual(len(result), 4)  # 800 / 200 = 4 chunks
        self.assertEqual(mp.active_children(), [])

    def test_fallback_chunk_size_for_monolithic(self):
        """Test fallback to DEFAULT_PARALLEL_CHUNK_SIZE when chunks=None."""
        mock_attrs = Mock()
        mock_attrs.size = 16 * 1024 * 1024  # 16 MiB
        mock_attrs.chunks = None
        self.mock_client.head_v2.return_value = mock_attrs
        self.mock_client.get_chunk.return_value = _make_resp(b"x" * (8 * 1024 * 1024))

        provider = ParallelContentIterProvider(
            self.mock_client, chunk_size=None, num_workers=4
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            result = list(provider.create_iter())
            self.assertTrue(spy_close.called)
        self.assertEqual(len(result), 2)  # 16 MiB / 8 MiB = 2 chunks
        self.assertEqual(mp.active_children(), [])

    def test_read_timeout_in_worker_aborts_and_cleans_up(self):
        """Read timeout in a worker aborts the entire download and frees all resources."""

        def get_chunk(start, end):
            if start >= 200:
                raise requests.exceptions.ReadTimeout("Read timed out")
            return _make_resp(b"X" * (end - start))

        self.mock_client.get_chunk.side_effect = get_chunk
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, self.num_workers
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            gen = provider.create_iter()
            self.assertEqual(next(gen), b"X" * 100)  # chunk [0,   100) — ok
            self.assertEqual(next(gen), b"X" * 100)  # chunk [100, 200) — ok
            with self.assertRaises(requests.exceptions.ReadTimeout):
                next(gen)  # chunk [200, 300) — raises ReadTimeout
            self.assertTrue(spy_close.called)
        self.assertEqual(mp.active_children(), [])

    def test_worker_sigkill_raises_broken_process_pool_and_cleans_up(self):
        """Killing a worker process raises BrokenProcessPool and frees all resources."""

        def get_chunk(start, end):
            if start >= 100:
                os.kill(os.getpid(), signal.SIGKILL)
            return _make_resp(b"X" * (end - start))

        self.mock_client.get_chunk.side_effect = get_chunk
        # num_workers=1: chunk 0 completes before the single worker is killed on chunk 1,
        # preventing BrokenProcessPool from racing with the already-resolved chunk 0 future.
        provider = ParallelContentIterProvider(
            self.mock_client, self.chunk_size, num_workers=1
        )
        with patch.object(
            RingBuffer, "close", autospec=True, wraps=RingBuffer.close
        ) as spy_close:
            gen = provider.create_iter()
            self.assertEqual(next(gen), b"X" * 100)  # chunk [0, 100) — ok
            with self.assertRaises(BrokenProcessPool):
                next(gen)  # chunk [100, 200) — worker killed; BrokenProcessPool raised
            self.assertTrue(spy_close.called)
        self.assertEqual(mp.active_children(), [])
