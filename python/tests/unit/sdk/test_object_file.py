#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import io
import os
import unittest
from unittest import mock
from requests.exceptions import ChunkedEncodingError
from aistore.sdk.object_file import ObjectFile
from aistore.sdk.object_reader import ObjectReader
from aistore.sdk.const import DEFAULT_CHUNK_SIZE


class BadObjectStream(io.BytesIO):
    """Simulates a stream that fails with ChunkedEncodingError after a few reads."""

    def __init__(self, *args, fail_on_read=2, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_count = 0
        self.fail_on_read = fail_on_read

    def read(self, size=-1):
        self.read_count += 1
        if self.read_count == self.fail_on_read:
            raise ChunkedEncodingError("Simulated ChunkedEncodingError")
        return super().read(size)


class BadObjectReader(ObjectReader):
    """Simulate an ObjectReader that returns BadObjectStream and simulates intermittent failures."""

    def __init__(self, data=None, fail_on_read=2):
        super().__init__(client=mock.Mock(), path="", params=[])
        self.data = data
        self.fail_on_read = fail_on_read

    def iter_from_position(self, start_position=0):
        """Simulate streaming the object from a certain position."""

        def iterator():
            stream = BadObjectStream(
                self.data[start_position:], fail_on_read=self.fail_on_read
            )
            while True:
                chunk = stream.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk

        return iterator()


class TestObjectFile(unittest.TestCase):

    # Basic Tests

    def test_buffer_usage(self):
        """Test ObjectFile uses buffer correctly, only fetching new chunks when needed."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 2)
        mock_reader = BadObjectReader(data=data, fail_on_read=0)
        object_file = ObjectFile(mock_reader, max_resume=0)

        # Mock `next` to track how many times we fetch new data
        with mock.patch("builtins.next", wraps=next) as mock_next:

            # Read data that will cause buffer to fill partially
            read_size = DEFAULT_CHUNK_SIZE // 2
            read_data = object_file.read(read_size)
            self.assertEqual(read_data, data[:read_size])

            # Check that next() was called once to fill the buffer
            self.assertEqual(mock_next.call_count, 1)

            # Read more data from the buffer (without fetching new chunks)
            read_data = object_file.read(DEFAULT_CHUNK_SIZE // 2)
            self.assertEqual(read_data, data[read_size:DEFAULT_CHUNK_SIZE])

            # Ensure next() was not called again (since the buffer had the remaining data)
            self.assertEqual(mock_next.call_count, 1)

            # Read more data that requires fetching additional chunks
            read_data = object_file.read(DEFAULT_CHUNK_SIZE)
            self.assertEqual(
                read_data, data[DEFAULT_CHUNK_SIZE : DEFAULT_CHUNK_SIZE * 2]
            )

            # Now check that next() was called again to fetch more data
            self.assertEqual(mock_next.call_count, 2)

    # Retry & Resume Related Tests

    def test_read_all_catch_error(self):
        """Test ObjectFile catches ChunkedEncodingError and retries."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 2)
        mock_reader = BadObjectReader(data=data, fail_on_read=2)
        object_file = ObjectFile(mock_reader, max_resume=0)
        with self.assertRaises(ChunkedEncodingError):
            object_file.read()

    def test_read_fixed_catch_error(self):
        """Test ObjectFile catches ChunkedEncodingError and retries."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 2)
        mock_reader = BadObjectReader(data=data, fail_on_read=2)
        object_file = ObjectFile(mock_reader, max_resume=0)
        with self.assertRaises(ChunkedEncodingError):
            object_file.read(DEFAULT_CHUNK_SIZE * 2)

    def test_success_after_retries(self):
        """Test ObjectFile retries and succeeds after intermittent ChunkedEncodingError."""
        # Error raised on every 5th read (fail 4 times before succeeding)
        data = os.urandom(20 * DEFAULT_CHUNK_SIZE)
        mock_reader = BadObjectReader(data=data, fail_on_read=5)
        object_file = ObjectFile(mock_reader, max_resume=5)
        result = object_file.read()
        self.assertEqual(result, data)

    def test_fail_max_retries(self):
        """Test ObjectFile raises ChunkedEncodingError after exceeding max retries."""
        # Error raised on every 5th read (fail 2 times before raising)
        data = os.urandom(20 * DEFAULT_CHUNK_SIZE)
        mock_reader = BadObjectReader(data=data, fail_on_read=5)
        object_file = ObjectFile(mock_reader, max_resume=2)
        with self.assertRaises(ChunkedEncodingError):
            object_file.read()
