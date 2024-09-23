#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import io
import os
import unittest
from unittest import mock
from requests.exceptions import ChunkedEncodingError
from aistore.sdk.obj.object_file import ObjectFile
from aistore.sdk.obj.object_reader import ObjectReader
from aistore.sdk.const import DEFAULT_CHUNK_SIZE


class BadObjectStream(io.BytesIO):
    """
    Simulates a stream that fails with ChunkedEncodingError intermittently every `fail_on_read`
    chunks read.
    """

    def __init__(self, *args, fail_on_read=2, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_count = 0
        self.fail_on_read = fail_on_read

    def read(self, size=-1):
        """Overrides `BytesIO.read` to simulate failure after a specific number of reads."""
        self.read_count += 1
        if self.read_count == self.fail_on_read:
            raise ChunkedEncodingError("Simulated ChunkedEncodingError")
        return super().read(size)


class BadObjectReader(ObjectReader):
    """
    Simulates an ObjectReader that streams data using BadObjectStream that fails with ChunksEncoding
    error every `fail_on_read` chunks read.

    This class extends `ObjectReader` and the chunk size (DEFAULT_CHUNK_SIZE) is inherited from the
    parent class `ObjectReader`.

    The streaming starts from a specific position (`start_position`), allowing the object to resume
    reading from that point if necessary.
    """

    def __init__(self, data=None, fail_on_read=2):
        super().__init__(client=mock.Mock(), path="", params=[])
        self.data = data
        self.fail_on_read = fail_on_read

    def iter_from_position(self, start_position=0):
        """Simulate streaming the object from the specified position `start_position`."""

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

    def test_read_all_fails_after_max_retries(self):
        """Test that ObjectFile gives up after exceeding max retry attempts for read-all."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadObjectReader(data=data, fail_on_read=2)
        object_file = ObjectFile(mock_reader, max_resume=2)

        # Test that the read fails after 2 retry attempts
        with self.assertRaises(ChunkedEncodingError):
            object_file.read()

    def test_read_fixed_fails_after_max_retries(self):
        """Test that ObjectFile gives up after exceeding max retry attempts for fixed-size reads."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadObjectReader(data=data, fail_on_read=2)
        object_file = ObjectFile(mock_reader, max_resume=2)

        # Test that the read fails after 2 retry attempts for a fixed-size read
        with self.assertRaises(ChunkedEncodingError):
            object_file.read(DEFAULT_CHUNK_SIZE * 4)

    def test_read_all_success_after_retries(self):
        """Test that ObjectFile retries and succeeds after intermittent ChunkedEncodingError for read-all."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadObjectReader(data=data, fail_on_read=2)
        object_file = ObjectFile(mock_reader, max_resume=4)

        result = object_file.read()
        # Ensure all data was correctly read after retries
        self.assertEqual(result, data)
        self.assertEqual(object_file.tell(), DEFAULT_CHUNK_SIZE * 4)

    def test_read_fixed_success_after_retries(self):
        """Test that ObjectFile retries and succeeds for fixed-size reads after intermittent ChunkedEncodingError."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadObjectReader(data=data, fail_on_read=2)
        object_file = ObjectFile(mock_reader, max_resume=4)

        # Read the first half of the data and check the position
        first_part = object_file.read(DEFAULT_CHUNK_SIZE * 2)
        self.assertEqual(first_part, data[: DEFAULT_CHUNK_SIZE * 2])
        self.assertEqual(object_file.tell(), DEFAULT_CHUNK_SIZE * 2)

        # Resume reading the second half and check position
        second_part = object_file.read(DEFAULT_CHUNK_SIZE * 2)
        self.assertEqual(
            second_part, data[DEFAULT_CHUNK_SIZE * 2 : DEFAULT_CHUNK_SIZE * 4]
        )
        self.assertEqual(object_file.tell(), DEFAULT_CHUNK_SIZE * 4)

        # Ensure all data was read correctly in two parts
        self.assertEqual(first_part + second_part, data)
