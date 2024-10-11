#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access, duplicate-code, too-few-public-methods

import io
import os
import unittest

from unittest.mock import Mock
from requests.exceptions import ChunkedEncodingError
from aistore.sdk.const import DEFAULT_CHUNK_SIZE
from aistore.sdk.obj.obj_file.buffer import SimpleBuffer
from aistore.sdk.obj.content_iterator import ContentIterator
from aistore.sdk.obj.obj_file.errors import (
    ObjectFileMaxResumeError,
    ObjectFileStreamError,
)


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


class BadContentIterator(ContentIterator):
    """
    Simulates an ContentIterator that streams data using BadObjectStream that fails with ChunksEncoding
    error every `fail_on_read` chunks read.

    This class extends `ContentIterator` and the chunk size (DEFAULT_CHUNK_SIZE) is inherited from the
    parent class `ContentIterator`.

    The streaming starts from a specific position (`start_position`), allowing the object to resume
    reading from that point if necessary.
    """

    def __init__(self, data=None, fail_on_read=2):
        super().__init__(client=Mock(), chunk_size=DEFAULT_CHUNK_SIZE)
        self.data = data
        self.fail_on_read = fail_on_read

    def iter_from_position(self, start_position=0):
        """Simulate streaming the object from the specified position `start_position`."""

        def iterator():
            stream = BadObjectStream(
                self.data[start_position:], fail_on_read=self.fail_on_read
            )
            while True:
                chunk = stream.read(self._chunk_size)
                if not chunk:
                    break
                yield chunk

        return iterator()


class TestSimpleBuffer(unittest.TestCase):
    def setUp(self):
        self.content_iterator = Mock(ContentIterator)
        self.simple_buffer = SimpleBuffer(self.content_iterator, max_resume=3)

    # Read Tests

    def test_read_less_than_buffer(self):
        """
        Test reading fixed bytes from the buffer within the buffer's size.
        """
        # Simulate the buffer filled with data
        self.simple_buffer._buffer = bytearray(b"chunk1chunk2")

        # Read 6 bytes from the buffer
        result = self.simple_buffer.read(6)

        # Should read 6 bytes and leave buffer with remaining 6 bytes
        self.assertEqual(result, b"chunk1")
        self.assertEqual(self.simple_buffer._buffer, b"chunk2")

    def test_read_all_from_buffer(self):
        """
        Test reading all bytes from the buffer when size is -1.
        """
        self.simple_buffer._buffer = bytearray(b"chunk1chunk2")

        result = self.simple_buffer.read(-1)

        # Should read all bytes and leave the buffer empty
        self.assertEqual(result, b"chunk1chunk2")
        self.assertEqual(len(self.simple_buffer._buffer), 0)

    def test_read_more_than_buffer(self):
        """
        Test reading more bytes than available in the buffer.
        """
        self.simple_buffer._buffer = bytearray(b"chunk1chunk2")

        # Read more than what is available in buffer
        result = self.simple_buffer.read(15)

        # Should read all bytes and leave the buffer empty
        self.assertEqual(result, b"chunk1chunk2")
        self.assertEqual(len(self.simple_buffer._buffer), 0)

    def test_read_fixed_empty_buffer(self):
        """
        Test reading from an empty buffer returns an empty byte string.
        """
        result = self.simple_buffer.read(10)

        # Should return an empty byte string
        self.assertEqual(result, b"")
        self.assertEqual(len(self.simple_buffer._buffer), 0)

    def test_read_all_empty_buffer(self):
        """
        Test reading all bytes from an empty buffer returns an empty byte string.
        """
        result = self.simple_buffer.read(-1)

        # Should return an empty byte string
        self.assertEqual(result, b"")
        self.assertEqual(len(self.simple_buffer._buffer), 0)

    # Fill Tests

    def test_fill_buffer_until_eof(self):
        """
        Test filling the buffer with data from the content iterator until the iterator
        is exhausted.
        """
        # Simulate the content iterator returning two chunks
        self.simple_buffer._chunk_iterator = iter([b"chunk1", b"chunk2"])

        # Fill the buffer
        self.simple_buffer.fill()

        # Buffer should be filled with the two chunks
        self.assertEqual(self.simple_buffer._buffer, b"chunk1chunk2")

    def test_fill_buffer_more_than_available(self):
        """
        Test filling the buffer with more data than available from the content iterator
        returns what is available.
        """
        # Simulate the content iterator returning two chunks
        self.simple_buffer._chunk_iterator = iter([b"chunk1", b"chunk2"])

        # Fill the buffer
        self.simple_buffer.fill(20)  # More than is available

        # Buffer should be filled with the two chunks that are available
        self.assertEqual(self.simple_buffer._buffer, b"chunk1chunk2")

    def test_fill_buffer(self):
        """
        Test filling the buffer with fixed bytes from the content iterator.
        """
        # Simulate the content iterator returning two chunks
        self.simple_buffer._chunk_iterator = iter([b"chunk1", b"chunk2"])

        # Fill the buffer
        self.simple_buffer.fill(3)

        # Buffer should be filled with one chunk only
        self.assertEqual(self.simple_buffer._buffer, b"chunk1")

    # Resume Tests

    def test_init_raises_stream_error(self):
        """Test that initializing SimpleBuffer raises ObjectFileStreamError if no stream can be established."""
        stream_creation_err = Exception("Can't connect to AIS")
        mock_content_iterator = Mock()
        mock_content_iterator.iter_from_position.side_effect = stream_creation_err

        # Attempt to initialize the buffer, which should raise an ObjectFileStreamError due to the connection failure
        with self.assertRaises(ObjectFileStreamError):
            SimpleBuffer(mock_content_iterator, max_resume=2)

    def test_mid_fill_raises_stream_error(self):
        """Test that SimpleBuffer raises ObjectFileStreamError if no stream can be established mid-fill."""
        # Simulate the first iterator working and the second failing
        stream_creation_err = Exception("Can't connect to AIS")
        bad_iterator = BadContentIterator(
            data=b"some data", fail_on_read=2
        ).iter_from_position(0)

        # Mock the content iterator to return a valid iterator first and then simulate connection failure
        mock_content_iterator = Mock(spec=ContentIterator)
        mock_content_iterator.iter_from_position.side_effect = [
            bad_iterator,  # Success on the first attempt
            stream_creation_err,  # Failure after retry (on the second attempt)
        ]

        # Initialize the SimpleBuffer with the mock content iterator
        buffer = SimpleBuffer(mock_content_iterator, max_resume=2)

        with self.assertRaises(ObjectFileStreamError):
            buffer.fill()

    def test_fill_all_success_after_retries(self):
        """Test that SimpleBuffer retries and succeeds after intermittent ChunkedEncodingError."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadContentIterator(data=data, fail_on_read=2)
        buffer = SimpleBuffer(mock_reader, max_resume=4)

        # Try to fill the buffer with the entire data
        buffer.fill()

        # Ensure the buffer was correctly filled after retries
        result = buffer.read(DEFAULT_CHUNK_SIZE * 4)
        self.assertEqual(result, data)

    def test_fill_all_fails_after_max_retries(self):
        """Test that SimpleBuffer gives up after exceeding max retry attempts for fill."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadContentIterator(data=data, fail_on_read=2)
        buffer = SimpleBuffer(mock_reader, max_resume=2)

        # Test that fill fails after 2 retry attempts
        with self.assertRaises(ObjectFileMaxResumeError):
            buffer.fill()

    def test_fill_fixed_success_after_retries(self):
        """Test that SimpleBuffer retries and succeeds for fixed-size fill after intermittent ChunkedEncodingError."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadContentIterator(data=data, fail_on_read=2)
        buffer = SimpleBuffer(mock_reader, max_resume=4)

        # Attempt to fill the buffer with a fixed size
        buffer.fill(size=DEFAULT_CHUNK_SIZE * 4)

        # Ensure the buffer was correctly filled after retries
        result = buffer.read(DEFAULT_CHUNK_SIZE * 4)
        self.assertEqual(result, data)

    def test_fill_fixed_fails_after_max_retries(self):
        """Test that SimpleBuffer gives up after exceeding max retry attempts for fixed-size fill."""
        data = os.urandom(DEFAULT_CHUNK_SIZE * 4)
        mock_reader = BadContentIterator(data=data, fail_on_read=2)
        buffer = SimpleBuffer(mock_reader, max_resume=2)

        # Test that fill fails after 2 retry attempts
        with self.assertRaises(ObjectFileMaxResumeError):
            buffer.fill(size=DEFAULT_CHUNK_SIZE * 4)
