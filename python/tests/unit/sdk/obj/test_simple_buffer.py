#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest

from unittest.mock import Mock
from aistore.sdk.obj.obj_file.simple_buffer import SimpleBuffer
from aistore.sdk.obj.content_iterator import ContentIterator
from aistore.sdk.obj.obj_file.errors import (
    ObjectFileMaxResumeError,
    ObjectFileStreamError,
)
from tests.utils import BadContentIterator


class TestSimpleBuffer(unittest.TestCase):
    def setUp(self):
        self.content_iterator = Mock(ContentIterator)
        self.simple_buffer = SimpleBuffer(
            self.content_iterator, max_resume=0, buffer_size=0
        )

    def test_len(self):
        """
        Test overridden __len__ method returns the correct length of the buffer.
        """
        # Simulate the buffer filled with some data
        self.simple_buffer._buffer = bytearray(b"chunk1")

        # Call len() on the simple_buffer and assert it matches the actual buffer length
        self.assertEqual(len(self.simple_buffer), 6)

        # Modify the buffer and test len() again
        self.simple_buffer._buffer = bytearray(b"chunk1chunk2")
        self.assertEqual(len(self.simple_buffer), 12)

    def test_eof_property(self):
        """
        Test eof property correctly reflects the end-of-file state.
        """
        # Simulate content iterator returning two chunks
        self.simple_buffer._chunk_iterator = iter([b"chunk1", b"chunk2"])

        # Initially, eof should be False
        self.assertFalse(self.simple_buffer.eof)

        # Set buffer size to 15 bytes (larger than total data)
        self.simple_buffer._buffer_size = 15

        # Fill the buffer, it should be at EOF
        self.simple_buffer.fill()
        self.assertTrue(self.simple_buffer.eof)

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
        self.assertEqual(len(self.simple_buffer), 0)

    def test_read_more_than_buffer(self):
        """
        Test reading more bytes than available in the buffer.
        """
        self.simple_buffer._buffer = bytearray(b"chunk1chunk2")

        # Read more than what is available in buffer
        result = self.simple_buffer.read(15)

        # Should read all bytes and leave the buffer empty
        self.assertEqual(result, b"chunk1chunk2")
        self.assertEqual(len(self.simple_buffer), 0)

    def test_read_fixed_empty_buffer(self):
        """
        Test reading from an empty buffer returns an empty byte string.
        """
        result = self.simple_buffer.read(10)

        # Should return an empty byte string
        self.assertEqual(result, b"")
        self.assertEqual(len(self.simple_buffer), 0)

    def test_read_all_empty_buffer(self):
        """
        Test reading all bytes from an empty buffer returns an empty byte string.
        """
        result = self.simple_buffer.read(-1)

        # Should return an empty byte string
        self.assertEqual(result, b"")
        self.assertEqual(len(self.simple_buffer), 0)

    # Fill Tests

    def test_fill_buffer_until_eof(self):
        """
        Test filling the buffer until EOF is reached with data that fits within the buffer.
        """
        # Simulate the content iterator returning two chunks, totaling less than the buffer size
        self.simple_buffer._chunk_iterator = iter([b"chunk1", b"chunk2"])

        self.simple_buffer._buffer_size = (
            15  # Set buffer size to 15 bytes (larger than total data)
        )

        # Fill the buffer
        self.simple_buffer.fill()

        # Buffer should be filled with both chunks, but not exceed the buffer size (15 bytes)
        self.assertEqual(self.simple_buffer._buffer, b"chunk1chunk2")

        # Ensure the buffer is at EOF
        self.assertTrue(self.simple_buffer.eof)

    def test_fill_buffer_up_to_buffer_size(self):
        """
        Test that the buffer fills to the buffer size and no more.
        """
        # Set the buffer size to 15 bytes
        self.simple_buffer._buffer_size = 15

        self.simple_buffer._chunk_iterator = iter(
            [b"chunk1", b"chunk2", b"chunk3", b"chunk4"]
        )

        # Fill the buffer
        self.simple_buffer.fill()

        # The buffer should only be filled to the buffer size
        self.assertEqual(self.simple_buffer._buffer, b"chunk1chunk2chunk3")
        self.assertEqual(len(self.simple_buffer), 18)

        # Ensure the buffer is not at EOF
        self.assertFalse(self.simple_buffer.eof)

    # Resume Tests

    def test_init_raises_stream_error(self):
        """Test that initializing SimpleBuffer raises ObjectFileStreamError if no stream can be established."""
        stream_creation_err = Exception("Can't connect to AIS")
        mock_content_iterator = Mock()
        mock_content_iterator.iter_from_position.side_effect = stream_creation_err

        # Attempt to initialize the buffer, which should raise an ObjectFileStreamError due to the connection failure
        with self.assertRaises(ObjectFileStreamError):
            SimpleBuffer(mock_content_iterator, max_resume=Mock(), buffer_size=Mock())

    def test_mid_fill_raises_stream_error(self):
        """Test that SimpleBuffer raises ObjectFileStreamError if no stream can be established mid-fill."""
        # Simulate the first iterator working and the second failing
        data = b"chunk1"
        stream_creation_err = Exception("Can't connect to AIS")
        bad_iterator = BadContentIterator(data=data, fail_on_read=2).iter_from_position(
            0
        )

        # Mock the content iterator to return a valid iterator first and then simulate connection failure
        mock_content_iterator = Mock(spec=ContentIterator)
        mock_content_iterator.iter_from_position.side_effect = [
            bad_iterator,  # Success on the first attempt
            stream_creation_err,  # Failure after retry (on the second attempt)
        ]

        # Set a large buffer size so it doesn't affect the test logic
        self.simple_buffer._content_iterator = mock_content_iterator
        self.simple_buffer._chunk_iterator = (
            self.simple_buffer._content_iterator.iter_from_position(0)
        )
        self.simple_buffer._buffer_size = len(data) * 2
        self.simple_buffer._max_resume = 2

        with self.assertRaises(ObjectFileStreamError):
            self.simple_buffer.fill()

        # Ensure the buffer is not at EOF
        self.assertFalse(self.simple_buffer.eof)

    def test_fill_success_after_retries(self):
        """Test that SimpleBuffer retries and succeeds after intermittent ChunkedEncodingError."""
        # Simulate content iterator returning chunks with intermittent failures
        data = b"chunk1chunk2chunk3chunk4"
        bad_iterator = BadContentIterator(data=data, fail_on_read=2, chunk_size=6)

        # Set a large buffer size so it doesn't affect the test logic
        self.simple_buffer._content_iterator = bad_iterator
        self.simple_buffer._chunk_iterator = (
            self.simple_buffer._content_iterator.iter_from_position(0)
        )
        self.simple_buffer._buffer_size = len(data) * 2
        self.simple_buffer._max_resume = 4

        # Try to fill the buffer with the entire data (retries will be needed)
        self.simple_buffer.fill()

        # The result should match the original data
        self.assertEqual(bytes(self.simple_buffer._buffer), data)

        # Ensure the buffer is at EOF
        self.assertTrue(self.simple_buffer.eof)

    def test_fill_fails_after_max_retries(self):
        """Test that SimpleBuffer gives up after exceeding max retry attempts for fill."""
        # Simulate content iterator returning chunks with intermittent failures
        data = b"chunk1chunk2chunk3chunk4"
        bad_iterator = BadContentIterator(data=data, fail_on_read=2, chunk_size=6)

        # Set a large buffer size so it doesn't affect the test logic
        self.simple_buffer._content_iterator = bad_iterator
        self.simple_buffer._chunk_iterator = (
            self.simple_buffer._content_iterator.iter_from_position(0)
        )
        self.simple_buffer._buffer_size = len(data) * 2
        self.simple_buffer._max_resume = 2

        # Test that fill fails after 2 retry attempts
        with self.assertRaises(ObjectFileMaxResumeError):
            self.simple_buffer.fill()

        # Ensure the buffer is not at EOF
        self.assertFalse(self.simple_buffer.eof)
