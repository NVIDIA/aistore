#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest
from unittest.mock import call, Mock

from aistore.sdk.obj.obj_file.simple_buffer import SimpleBuffer
from aistore.sdk.obj.obj_file.object_file import ObjectFile


class TestObjectFile(unittest.TestCase):

    def setUp(self):
        self.object_file = ObjectFile(
            content_iterator=Mock(),
            max_resume=0,
            buffer_size=0,
        )

    def test_close(self):
        """Test that ObjectFile closes correctly."""
        self.assertFalse(self.object_file._closed)
        self.object_file.close()  # Close the file
        self.assertTrue(self.object_file._closed)
        with self.assertRaises(ValueError):
            self.object_file.close()  # Attempt to close already closed file

    def test_init(self):
        """Test that ObjectFile initializes correctly."""
        self.assertEqual(self.object_file._read_position, 0)
        self.assertFalse(self.object_file._closed)
        self.assertIsInstance(self.object_file._buffer, SimpleBuffer)

    def test_tell(self):
        """Test that ObjectFile returns the correct read position."""
        self.assertEqual(self.object_file.tell(), 0)
        self.object_file._read_position = 10
        self.assertEqual(self.object_file.tell(), 10)

        # Test that ValueError is raised when file is closed
        self.object_file.close()
        with self.assertRaises(ValueError):
            self.object_file.tell()

    def test_readable(self):
        """Test that ObjectFile is readable when file is not closed."""
        self.assertTrue(self.object_file.readable())
        self.object_file.close()
        self.assertFalse(self.object_file.readable())

    def test_seekable(self):
        """Test that ObjectFile is not seekable."""
        self.assertFalse(self.object_file.seekable())

    def test_read(self):
        """Test basic ObjectFile read."""
        # Test reading size zero bytes
        self.assertEqual(self.object_file.read(0), b"")

        # Test that ValueError is raised when file is closed
        self.object_file.close()
        with self.assertRaises(ValueError):
            self.object_file.read()

    def test_read_advances_read_position(self):
        """
        Test that ObjectFile advances the read position correctly as data is read.
        """
        read_data = b"testdata"
        self.object_file._buffer.read = Mock(return_value=read_data)

        # Check initial read position
        self.assertEqual(self.object_file._read_position, 0)

        # Perform the read
        result = self.object_file.read(len(read_data))

        # Check that the read position was advanced by the length of the data
        self.assertEqual(self.object_file._read_position, len(result))

    def test_read_request_more_data(self):
        """
        Test that ObjectFile calls fill() when buffer is empty and additional data is required to
        satisfy a read request.
        """
        # Mock read to return empty data first, then valid data after fill
        self.object_file._buffer.read = Mock(side_effect=[b"", b"testdata"])

        # Mock fill to check if it's called after empty read
        self.object_file._buffer.fill = Mock()

        # Read 8 bytes
        result = self.object_file.read(8)

        # Assert read and fill calls
        self.object_file._buffer.read.assert_has_calls([call(8), call(8)])
        self.object_file._buffer.fill.assert_called_once()

        # Verify the result
        self.assertEqual(result, b"testdata")

    def test_read_no_additional_data_request(self):
        """
        Test that ObjectFile does not call fill() when buffer has enough data to satisfy the read request.
        """
        # Mock read to return sufficient data
        self.object_file._buffer.read = Mock(return_value=b"testdata")

        # Mock fill to verify it is not called
        self.object_file._buffer.fill = Mock()

        # Read 8 bytes
        result = self.object_file.read(8)

        # Assert read and no call to fill
        self.object_file._buffer.read.assert_called_once_with(8)
        self.object_file._buffer.fill.assert_not_called()

        # Verify the result
        self.assertEqual(result, b"testdata")

    def test_read_partial_data_when_eof(self):
        """
        Test that ObjectFile returns available data when EOF is reached before fulfilling the requested read size.
        """
        # Mock read to return partial data, then EOF
        self.object_file._buffer.read = Mock(side_effect=[b"data1", b""])
        self.object_file._buffer._eof = True

        # Read 10 bytes
        result = self.object_file.read(10)

        # Assert read calls and verify returned data
        self.object_file._buffer.read.assert_has_calls([call(10), call(5)])
        self.assertEqual(result, b"data1")

    def test_read_until_eof(self):
        """
        Test that ObjectFile reads all available data until EOF when size is -1.
        """
        # Mock read to return data until EOF
        self.object_file._buffer.read = Mock(side_effect=[b"data1", b"data2", b""])
        self.object_file._buffer._eof = True

        # Read until EOF
        result = self.object_file.read(-1)

        # Assert read calls and verify the result
        self.object_file._buffer.read.assert_has_calls([call(-1), call(-1), call(-1)])
        self.assertEqual(result, b"data1data2")

    def test_read_raises_exception_when_fill_fails(self):
        """
        Test that ObjectFile raises an exception and closes the file when buffer.fill() fails.
        """
        # Mock the buffer's read method to return no data
        self.object_file._buffer.read = Mock(return_value=b"")

        # Mock the buffer's fill method to raise an exception
        self.object_file._buffer.fill = Mock(
            side_effect=Exception("Simulated fill error")
        )

        # Assert that the exception is raised during the read
        with self.assertRaises(Exception) as context:
            self.object_file.read(10)
        self.assertEqual(str(context.exception), "Simulated fill error")

        # Verify that the file was closed after the exception
        self.assertTrue(self.object_file._closed)

    def test_context_manager(self):
        """Test that ObjectFile functions as a context manager."""
        with self.object_file as obj_file:
            self.assertFalse(obj_file._closed)
            self.assertEqual(obj_file._read_position, 0)
            self.assertIsInstance(obj_file._buffer, SimpleBuffer)
        self.assertTrue(obj_file._closed)
