#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest
from unittest.mock import Mock

from aistore.sdk.obj.obj_file.buffer import SimpleBuffer
from aistore.sdk.obj.obj_file.object_file import ObjectFile


class TestObjectFile(unittest.TestCase):

    def setUp(self):
        self.content_iterator_mock = Mock()
        self.object_file = ObjectFile(
            content_iterator=self.content_iterator_mock, max_resume=0
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
        self.assertEqual(self.object_file._max_resume, 0)
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

    def test_context_manager(self):
        """Test that ObjectFile functions as a context manager."""
        with self.object_file as obj_file:
            self.assertFalse(obj_file._closed)
            self.assertEqual(obj_file._read_position, 0)
            self.assertIsInstance(obj_file._buffer, SimpleBuffer)
        self.assertTrue(obj_file._closed)

    def test_read_and_position_advance(self):
        """
        Test that the internal buffer is used to fill and read data,
        and that the read position advances correctly.
        """
        read_data = b"testdata"

        self.object_file._buffer.fill = Mock()
        self.object_file._buffer.read = Mock(return_value=read_data)

        data = self.object_file.read(len(read_data))

        # Assert that the buffer's fill method was called
        self.object_file._buffer.fill.assert_called_once()

        # Assert that the buffer's read method was called with the correct size
        self.object_file._buffer.read.assert_called_once_with(len(read_data))

        # Assert that the data returned is correct
        self.assertEqual(data, read_data)

        # Check that the read position was advanced by the length of the data
        self.assertEqual(self.object_file.tell(), len(data))
