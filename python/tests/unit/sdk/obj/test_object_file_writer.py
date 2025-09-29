#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest
from unittest.mock import Mock
from aistore.sdk.obj.object_writer import ObjectWriter
from aistore.sdk.obj.obj_file.object_file import ObjectFileWriter


class TestObjectFileWriter(unittest.TestCase):
    def setUp(self):
        self.object_writer_mock = Mock(spec=ObjectWriter)
        self.file_writer = ObjectFileWriter(
            obj_writer=self.object_writer_mock, mode="a"
        )

    def test_init_in_write_mode_truncates_content(self):
        """Test that initializing in 'w' mode truncates existing content."""
        self.object_writer_mock.put_content = Mock()
        ObjectFileWriter(obj_writer=self.object_writer_mock, mode="w")
        self.object_writer_mock.put_content.assert_called_once_with(b"")

    def test_init_in_append_mode_does_not_truncate_content(self):
        """Test that initializing in 'a' mode does not truncate existing content."""
        self.object_writer_mock.put_content = Mock()
        ObjectFileWriter(obj_writer=self.object_writer_mock, mode="a")
        self.object_writer_mock.put_content.assert_not_called()

    # pylint: disable=unused-variable
    def test_context_manager_in_write_mode_calls_truncates_content(self):
        """Test that entering the context manager in 'w' mode truncates existing content."""
        self.file_writer = ObjectFileWriter(
            obj_writer=self.object_writer_mock, mode="w"
        )
        self.object_writer_mock.put_content = Mock()
        with self.file_writer as fw:
            # Assert that put_content was called once during __enter__
            self.object_writer_mock.put_content.assert_called_once_with(b"")

    # pylint: disable=unused-variable
    def test_context_manager_in_append_mode_does_not_truncate(self):
        """Test that entering the context manager in 'a' mode does not truncate existing content."""
        self.file_writer = ObjectFileWriter(
            obj_writer=self.object_writer_mock, mode="a"
        )
        self.object_writer_mock.put_content = Mock()
        with self.file_writer as fw:
            # Assert that put_content was not called during __enter__
            self.object_writer_mock.put_content.assert_not_called()

    def test_write(self):
        """Test writing data appends content and updates the handle."""
        data = b"some data"
        self.object_writer_mock.append_content.return_value = "updated-handle"

        written = self.file_writer.write(data)

        self.object_writer_mock.append_content.assert_called_once_with(data, handle="")
        self.assertEqual(self.file_writer._handle, "updated-handle")
        self.assertEqual(written, len(data))

    def test_flush(self):
        """Test flushing the writer finalizes the object."""
        self.file_writer._handle = "test-handle"
        self.file_writer.flush()

        self.object_writer_mock.append_content.assert_called_once_with(
            content=b"", handle="test-handle", flush=True
        )
        self.assertEqual(self.file_writer._handle, "")

    def test_close(self):
        """Test closing the writer finalizes the object."""
        self.file_writer._handle = "final-handle"
        self.file_writer.close()

        self.object_writer_mock.append_content.assert_called_once_with(
            content=b"", handle="final-handle", flush=True
        )
        self.assertTrue(self.file_writer._closed)
        self.assertEqual(self.file_writer._handle, "")

    def test_close_does_nothing_when_already_closed(self):
        """Test closing a closed file does nothing."""
        self.file_writer._closed = True
        self.file_writer.close()
        self.object_writer_mock.append_content.assert_not_called()

    def test_write_raises_exception_when_closed(self):
        """Test writing to a closed file raises an exception."""
        self.file_writer.close()
        with self.assertRaises(ValueError) as context:
            self.file_writer.write(b"data")
        self.assertEqual(str(context.exception), "I/O operation on closed file.")

    def test_flush_raises_exception_when_closed(self):
        """Test flushing a closed file raises an exception."""
        self.file_writer.close()
        with self.assertRaises(ValueError) as context:
            self.file_writer.flush()
        self.assertEqual(str(context.exception), "I/O operation on closed file.")
