#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest
import warnings
from unittest.mock import Mock, call
from aistore.sdk.obj.object_writer import ObjectWriter
from aistore.sdk.obj.obj_file.object_file import ObjectFileWriter


class TestObjectFileWriter(unittest.TestCase):
    def setUp(self):
        self.object_writer_mock = Mock(spec=ObjectWriter)
        self.file_writer = ObjectFileWriter(
            obj_writer=self.object_writer_mock, mode="a"
        )
        self.addCleanup(self.file_writer.close)

    def test_init_in_write_mode_truncates_content(self):
        """Test that initializing in 'w' mode truncates existing content."""
        self.object_writer_mock.put_content = Mock()
        ObjectFileWriter(obj_writer=self.object_writer_mock, mode="w").close()
        self.object_writer_mock.put_content.assert_called_once_with(b"")

    def test_init_in_append_mode_does_not_truncate_content(self):
        """Test that initializing in 'a' mode does not truncate existing content."""
        self.object_writer_mock.put_content = Mock()
        ObjectFileWriter(obj_writer=self.object_writer_mock, mode="a").close()
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

        self.object_writer_mock.reset_mock()
        with self.assertRaises(ValueError), self.file_writer:
            self.fail("Entered a closed writer")
        self.object_writer_mock.put_content.assert_not_called()

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

        written = self.file_writer.write(buffer=data)

        self.object_writer_mock.append_content.assert_called_once_with(data, handle="")
        self.assertEqual(self.file_writer._handle, "updated-handle")
        self.assertEqual(written, len(data))

        self.file_writer.writelines([data])
        self.object_writer_mock.append_content.assert_called_with(
            data, handle="updated-handle"
        )

    def test_flush(self):
        """Test flushing the writer finalizes the object."""
        self.file_writer._handle = "test-handle"
        self.file_writer.flush()

        self.object_writer_mock.append_content.assert_called_once_with(
            content=b"", handle="test-handle", flush=True
        )
        self.assertEqual(self.file_writer._handle, "")

    def test_close(self):
        """A failed close retains the handle; a retry finalizes the object."""
        self.assertFalse(self.file_writer.closed)
        self.assertTrue(self.file_writer.writable())
        self.file_writer._handle = "final-handle"
        self.object_writer_mock.append_content.side_effect = [
            OSError("flush failed"),
            "",
        ]
        with self.assertRaises(OSError):
            self.file_writer.close()
        self.assertFalse(self.file_writer.closed)
        self.assertEqual(self.file_writer._handle, "final-handle")
        self.file_writer.close()

        self.assertEqual(
            self.object_writer_mock.append_content.call_args_list,
            [call(content=b"", handle="final-handle", flush=True)] * 2,
        )
        self.assertTrue(self.file_writer.closed)
        self.assertFalse(self.file_writer.writable())
        self.assertEqual(self.file_writer._handle, "")

    def test_close_does_nothing_when_already_closed(self):
        """Test closing a closed file does nothing."""
        self.file_writer._closed = True
        self.file_writer.close()
        self.assertTrue(self.file_writer.closed)
        self.object_writer_mock.append_content.assert_not_called()

    def test_finalizer_warns_without_flushing(self):
        """Finalization warns about an open writer without sending a request."""
        # pylint: disable=unnecessary-dunder-call,no-value-for-parameter
        self.file_writer._handle = "pending"
        with self.assertWarnsRegex(ResourceWarning, "unclosed .*ObjectFileWriter"):
            self.file_writer.__del__()
        self.assertFalse(self.file_writer.closed)
        self.assertEqual(self.file_writer._handle, "pending")
        self.object_writer_mock.append_content.assert_not_called()

        self.file_writer.close()
        self.object_writer_mock.reset_mock()
        with warnings.catch_warnings():
            warnings.simplefilter("error", ResourceWarning)
            self.file_writer.__del__()
            writer = ObjectFileWriter.__new__(ObjectFileWriter)
            writer.__del__()
            self.object_writer_mock.put_content.side_effect = OSError("PUT failed")
            with self.assertRaises(OSError):
                writer.__init__(self.object_writer_mock, mode="w")
            self.assertTrue(writer.closed)
            writer.close()
            writer.__del__()
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
