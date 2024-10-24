#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest
from unittest.mock import Mock
from io import IOBase
from aistore.sdk.obj.obj_file.object_file import ObjectFile
from aistore.sdk.obj.obj_file.errors import ObjectFileMaxResumeError
from tests.utils import BadContentIterator


class TestObjectFile(unittest.TestCase):

    def setUp(self):
        self.content_iterator_mock = Mock()
        self.content_iterator_mock.iter_from_position.return_value = iter(
            [b"chunk1", b"chunk2", b"chunk3"]
        )
        self.object_file = ObjectFile(
            content_iterator=self.content_iterator_mock,
            max_resume=3,
        )

    def test_init(self):
        """Test that ObjectFile initializes all attributes correctly."""
        # Ensure all attributes are initialized properly
        self.assertEqual(self.object_file._content_iterator, self.content_iterator_mock)
        self.assertEqual(self.object_file._max_resume, 3)
        self.assertEqual(self.object_file._resume_position, 0)
        self.assertEqual(self.object_file._resume_total, 0)
        self.assertEqual(self.object_file._remainder, b"")
        self.assertFalse(self.object_file._closed)

        # Verify that iter_from_position(0) is called
        self.content_iterator_mock.iter_from_position.assert_called_once_with(0)

        # Verify ObjectFile extends IOBase
        self.assertIsInstance(self.object_file, IOBase)

    def test_close(self):
        """Test that ObjectFile closes correctly and raises an error if closed again."""
        self.assertFalse(self.object_file._closed)
        self.object_file.close()
        self.assertTrue(self.object_file._closed)

    def test_readable(self):
        """Test that ObjectFile is readable when not closed and unreadable when closed."""
        self.assertTrue(self.object_file.readable())
        self.object_file.close()
        self.assertFalse(self.object_file.readable())

    def test_seekable(self):
        """Test that ObjectFile is not seekable."""
        self.assertFalse(self.object_file.seekable())

    def test_read_zero_size(self):
        """Test that reading zero bytes returns empty bytes."""
        result = self.object_file.read(0)
        self.assertEqual(result, b"")
        self.assertEqual(self.object_file._resume_position, 0)

    def test_read_exact_size_across_chunks(self):
        """Test reading exactly the requested size from across chunks, handling remainder."""
        # Simulate reading a partial size that spans two chunks
        result = self.object_file.read(10)  # Expect to read exactly 10 bytes
        self.assertEqual(
            result, b"chunk1chun"
        )  # 'chunk1' (6 bytes) + 'chun' (4 bytes from chunk2)

        # Since both chunks were fetched, resume_position should be 12 (6 bytes from chunk1 + 6 bytes from chunk2)
        self.assertEqual(self.object_file._resume_position, 12)

        # Check that the remainder contains the rest of 'chunk2'
        self.assertEqual(self.object_file._remainder, b"k2")

    def test_read_less_data_than_requested(self):
        """Test that read() returns available data if less than requested and hits EOF."""
        result = self.object_file.read(20)  # Request more than available
        self.assertEqual(result, b"chunk1chunk2chunk3")
        self.assertEqual(self.object_file._resume_position, len(b"chunk1chunk2chunk3"))

    def test_read_all_data(self):
        """Test that read() reads all data until EOF when no size is specified."""
        # Read all data from the mock iterator
        result = self.object_file.read()  # Read until EOF
        self.assertEqual(result, b"chunk1chunk2chunk3")
        self.assertEqual(self.object_file._resume_position, len(b"chunk1chunk2chunk3"))

    def test_read_remainder_then_new_chunk(self):
        """Test that read() first consumes the remainder before fetching new chunks."""
        # Simulate the remainder being part of a previous chunk ('chunk0') and resume position at 6
        self.object_file._remainder = b"hunk0"  # Leftover part of chunk0 (5 bytes)
        self.object_file._resume_position = 6

        # Read 10 bytes total, remainder should provide the first 5 bytes ('hunk0')
        result = self.object_file.read(10)

        # Assert that we get exactly 10 bytes in total: remainder 'hunk0' + 5 bytes from 'chunk1'
        self.assertEqual(result, b"hunk0chunk")

        # Since we fetched part of chunk1 to satisfy the read, _resume_position should reflect the total bytes fetched
        self.assertEqual(
            self.object_file._resume_position, 12
        )  # 6 bytes from chunk1 fetched (resume_position = 6 + 6)

        # Ensure the remainder has only the leftover part of chunk1
        self.assertEqual(
            self.object_file._remainder, b"1"
        )  # The remaining part of chunk1 is '1'

    def test_read_raises_exception_when_closed(self):
        """Test that read() raises an exception if called on a closed ObjectFile."""
        self.object_file.close()
        with self.assertRaises(ValueError) as context:
            self.object_file.read(10)
        self.assertEqual(str(context.exception), "I/O operation on closed file.")

    def test_context_manager(self):
        """Test that ObjectFile functions as a context manager and resets state."""
        # Modify the object's state to simulate previous use
        self.object_file._resume_position = 10
        self.object_file._closed = True

        with self.object_file as obj_file:
            # State should be reset inside context
            self.assertFalse(obj_file._closed)
            self.assertEqual(self.object_file._resume_position, 0)
            self.assertEqual(self.object_file._remainder, b"")

        # After context, file should be closed
        self.assertTrue(self.object_file._closed)


class TestObjectFileResume(unittest.TestCase):

    def setUp(self):
        self.data = b"chunk1chunk2chunk3chunk4"
        self.chunk_size = 6
        self.max_resume_attempts = 3

        # Simulate a content iterator that fails after reading each chunk (fails every other).
        self.content_iterator = BadContentIterator(
            data=self.data, fail_on_read=2, chunk_size=self.chunk_size
        )
        self.object_file = ObjectFile(
            content_iterator=self.content_iterator,
            max_resume=self.max_resume_attempts,
        )

    def test_read_raises_any_exception_and_closes(self):
        """
        Test that ObjectFile raises exception during reading and closes the file.

        - Simulate a generic exception occurring during a read operation.
        - Ensure the file is properly closed after the exception is raised.
        """
        # Reinitialize BadContentIterator to raise a generic Exception
        self.content_iterator = BadContentIterator(
            data=self.data,
            fail_on_read=2,
            chunk_size=self.chunk_size,
            error=Exception("Simulated Exception"),
        )
        self.object_file = ObjectFile(
            content_iterator=self.content_iterator,
            max_resume=self.max_resume_attempts,
        )

        # Assert that the exception is raised during the read
        with self.assertRaises(Exception) as context:
            self.object_file.read()
        self.assertEqual(str(context.exception), "Simulated Exception")

        # Verify that the file was closed after the exception
        self.assertTrue(self.object_file._closed)

    def test_read_success_after_resumes(self):
        """
        Test that ObjectFile successfully reads w/ resumes after encountering `ChunkedEncodingError`
        within the allowed `max_resume` attempts, and eventually reads the entire content.

        - Read retrieves chunk1 successfully.
        - Read fails to retrieve chunk2, resumes and gets chunk2.
        - Read fails to retrieve chunk3, resumes and gets chunk3.
        - Read fails to retrieve chunk4, resumes and gets chunk4.
        - Read returns the entire content.

        Total of 3 resumes, which is within the set limit of `max_resume=3`.
        """
        # Read the entire content and verify it handles the error and resumes correctly
        result = self.object_file.read()

        # Ensure that we received the full data
        self.assertEqual(result, self.data)

        # Verify that the file was not closed
        self.assertFalse(self.object_file._closed)

    def test_read_fail_after_max_retries(self):
        """
        Test that ObjectFile fails and raises an `ObjectFileMaxResumeError` after exceeding the
        allowed `max_resume` attempts during multiple stream interruptions.

        - Reads chunk1 successfully.
        - Fails to retrieve chunk2, resumes and gets chunk2.
        - Fails to retrieve chunk3, resumes and gets chunk3.
        - Fails to retrieve chunk4.
        - Raises `ObjectFileMaxResumeError`.

        Total of 3 resumes, which exceeds the set limit of `max_resume=3`.
        """
        # Reduce max_resume to 2 to test failure scenario
        self.object_file._max_resume = 2

        # Attempting to read should fail after exceeding max retries
        with self.assertRaises(ObjectFileMaxResumeError):
            self.object_file.read()

        # Verify that the file was closed after the exception
        self.assertTrue(self.object_file._closed)

    def test_multiple_reads_success_after_resumes(self):
        """
        Test that multiple read operations succeed, with resumes after encountering `ChunkedEncodingError`.

        - First read retrieves chunk1 successfully.
        - First read fails to retrieve chunk2, resumes and gets chunk2.
        - First read returns 'chunk1chun', leaving 'k2' in the remainder.

        - Second read consumes 'k2' from the remainder.
        - Second read fails to retrieve chunk3, resumes and gets chunk3.
        - Second read fails to retrieve chunk4, resumes and gets chunk4.
        - Second read returns 'k2chunk3chunk4'.

        Total of 3 resumes, within the set limit of `max_resume=3`.
        """
        # Read portion of content and verify it handles the error and resumes correctly
        result = self.object_file.read(10)
        self.assertEqual(result, b"chunk1chun")

        # Read rest of content and verify it handles the error and resumes correctly
        result = self.object_file.read(14)
        self.assertEqual(result, b"k2chunk3chunk4")

        # Verify that the file was not closed
        self.assertFalse(self.object_file._closed)

    def test_multiple_reads_fail_after_resumes(self):
        """
        Test that multiple read operations fail after exceeding the allowed `max_resume` limit.

        - First read retrieves chunk1 successfully.
        - First read fails to retrieve chunk2, resumes and gets chunk2.
        - First read returns 'chunk1chun', leaving 'k2' in the remainder.
        - Second read consumes 'k2' from the remainder.
        - Second read fails to retrieve chunk3, resumes and gets chunk3.
        - Second read fails to retrieve chunk4, raises `ObjectFileMaxResumeError`.

        Total of 3 resumes, exceeding the set limit of `max_resume=2`.
        """
        # Reduce max_resume to 2 to test failure scenario
        self.object_file._max_resume = 2

        # Read portion of content and verify it handles the error and resumes correctly
        result = self.object_file.read(10)
        self.assertEqual(result, b"chunk1chun")

        # Attempting to read should fail after exceeding max retries
        with self.assertRaises(ObjectFileMaxResumeError):
            self.object_file.read(14)

        # Verify that the file was closed after the exception
        self.assertTrue(self.object_file._closed)
