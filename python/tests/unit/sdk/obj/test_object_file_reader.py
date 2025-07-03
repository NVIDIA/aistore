#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest
from unittest.mock import Mock, patch
from io import IOBase
from requests.exceptions import ChunkedEncodingError
from aistore.sdk.obj.obj_file.object_file import ObjectFileReader
from aistore.sdk.obj.obj_file.errors import ObjectFileReaderMaxResumeError
from tests.utils import BadContentIterProvider


class TestObjectFileReader(unittest.TestCase):

    def setUp(self):
        self.content_provider_mock = Mock()
        self.mock_generator = Mock()
        self.mock_generator.__next__ = Mock(
            side_effect=[b"chunk1", b"chunk2", b"chunk3", StopIteration()]
        )
        self.mock_generator.close = Mock()
        self.content_provider_mock.create_iter.return_value = self.mock_generator
        self.object_file = ObjectFileReader(
            content_provider=self.content_provider_mock,
            max_resume=3,
        )

    def test_init(self):
        """Test that ObjectFileReader initializes all attributes correctly."""
        # Ensure all attributes are initialized properly
        self.assertEqual(self.object_file._content_provider, self.content_provider_mock)
        self.assertEqual(self.object_file._max_resume, 3)
        self.assertEqual(self.object_file._resume_position, 0)
        self.assertEqual(self.object_file._resume_total, 0)
        self.assertIsNone(self.object_file._remainder)
        self.assertFalse(self.object_file._closed)
        self.assertIsNotNone(self.object_file._content_iter)

        # Verify ObjectFileReader extends IOBase
        self.assertIsInstance(self.object_file, IOBase)

    def test_close(self):
        """Test that ObjectFileReader closes correctly."""
        # Read some data to initialize the generator
        self.object_file.read(4)

        # Verify file is not closed initially
        self.assertFalse(self.object_file._closed)

        # Close the file
        self.object_file.close()

        # Verify file is closed and stream is closed
        self.assertTrue(self.object_file._closed)
        self.mock_generator.close.assert_called_once()

    def test_readable(self):
        """Test that ObjectFileReader is readable when not closed and unreadable when closed."""
        self.assertTrue(self.object_file.readable())
        self.object_file.close()
        self.assertFalse(self.object_file.readable())

    def test_seekable(self):
        """Test that ObjectFileReader is not seekable."""
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
        self.assertEqual(self.object_file._remainder, bytearray(b"k2"))

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
        self.object_file._remainder = bytearray(
            b"hunk0"
        )  # Leftover part of chunk0 (5 bytes)
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
            self.object_file._remainder, bytearray(b"1")
        )  # The remaining part of chunk1 is '1'

    def test_read_raises_exception_when_closed(self):
        """Test that read() raises an exception if called on a closed ObjectFileReader."""
        self.object_file.close()
        with self.assertRaises(ValueError) as context:
            self.object_file.read(10)
        self.assertEqual(str(context.exception), "I/O operation on closed file.")

    def test_context_manager(self):
        """Test that ObjectFileReader can be used with context manager, resets state, and closes stream."""
        # Modify the object's state to simulate previous use
        self.object_file._resume_position = 10
        self.object_file._closed = True
        self.object_file._remainder = bytearray(b"remainder")

        with self.object_file as obj_file:
            # State should be reset inside context
            self.assertFalse(obj_file._closed)
            self.assertEqual(self.object_file._resume_position, 0)
            self.assertIsNone(self.object_file._remainder)

            # Read some data to initialize the generator
            obj_file.read(4)

        # After context, file should be closed and stream should be closed
        self.assertTrue(self.object_file._closed)
        self.mock_generator.close.assert_called_once()


class TestObjectFileReaderResume(unittest.TestCase):

    def setUp(self):
        self.data = b"chunk1chunk2chunk3chunk4"
        self.chunk_size = 6

    def _create_reader_with_bad_iterator(self, exc, fail_on_read, max_resume_attempts):
        err_instance = (
            exc if isinstance(exc, BaseException) else exc("Simulated Exception")
        )
        content_provider = BadContentIterProvider(
            data=self.data,
            fail_on_read=fail_on_read,
            chunk_size=self.chunk_size,
            error=err_instance,
        )
        return ObjectFileReader(content_provider, max_resume=max_resume_attempts)

    def test_read_raises_any_exception_and_closes(self):
        """
        Test that ObjectFileReader raises exception during reading and closes the file.

        - Simulate a generic exception occurring during a read operation.
        - Ensure the file is properly closed after the exception is raised.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every other read w/ a max of 3 resumes
        object_file = self._create_reader_with_bad_iterator(
            exc=Exception,
            fail_on_read=2,
            max_resume_attempts=3,
        )

        # Assert that the exception is raised during the read
        with self.assertRaises(Exception) as context:
            object_file.read()
        self.assertEqual(str(context.exception), "Simulated Exception")

        # Verify that the file was closed after the exception
        self.assertTrue(object_file._closed)

    def test_read_success_after_resumes(self):
        """
        Test that ObjectFileReader successfully reads w/ resumes after encountering `ChunkedEncodingError`
        within the allowed `max_resume` attempts, and eventually reads the entire content.

        - Read retrieves chunk1 successfully.
        - Read fails to retrieve chunk2, resumes and gets chunk2.
        - Read fails to retrieve chunk3, resumes and gets chunk3.
        - Read fails to retrieve chunk4, resumes and gets chunk4.
        - Read returns the entire content.

        Total of 3 resumes, which is within the set limit of `max_resume=3`.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every other read w/ a max of 3 resumes
        object_file = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=2,
            max_resume_attempts=3,
        )

        # Read the entire content and verify it handles the error and resumes correctly
        result = object_file.read()

        # Ensure that we received the full data
        self.assertEqual(result, self.data)

        # Verify that the file was not closed
        self.assertFalse(object_file._closed)

    def test_read_fail_after_max_retries(self):
        """
        Test that ObjectFileReader fails and raises an `ObjectFileReaderMaxResumeError` after exceeding the
        allowed `max_resume` attempts during multiple stream interruptions.

        - Reads chunk1 successfully.
        - Fails to retrieve chunk2, resumes and gets chunk2.
        - Fails to retrieve chunk3, resumes and gets chunk3.
        - Fails to retrieve chunk4.
        - Raises `ObjectFileReaderMaxResumeError`.

        Total of 3 resumes, which exceeds the set limit of `max_resume=3`.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every other read w/ a max of 2 resumes
        object_file = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=2,
            max_resume_attempts=2,
        )

        # Attempting to read should fail after exceeding max retries
        with self.assertRaises(ObjectFileReaderMaxResumeError):
            object_file.read()

        # Verify that the file was closed after the exception
        self.assertTrue(object_file._closed)

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
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every other read w/ a max of 3 resumes
        object_file = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=2,
            max_resume_attempts=3,
        )

        # Read portion of content and verify it handles the error and resumes correctly
        result = object_file.read(10)
        self.assertEqual(result, b"chunk1chun")

        # Read rest of content and verify it handles the error and resumes correctly
        result = object_file.read(14)
        self.assertEqual(result, b"k2chunk3chunk4")

        # Verify that the file was not closed
        self.assertFalse(object_file._closed)

    def test_multiple_reads_fail_after_resumes(self):
        """
        Test that multiple read operations fail after exceeding the allowed `max_resume` limit.

        - First read retrieves chunk1 successfully.
        - First read fails to retrieve chunk2, resumes and gets chunk2.
        - First read returns 'chunk1chun', leaving 'k2' in the remainder.
        - Second read consumes 'k2' from the remainder.
        - Second read fails to retrieve chunk3, resumes and gets chunk3.
        - Second read fails to retrieve chunk4, raises `ObjectFileReaderMaxResumeError`.

        Total of 3 resumes, exceeding the set limit of `max_resume=2`.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every other read w/ a max of 2 resumes
        object_file = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=2,
            max_resume_attempts=2,
        )

        # Read portion of content and verify it handles the error and resumes correctly
        result = object_file.read(10)
        self.assertEqual(result, b"chunk1chun")

        # Attempting to read should fail after exceeding max retries
        with self.assertRaises(ObjectFileReaderMaxResumeError):
            object_file.read(14)

        # Verify that the file was closed after the exception
        self.assertTrue(object_file._closed)

    def test_reset_if_not_cached(self):
        """
        Test that ObjectFileReader resets correctly if the object is not cached on attempt to resume.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every read w/ a max of 1 resumes
        object_file = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=1,
            max_resume_attempts=1,
        )

        # Simulate the object not being cached
        setattr(
            object_file._content_provider.client,
            "head",
            Mock(return_value=Mock(present=False)),
        )
        # Attempt to read should fail after exceeding one max retry
        with patch.object(
            object_file, "_reset", wraps=object_file._reset
        ) as mock_reset:
            with self.assertRaises(ObjectFileReaderMaxResumeError):
                object_file.read()
        # Verify that _reset was called once with retain_resumes=True
        mock_reset.assert_called_once_with(retain_resumes=True)

    def test_resume_if_cached(self):
        """
        Test that ObjectFileReader resumes (does not reset) if the object is cached on attempt to resume.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every read w/ a max of 1 resumes
        object_file = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=1,
            max_resume_attempts=1,
        )

        # Simulate the object being cached
        setattr(
            object_file._content_provider.client,
            "head",
            Mock(return_value=Mock(present=True)),
        )
        # Attempt to read should fail after exceeding one max retry
        with patch.object(
            object_file, "_reset", wraps=object_file._reset
        ) as mock_reset:
            with self.assertRaises(ObjectFileReaderMaxResumeError):
                object_file.read()
        # Verify that _reset was not called
        mock_reset.assert_not_called()
