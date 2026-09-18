#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=protected-access

import unittest
from unittest.mock import Mock, patch
from io import IOBase
from requests.exceptions import ChunkedEncodingError
from aistore.sdk.obj.obj_file.object_file import ObjectFileReader
from aistore.sdk.obj.obj_file.errors import ObjectFileReaderMaxResumeError
from aistore.sdk.obj.obj_file.stream import ResumableStream
from tests.utils import BadContentIterProvider, cases


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
        self.assertIsInstance(self.object_file._stream, ResumableStream)
        self.assertEqual(self.object_file._stream.position, 0)
        self.assertEqual(self.object_file._stream.resumes, 0)
        self.assertIsNone(self.object_file._remainder)
        self.assertFalse(self.object_file._closed)
        self.content_provider_mock.create_iter.assert_called_once()

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
        self.assertEqual(self.object_file._stream.position, 0)

    def test_read_exact_size_across_chunks(self):
        """Test reading exactly the requested size from across chunks, handling remainder."""
        # Simulate reading a partial size that spans two chunks
        result = self.object_file.read(10)  # Expect to read exactly 10 bytes
        self.assertEqual(
            result, b"chunk1chun"
        )  # 'chunk1' (6 bytes) + 'chun' (4 bytes from chunk2)

        # Since both chunks were fetched, the stream position should be 12 (6 bytes from chunk1 + 6 bytes from chunk2)
        self.assertEqual(self.object_file._stream.position, 12)

        # Check that the remainder contains the rest of 'chunk2'
        self.assertEqual(self.object_file._remainder, bytearray(b"k2"))

    def test_read_less_data_than_requested(self):
        """Test that read() returns available data if less than requested and hits EOF."""
        result = self.object_file.read(20)  # Request more than available
        self.assertEqual(result, b"chunk1chunk2chunk3")
        self.assertEqual(self.object_file._stream.position, len(b"chunk1chunk2chunk3"))

    def test_read_all_data(self):
        """Test that read() reads all data until EOF when no size is specified."""
        # Read all data from the mock iterator
        result = self.object_file.read()  # Read until EOF
        self.assertEqual(result, b"chunk1chunk2chunk3")
        self.assertEqual(self.object_file._stream.position, len(b"chunk1chunk2chunk3"))

    def test_read_remainder_then_new_chunk(self):
        """Test that read() first consumes the remainder before fetching new chunks."""
        # Simulate the remainder being part of a previous chunk ('chunk0') and resume position at 6
        self.object_file._remainder = bytearray(
            b"hunk0"
        )  # Leftover part of chunk0 (5 bytes)
        self.object_file._stream._position = 6

        # Read 10 bytes total, remainder should provide the first 5 bytes ('hunk0')
        result = self.object_file.read(10)

        # Assert that we get exactly 10 bytes in total: remainder 'hunk0' + 5 bytes from 'chunk1'
        self.assertEqual(result, b"hunk0chunk")

        # Since we fetched part of chunk1 to satisfy the read, the position reflects all bytes fetched
        self.assertEqual(
            self.object_file._stream.position, 12
        )  # 6 bytes from chunk1 fetched (position = 6 + 6)

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
        self.object_file._stream._position = 10
        self.object_file._closed = True
        self.object_file._remainder = bytearray(b"remainder")

        with self.object_file as obj_file:
            # State should be reset inside context
            self.assertFalse(obj_file._closed)
            self.assertEqual(self.object_file._stream.position, 0)
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
        return (
            ObjectFileReader(content_provider, max_resume=max_resume_attempts),
            content_provider,
        )

    def test_read_raises_any_exception_and_closes(self):
        """
        Test that ObjectFileReader raises exception during reading and closes the file.

        - Simulate a generic exception occurring during a read operation.
        - Ensure the file is properly closed after the exception is raised.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every other read w/ a max of 3 resumes
        object_file, _ = self._create_reader_with_bad_iterator(
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
        object_file, _ = self._create_reader_with_bad_iterator(
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
        object_file, _ = self._create_reader_with_bad_iterator(
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
        object_file, _ = self._create_reader_with_bad_iterator(
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
        object_file, _ = self._create_reader_with_bad_iterator(
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

    def test_restart_if_not_cached(self):
        """
        Test that ObjectFileReader requests a full stream if the object is not cached on attempt to resume.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every read w/ a max of 1 resumes
        object_file, provider = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=1,
            max_resume_attempts=1,
        )

        # Simulate the object not being cached
        setattr(
            provider.client,
            "head",
            Mock(return_value=Mock(present=False)),
        )
        # Attempt to read should fail after exceeding one max retry
        with patch.object(
            provider, "create_iter", wraps=provider.create_iter
        ) as mock_create_iter:
            with self.assertRaises(ObjectFileReaderMaxResumeError):
                object_file.read()
        # A cold restart must not issue a ranged request.
        mock_create_iter.assert_called_once_with()

    def test_resume_if_cached(self):
        """
        Test that ObjectFileReader resumes from the last position if the object is cached.
        """
        # Create an ObjectFileReader with a bad iterator that raises ChunkedEncodingError
        # and simulates a failure on every read w/ a max of 1 resumes
        object_file, provider = self._create_reader_with_bad_iterator(
            exc=ChunkedEncodingError,
            fail_on_read=1,
            max_resume_attempts=1,
        )

        # Simulate the object being cached
        setattr(
            provider.client,
            "head",
            Mock(return_value=Mock(present=True)),
        )
        # Attempt to read should fail after exceeding one max retry
        with patch.object(
            provider, "create_iter", wraps=provider.create_iter
        ) as mock_create_iter:
            with self.assertRaises(ObjectFileReaderMaxResumeError):
                object_file.read()
        # A cached resume must issue a ranged request rather than start over.
        mock_create_iter.assert_called_once()
        self.assertIn("offset", mock_create_iter.call_args.kwargs)


class TestObjectFileReaderColdResume(unittest.TestCase):
    """Buffered reads must survive a cold restart without returning bytes twice."""

    def setUp(self):
        self.data = b"0123456789abcdef"

    def _make_reader(self, streams):
        """Each stream specifies its end offset, chunk size, and terminal error."""
        provider = Mock()
        provider.client.path = "objects/bucket/object"
        provider.client.head.return_value = Mock(present=False)
        provider.expected_end_position = len(self.data)
        provider.offsets = []
        provider.closed_streams = []

        def create_iter(offset=0):
            stream_index = len(provider.offsets)
            provider.offsets.append(offset)
            end, chunk_size, error = streams[stream_index]

            def iterator():
                try:
                    for pos in range(offset, end, chunk_size):
                        yield self.data[pos : min(pos + chunk_size, end)]
                    if error:
                        raise error
                finally:
                    provider.closed_streams.append(stream_index)

            return iterator()

        provider.create_iter.side_effect = create_iter
        return ObjectFileReader(provider, max_resume=3), provider

    @cases(ChunkedEncodingError("interrupted"), None)
    def test_uncached_partial_reads_preserve_remainder_and_size(self, error):
        """Both a broken stream and a clean short EOF restart underneath a buffered read."""
        reader, provider = self._make_reader([(8, 4, error), (16, 3, None)])

        self.assertEqual(reader.read(3), b"012")
        self.assertEqual(reader.read(6), b"345678")
        self.assertEqual(reader.read(), b"9abcdef")
        self.assertEqual(reader.read(), b"")
        self.assertEqual(provider.offsets, [0, 0])

    def test_close_after_uncached_restart_closes_active_stream(self):
        reader, provider = self._make_reader(
            [(8, 4, ChunkedEncodingError("interrupted")), (16, 3, None)]
        )

        self.assertEqual(reader.read(9), self.data[:9])
        reader.close()

        self.assertEqual(provider.closed_streams, [0, 1])
        self.assertFalse(reader.readable())
