#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch, MagicMock, ANY
from io import BytesIO
import tarfile

from aistore.sdk.batch.extractor.tar_stream_extractor import TarStreamExtractor
from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.batch.batch_response import BatchResponseItem


# pylint: disable=duplicate-code
class TestTarStreamExtractor(unittest.TestCase):
    """Unit tests for TarStreamExtractor class."""

    def setUp(self):
        self.tar_extractor = TarStreamExtractor()

        # Create actual BatchRequest with real objects for TAR
        self.batch_request = BatchRequest(
            output_format=".tar", continue_on_err=True, streaming=True
        )

        # Create mock objects for testing
        test_obj1 = Mock()
        test_obj1.name = "missing.txt"
        test_obj1.bucket_provider.value = "ais"
        test_obj1.bucket_name = "test-bucket"

        test_obj2 = Mock()
        test_obj2.name = "file1.txt"
        test_obj2.bucket_provider.value = "ais"
        test_obj2.bucket_name = "test-bucket"

        self.batch_request.add_object_request(test_obj1)
        self.batch_request.add_object_request(test_obj2)

        # Create mock response
        self.mock_response = Mock()

    def test_get_supported_formats(self):
        """Test that TarStreamExtractor returns correct supported formats."""
        supported_formats = self.tar_extractor.get_supported_formats()
        expected_formats = (".tar.gz", ".tgz", ".tar")
        self.assertEqual(supported_formats, expected_formats)

    @patch("tarfile.open")
    def test_successful_extraction(self, mock_tar_open):
        """Test successful file extraction from tar stream."""
        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Create mock tarinfo and file content
        mock_tarinfo = Mock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "file1.txt"

        mock_file = Mock()
        mock_file.read.return_value = b"file content"

        # Setup iterator and extraction
        mock_tar_file.__iter__.return_value = [mock_tarinfo]
        mock_tar_file.extractfile.return_value.__enter__.return_value = mock_file

        # Execute
        result = list(
            self.tar_extractor.extract(
                self.mock_response, BytesIO(b"tar data"), self.batch_request, None
            )
        )

        # Verify
        self.assertEqual(len(result), 1)
        response_item, content = result[0]
        self.assertIsInstance(response_item, BatchResponseItem)
        self.assertEqual(content, b"file content")
        self.assertFalse(response_item.is_missing)

        # Verify tarfile was opened correctly - use ANY for BytesIO comparison
        mock_tar_open.assert_called_once_with(fileobj=ANY, mode="r|*")

    @patch("tarfile.open")
    def test_skip_non_file_entries(self, mock_tar_open):
        """Test non-file entries (directories) are skipped."""
        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Create directory entry
        dir_tarinfo = Mock()
        dir_tarinfo.isfile.return_value = False

        # Create file entry
        file_tarinfo = Mock()
        file_tarinfo.isfile.return_value = True
        file_tarinfo.name = "file.txt"

        mock_file = Mock()
        mock_file.read.return_value = b"content"

        # Setup iterator
        mock_tar_file.__iter__.return_value = [dir_tarinfo, file_tarinfo]
        mock_tar_file.extractfile.return_value.__enter__.return_value = mock_file

        # Execute
        result = list(
            self.tar_extractor.extract(
                self.mock_response, BytesIO(b"tar data"), self.batch_request, None
            )
        )

        # Should only process the file, not the directory
        self.assertEqual(len(result), 1)
        _, content = result[0]
        self.assertEqual(content, b"content")

    @patch("tarfile.open")
    def test_missing_file_detection(self, mock_tar_open):
        """Test detection of missing files marked with __404__/ prefix."""
        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Create mock tarinfo for missing file
        mock_tarinfo = Mock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "__404__/test-bucket/missing.txt"

        mock_file = Mock()
        mock_file.read.return_value = b""

        # Setup iterator
        mock_tar_file.__iter__.return_value = [mock_tarinfo]
        mock_tar_file.extractfile.return_value.__enter__.return_value = mock_file

        # Execute
        response_item, _ = list(
            self.tar_extractor.extract(
                self.mock_response, BytesIO(b"tar data"), self.batch_request, None
            )
        )[0]

        # Verify missing flag is set
        self.assertTrue(response_item.is_missing)

    @patch("tarfile.open")
    def test_non_streaming_mode(self, mock_tar_open):
        """Test extraction in non-streaming mode uses batch_response."""
        # Create non-streaming request
        batch_request_non_streaming = BatchRequest(
            output_format=".tar", continue_on_err=True, streaming=False
        )

        # Create mock batch response
        mock_batch_response = Mock()
        mock_response_item = Mock()
        mock_response_item.bucket = "test-bucket"
        mock_response_item.obj_name = "file1.txt"
        mock_batch_response.responses = [mock_response_item]

        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Create mock tarinfo
        mock_tarinfo = Mock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "file1.txt"

        mock_file = Mock()
        mock_file.read.return_value = b"content"

        # Setup iterator
        mock_tar_file.__iter__.return_value = [mock_tarinfo]
        mock_tar_file.extractfile.return_value.__enter__.return_value = mock_file

        # Execute
        result = list(
            self.tar_extractor.extract(
                self.mock_response,
                BytesIO(b"tar data"),
                batch_request_non_streaming,
                mock_batch_response,
            )
        )

        # Verify it used the batch_response
        self.assertEqual(len(result), 1)
        response_item, content = result[0]
        self.assertEqual(response_item, mock_response_item)
        self.assertEqual(content, b"content")

    @patch("tarfile.open")
    @patch("aistore.sdk.batch.extractor.tar_stream_extractor.logger")
    def test_continue_on_error(self, mock_logger, mock_tar_open):
        """Test extraction continues on error when continue_on_err=True."""
        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Create mock tarinfo that will cause error
        mock_tarinfo = Mock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "file1.txt"

        # Simulate extraction error
        mock_tar_file.__iter__.return_value = [mock_tarinfo]
        mock_tar_file.extractfile.side_effect = tarfile.TarError("Extraction failed")

        # Execute with continue_on_err=True
        result = list(
            self.tar_extractor.extract(
                self.mock_response,
                BytesIO(b"tar data"),
                self.batch_request,  # continue_on_err=True by default
                None,
            )
        )

        # Should log error but continue (no results due to error)
        self.assertEqual(len(result), 0)
        mock_logger.error.assert_called_once()
        # Verify response was closed at the end (in finally block)
        self.mock_response.close.assert_called_once()

    @patch("tarfile.open")
    def test_raise_on_error_when_continue_disabled(self, mock_tar_open):
        """Test extraction raises exception when continue_on_err=False."""
        # Create batch request with continue_on_err=False
        batch_request_no_continue = BatchRequest(
            output_format=".tar", continue_on_err=False, streaming=True
        )

        # Add a test object
        test_obj = Mock()
        test_obj.name = "file1.txt"
        test_obj.bucket_provider.value = "ais"
        test_obj.bucket_name = "test-bucket"

        batch_request_no_continue.add_object_request(test_obj)

        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Create mock tarinfo
        mock_tarinfo = Mock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "file1.txt"

        # Simulate extraction error
        mock_tar_file.__iter__.return_value = [mock_tarinfo]
        mock_tar_file.extractfile.side_effect = tarfile.TarError("Extraction failed")

        # Should raise RuntimeError (not TarError due to wrapping)
        with self.assertRaises(RuntimeError) as context:
            list(
                self.tar_extractor.extract(
                    self.mock_response,
                    BytesIO(b"tar data"),
                    batch_request_no_continue,
                    None,
                )
            )

        # Verify the error message
        self.assertIn("Failed to extract file", str(context.exception))
        # Verify response was closed due to error
        self.mock_response.close.assert_called_once()

    @patch("tarfile.open")
    def test_tar_open_error(self, mock_tar_open):
        """Test handling of tarfile.open errors."""
        # Simulate tarfile open error
        mock_tar_open.side_effect = tarfile.TarError("Cannot open tar file")

        # Should raise RuntimeError
        with self.assertRaises(RuntimeError) as context:
            list(
                self.tar_extractor.extract(
                    self.mock_response,
                    BytesIO(b"bad tar data"),
                    self.batch_request,
                    None,
                )
            )

        self.assertIn("Failed to extract tar stream", str(context.exception))
        # Verify response was closed due to error
        self.mock_response.close.assert_called_once()

    @patch("tarfile.open")
    def test_multiple_files_extraction(self, mock_tar_open):
        """Test extraction of multiple files from tar stream."""
        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Create multiple mock tarinfo entries
        mock_tarinfo1 = Mock()
        mock_tarinfo1.isfile.return_value = True
        mock_tarinfo1.name = "file1.txt"

        mock_tarinfo2 = Mock()
        mock_tarinfo2.isfile.return_value = True
        mock_tarinfo2.name = "file2.txt"

        # Create context managers for the files
        mock_file1 = MagicMock()
        mock_file1.__enter__.return_value.read.return_value = b"content1"

        mock_file2 = MagicMock()
        mock_file2.__enter__.return_value.read.return_value = b"content2"

        # Setup iterator and extraction
        mock_tar_file.__iter__.return_value = [mock_tarinfo1, mock_tarinfo2]
        mock_tar_file.extractfile.side_effect = [mock_file1, mock_file2]

        # Execute
        result = list(
            self.tar_extractor.extract(
                self.mock_response, BytesIO(b"tar data"), self.batch_request, None
            )
        )

        # Verify both files were extracted
        self.assertEqual(len(result), 2)

        response_item1, content1 = result[0]
        self.assertEqual(content1, b"content1")
        self.assertFalse(response_item1.is_missing)

        response_item2, content2 = result[1]
        self.assertEqual(content2, b"content2")
        self.assertFalse(response_item2.is_missing)

        # Verify response was closed at the end
        self.mock_response.close.assert_called_once()
