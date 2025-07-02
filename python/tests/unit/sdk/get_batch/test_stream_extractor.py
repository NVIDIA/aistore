#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO
import tarfile

from aistore.sdk.get_batch.archive_stream_extractor import ArchiveStreamExtractor
from aistore.sdk.get_batch.batch_request import BatchRequest
from aistore.sdk.get_batch.batch_response import BatchResponseItem


class TestArchiveStreamExtractor(unittest.TestCase):
    """Unit tests for ArchiveStreamExtractor class."""

    def setUp(self):
        self.extractor = ArchiveStreamExtractor()

        # Create actual BatchRequest with real objects
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
            self.extractor.extract(BytesIO(b"tar data"), self.batch_request, None)
        )

        # Verify
        self.assertEqual(len(result), 1)
        response_item, content = result[0]
        self.assertIsInstance(response_item, BatchResponseItem)
        self.assertEqual(content, b"file content")

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
            self.extractor.extract(BytesIO(b"tar data"), self.batch_request, None)
        )

        # Should only process the file, not the directory
        self.assertEqual(len(result), 1)

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
            self.extractor.extract(BytesIO(b"tar data"), self.batch_request, None)
        )[0]

        # Verify missing flag is set
        self.assertTrue(response_item.is_missing)

    @patch("tarfile.open")
    @patch("aistore.sdk.get_batch.archive_stream_extractor.logger")
    def test_continue_on_error(self, mock_logger, mock_tar_open):
        """Test extraction continues on error when continue_on_err=True."""
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

        # Execute with continue_on_err=True
        result = list(
            self.extractor.extract(
                BytesIO(b"tar data"),
                self.batch_request,  # continue_on_err=True by default
                None,
            )
        )

        # Should log error but continue
        self.assertEqual(len(result), 0)
        mock_logger.error.assert_called()

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

        # Should raise exception
        with self.assertRaises(RuntimeError):
            list(
                self.extractor.extract(
                    BytesIO(b"tar data"), batch_request_no_continue, None
                )
            )

    def test_unsupported_format_raises(self):
        """Test extract raises ValueError for unsupported formats."""
        # Create request with unsupported format
        batch_request_zip = BatchRequest(output_format=".zip")

        with self.assertRaises(ValueError) as context:
            list(self.extractor.extract(BytesIO(b"data"), batch_request_zip, None))

        self.assertIn("Unsupported output format type .zip", str(context.exception))

    def test_supported_formats(self):
        """Test format support detection."""
        # Supported formats
        self.assertTrue(self.extractor.supports_format(".tar"))
        self.assertTrue(self.extractor.supports_format(".tgz"))
        self.assertTrue(self.extractor.supports_format(".tar.gz"))

        # Unsupported formats
        self.assertFalse(self.extractor.supports_format(".zip"))
        self.assertFalse(self.extractor.supports_format(".rar"))
