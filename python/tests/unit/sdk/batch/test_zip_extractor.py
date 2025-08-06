#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO
import zipfile

from aistore.sdk.batch.extractor.zip_stream_extractor import ZipStreamExtractor
from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.batch.batch_response import BatchResponseItem


# pylint: disable=duplicate-code
class TestZipStreamExtractor(unittest.TestCase):
    """Unit tests for ZipStreamExtractor class."""

    def setUp(self):
        self.zip_extractor = ZipStreamExtractor()

        # Create BatchRequest for ZIP testing
        self.batch_request_zip = BatchRequest(
            output_format=".zip", continue_on_err=False, streaming=True
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

        # Add objects to ZIP request
        self.batch_request_zip.add_object_request(test_obj1)
        self.batch_request_zip.add_object_request(test_obj2)

        # Create mock response
        self.mock_response = Mock()

    def test_get_supported_formats(self):
        """Test that ZipStreamExtractor returns correct supported formats."""
        supported_formats = self.zip_extractor.get_supported_formats()
        expected_formats = (".zip",)
        self.assertEqual(supported_formats, expected_formats)

    @patch("zipfile.ZipFile")
    def test_successful_extraction(self, mock_zipfile):
        """Test successful file extraction from ZIP stream."""
        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create mock ZipInfo and file content
        mock_zipinfo = Mock()
        mock_zipinfo.is_dir.return_value = False
        mock_zipinfo.filename = "file1.txt"

        # Setup file list and content
        mock_zip_file.infolist.return_value = [mock_zipinfo]
        mock_zip_file.read.return_value = b"zip file content"

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response, BytesIO(b"zip data"), self.batch_request_zip, None
            )
        )

        # Verify
        self.assertEqual(len(result), 1)
        response_item, content = result[0]
        self.assertIsInstance(response_item, BatchResponseItem)
        self.assertEqual(content, b"zip file content")
        self.assertFalse(response_item.is_missing)
        mock_zip_file.read.assert_called_with("file1.txt")

    @patch("zipfile.ZipFile")
    def test_skip_directory_entries(self, mock_zipfile):
        """Test ZIP extraction skips directory entries."""
        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create directory entry
        dir_zipinfo = Mock()
        dir_zipinfo.is_dir.return_value = True
        dir_zipinfo.filename = "directory/"

        # Create file entry
        file_zipinfo = Mock()
        file_zipinfo.is_dir.return_value = False
        file_zipinfo.filename = "file.txt"

        # Setup file list
        mock_zip_file.infolist.return_value = [dir_zipinfo, file_zipinfo]
        mock_zip_file.read.return_value = b"content"

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response, BytesIO(b"zip data"), self.batch_request_zip, None
            )
        )

        # Should only process the file, not the directory
        self.assertEqual(len(result), 1)
        _, content = result[0]
        self.assertEqual(content, b"content")
        mock_zip_file.read.assert_called_once_with("file.txt")

    @patch("zipfile.ZipFile")
    def test_missing_file_detection(self, mock_zipfile):
        """Test ZIP extraction detects missing files marked with __404__/ prefix."""
        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create mock ZipInfo for missing file
        mock_zipinfo = Mock()
        mock_zipinfo.is_dir.return_value = False
        mock_zipinfo.filename = "__404__/test-bucket/missing.txt"

        # Setup file list
        mock_zip_file.infolist.return_value = [mock_zipinfo]
        mock_zip_file.read.return_value = b""

        # Execute
        response_item, _ = list(
            self.zip_extractor.extract(
                self.mock_response, BytesIO(b"zip data"), self.batch_request_zip, None
            )
        )[0]

        # Verify missing flag is set
        self.assertTrue(response_item.is_missing)

    @patch("zipfile.ZipFile")
    def test_streaming_mode_conversion(self, mock_zipfile):
        """Test ZIP extraction converts data_stream to BytesIO in streaming mode."""
        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create mock ZipInfo
        mock_zipinfo = Mock()
        mock_zipinfo.is_dir.return_value = False
        mock_zipinfo.filename = "file1.txt"

        mock_zip_file.infolist.return_value = [mock_zipinfo]
        mock_zip_file.read.return_value = b"content"

        # Execute with raw bytes (should convert to BytesIO)
        result = list(
            self.zip_extractor.extract(
                self.mock_response, b"raw zip data", self.batch_request_zip, None
            )
        )

        # Verify extraction worked
        self.assertEqual(len(result), 1)
        _, content = result[0]
        self.assertEqual(content, b"content")

        # Verify ZipFile was called with BytesIO object
        args, _ = mock_zipfile.call_args
        self.assertIsInstance(args[0], BytesIO)

    @patch("zipfile.ZipFile")
    def test_non_streaming_mode(self, mock_zipfile):
        """Test ZIP extraction in non-streaming mode uses batch_response."""
        # Create non-streaming request
        batch_request_non_streaming = BatchRequest(
            output_format=".zip", continue_on_err=True, streaming=False
        )

        # Create mock batch response
        mock_batch_response = Mock()
        mock_response_item = Mock()
        mock_response_item.bucket = "test-bucket"
        mock_response_item.obj_name = "file1.txt"
        mock_batch_response.responses = [mock_response_item]

        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create mock ZipInfo
        mock_zipinfo = Mock()
        mock_zipinfo.is_dir.return_value = False
        mock_zipinfo.filename = "file1.txt"

        mock_zip_file.infolist.return_value = [mock_zipinfo]
        mock_zip_file.read.return_value = b"content"

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response,
                BytesIO(b"zip data"),
                batch_request_non_streaming,
                mock_batch_response,
            )
        )

        # Verify it used the batch_response
        self.assertEqual(len(result), 1)
        response_item, content = result[0]
        self.assertEqual(response_item, mock_response_item)
        self.assertEqual(content, b"content")

    @patch("zipfile.ZipFile")
    def test_extraction_error_handling(self, mock_zipfile):
        """Test ZIP extraction handles zipfile errors properly."""
        # Setup mock ZipFile to raise exception
        mock_zipfile.side_effect = zipfile.BadZipFile("Corrupted ZIP file")

        # Should raise RuntimeError
        with self.assertRaises(RuntimeError) as context:
            list(
                self.zip_extractor.extract(
                    self.mock_response,
                    BytesIO(b"bad zip data"),
                    self.batch_request_zip,
                    None,
                )
            )

        self.assertIn("Failed to extract ZIP stream", str(context.exception))
        # Verify response was closed due to error
        self.mock_response.close.assert_called_once()

    @patch("zipfile.ZipFile")
    def test_multiple_files_extraction(self, mock_zipfile):
        """Test extraction of multiple files from ZIP stream."""
        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create multiple mock ZipInfo entries
        mock_zipinfo1 = Mock()
        mock_zipinfo1.is_dir.return_value = False
        mock_zipinfo1.filename = "file1.txt"

        mock_zipinfo2 = Mock()
        mock_zipinfo2.is_dir.return_value = False
        mock_zipinfo2.filename = "file2.txt"

        # Setup file list and content
        mock_zip_file.infolist.return_value = [mock_zipinfo1, mock_zipinfo2]
        mock_zip_file.read.side_effect = [b"content1", b"content2"]

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response, BytesIO(b"zip data"), self.batch_request_zip, None
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

        # Verify both files were read
        self.assertEqual(mock_zip_file.read.call_count, 2)
        mock_zip_file.read.assert_any_call("file1.txt")
        mock_zip_file.read.assert_any_call("file2.txt")

        # Verify response was closed at the end
        self.mock_response.close.assert_called_once()

    @patch("zipfile.ZipFile")
    def test_zip_read_error_handling(self, mock_zipfile):
        """Test handling of ZIP file read errors during extraction."""
        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create mock ZipInfo
        mock_zipinfo = Mock()
        mock_zipinfo.is_dir.return_value = False
        mock_zipinfo.filename = "file1.txt"

        mock_zip_file.infolist.return_value = [mock_zipinfo]
        # Simulate read error
        mock_zip_file.read.side_effect = zipfile.BadZipFile("Cannot read file")

        # Should raise RuntimeError with proper error message
        with self.assertRaises(RuntimeError) as context:
            list(
                self.zip_extractor.extract(
                    self.mock_response,
                    BytesIO(b"zip data"),
                    self.batch_request_zip,
                    None,
                )
            )

        self.assertIn("Failed to extract file", str(context.exception))
        # Verify response was closed due to error
        self.mock_response.close.assert_called_once()

    @patch("zipfile.ZipFile")
    def test_empty_zip_file(self, mock_zipfile):
        """Test handling of empty ZIP files."""
        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Empty file list
        mock_zip_file.infolist.return_value = []

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response, BytesIO(b"zip data"), self.batch_request_zip, None
            )
        )

        # Should return empty result
        self.assertEqual(len(result), 0)
        # Verify response was closed at the end
        self.mock_response.close.assert_called_once()
