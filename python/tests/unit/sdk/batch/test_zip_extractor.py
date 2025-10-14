#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO
import zipfile

from aistore.sdk.batch.extractor.zip_stream_extractor import ZipStreamExtractor
from aistore.sdk.batch.types import MossReq, MossIn, MossOut, MossResp


# pylint: disable=duplicate-code
class TestZipStreamExtractor(unittest.TestCase):
    """Unit tests for ZipStreamExtractor class."""

    def setUp(self):
        self.zip_extractor = ZipStreamExtractor()

        # Create MossReq with MossIn objects for ZIP testing
        self.moss_req_zip = MossReq(
            moss_in=[
                MossIn(obj_name="missing.txt", bck="test-bucket", provider="ais"),
                MossIn(obj_name="file1.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".zip",
            cont_on_err=False,
            streaming_get=True,
        )

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
                self.mock_response, BytesIO(b"zip data"), self.moss_req_zip, None
            )
        )

        # Verify
        self.assertEqual(len(result), 1)
        moss_out, content = result[0]
        self.assertIsInstance(moss_out, MossOut)
        self.assertEqual(content, b"zip file content")
        # In streaming mode, MossOut is constructed from MossIn
        self.assertEqual(moss_out.obj_name, "missing.txt")
        self.assertEqual(moss_out.bucket, "test-bucket")
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
                self.mock_response, BytesIO(b"zip data"), self.moss_req_zip, None
            )
        )

        # Should only process the file, not the directory
        self.assertEqual(len(result), 1)
        _, content = result[0]
        self.assertEqual(content, b"content")
        mock_zip_file.read.assert_called_once_with("file.txt")

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
                self.mock_response, b"raw zip data", self.moss_req_zip, None
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
        """Test ZIP extraction in non-streaming mode uses moss_resp."""
        # Create non-streaming request
        moss_req_non_streaming = MossReq(
            moss_in=[
                MossIn(obj_name="file1.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".zip",
            cont_on_err=True,
            streaming_get=False,
        )

        # Create MossResp with MossOut
        moss_resp = MossResp(
            out=[
                MossOut(
                    obj_name="file1.txt",
                    bucket="test-bucket",
                    provider="ais",
                    size=7,
                )
            ],
            uuid="test-uuid-456",
        )

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
                moss_req_non_streaming,
                moss_resp,
            )
        )

        # Verify it used the moss_resp
        self.assertEqual(len(result), 1)
        moss_out, content = result[0]
        self.assertEqual(moss_out.obj_name, "file1.txt")
        self.assertEqual(moss_out.bucket, "test-bucket")
        self.assertEqual(moss_out.size, 7)
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
                    self.moss_req_zip,
                    None,
                )
            )

        self.assertIn("Failed to read zip archive stream", str(context.exception))
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
                self.mock_response, BytesIO(b"zip data"), self.moss_req_zip, None
            )
        )

        # Verify both files were extracted
        self.assertEqual(len(result), 2)

        moss_out1, content1 = result[0]
        self.assertEqual(content1, b"content1")
        self.assertEqual(moss_out1.obj_name, "missing.txt")

        moss_out2, content2 = result[1]
        self.assertEqual(content2, b"content2")
        self.assertEqual(moss_out2.obj_name, "file1.txt")

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
                    self.moss_req_zip,
                    None,
                )
            )

        self.assertIn(
            "Failed to extract file1.txt from .zip archive", str(context.exception)
        )
        # Verify response was closed due to error
        self.mock_response.close.assert_called_once()

    @patch("zipfile.ZipFile")
    @patch("aistore.sdk.batch.extractor.archive_stream_extractor.logger")
    def test_continue_on_error(self, mock_logger, mock_zipfile):
        """Test extraction continues on error when cont_on_err=True."""
        # Create request with cont_on_err=True
        moss_req_cont = MossReq(
            moss_in=[
                MossIn(obj_name="file1.txt", bck="test-bucket", provider="ais"),
                MossIn(obj_name="file2.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".zip",
            cont_on_err=True,
            streaming_get=True,
        )

        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Create mock ZipInfo entries
        mock_zipinfo1 = Mock()
        mock_zipinfo1.is_dir.return_value = False
        mock_zipinfo1.filename = "file1.txt"

        mock_zipinfo2 = Mock()
        mock_zipinfo2.is_dir.return_value = False
        mock_zipinfo2.filename = "file2.txt"

        mock_zip_file.infolist.return_value = [mock_zipinfo1, mock_zipinfo2]
        # First file errors, second succeeds
        mock_zip_file.read.side_effect = [
            zipfile.BadZipFile("Cannot read file1"),
            b"content2",
        ]

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response, BytesIO(b"zip data"), moss_req_cont, None
            )
        )

        # Should log error but continue, returning only successful extraction
        self.assertEqual(len(result), 1)
        moss_out, content = result[0]
        self.assertEqual(content, b"content2")
        self.assertEqual(moss_out.obj_name, "file2.txt")

        mock_logger.error.assert_called_once()
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
                self.mock_response, BytesIO(b"zip data"), self.moss_req_zip, None
            )
        )

        # Should return empty result
        self.assertEqual(len(result), 0)
        # Verify response was closed at the end
        self.mock_response.close.assert_called_once()

    @patch("zipfile.ZipFile")
    def test_extraction_with_archpath(self, mock_zipfile):
        """Test extraction with archpath in MossIn."""
        # Create MossReq with archpath
        moss_req_with_archpath = MossReq(
            moss_in=[
                MossIn(
                    obj_name="archive.zip",
                    bck="test-bucket",
                    provider="ais",
                    archpath="data/nested/file.txt",
                ),
            ],
            output_format=".zip",
            streaming_get=True,
        )

        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        mock_zipinfo = Mock()
        mock_zipinfo.is_dir.return_value = False
        mock_zipinfo.filename = "data/nested/file.txt"

        mock_zip_file.infolist.return_value = [mock_zipinfo]
        mock_zip_file.read.return_value = b"nested content"

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response,
                BytesIO(b"zip data"),
                moss_req_with_archpath,
                None,
            )
        )

        # Verify
        self.assertEqual(len(result), 1)
        moss_out, content = result[0]
        self.assertEqual(content, b"nested content")
        self.assertEqual(moss_out.obj_name, "archive.zip")
        self.assertEqual(moss_out.archpath, "data/nested/file.txt")

    @patch("zipfile.ZipFile")
    def test_extraction_with_opaque(self, mock_zipfile):
        """Test that opaque data flows through from MossResp."""
        # Create MossReq
        moss_req = MossReq(
            moss_in=[
                MossIn(obj_name="file.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".zip",
            streaming_get=False,
        )

        # Create MossResp with opaque data
        moss_resp = MossResp(
            out=[
                MossOut(
                    obj_name="file.txt",
                    bucket="test-bucket",
                    provider="ais",
                    opaque=b"tracking-metadata",
                    size=10,
                )
            ],
            uuid="test-uuid",
        )

        # Setup mock ZipFile
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        mock_zipinfo = Mock()
        mock_zipinfo.is_dir.return_value = False
        mock_zipinfo.filename = "file.txt"

        mock_zip_file.infolist.return_value = [mock_zipinfo]
        mock_zip_file.read.return_value = b"content"

        # Execute
        result = list(
            self.zip_extractor.extract(
                self.mock_response, BytesIO(b"zip data"), moss_req, moss_resp
            )
        )

        # Verify opaque data is preserved
        moss_out, _ = result[0]
        self.assertEqual(moss_out.opaque, b"tracking-metadata")

    @patch("zipfile.ZipFile")
    def test_large_zip_file_error(self, mock_zipfile):
        """Test handling of large ZIP file errors."""
        # Setup mock to raise LargeZipFile error
        mock_zipfile.side_effect = zipfile.LargeZipFile("ZIP file is too large")

        # Should raise RuntimeError
        with self.assertRaises(RuntimeError) as context:
            list(
                self.zip_extractor.extract(
                    self.mock_response,
                    BytesIO(b"large zip data"),
                    self.moss_req_zip,
                    None,
                )
            )

        self.assertIn("Failed to read zip archive stream", str(context.exception))
        self.mock_response.close.assert_called_once()
