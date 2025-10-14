#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch, MagicMock, ANY
from io import BytesIO
import tarfile

from aistore.sdk.batch.extractor.tar_stream_extractor import TarStreamExtractor
from aistore.sdk.batch.types import MossReq, MossIn, MossOut, MossResp


# pylint: disable=duplicate-code
class TestTarStreamExtractor(unittest.TestCase):
    """Unit tests for TarStreamExtractor class."""

    def setUp(self):
        self.tar_extractor = TarStreamExtractor()

        # Create MossReq with MossIn objects
        self.moss_req = MossReq(
            moss_in=[
                MossIn(obj_name="missing.txt", bck="test-bucket", provider="ais"),
                MossIn(obj_name="file1.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".tar",
            cont_on_err=True,
            streaming_get=True,
        )

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
                self.mock_response, BytesIO(b"tar data"), self.moss_req, None
            )
        )

        # Verify
        self.assertEqual(len(result), 1)
        moss_out, content = result[0]
        self.assertIsInstance(moss_out, MossOut)
        self.assertEqual(content, b"file content")
        # In streaming mode, MossOut is constructed from MossIn
        self.assertEqual(moss_out.obj_name, "missing.txt")
        self.assertEqual(moss_out.bucket, "test-bucket")

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
                self.mock_response, BytesIO(b"tar data"), self.moss_req, None
            )
        )

        # Should only process the file, not the directory
        self.assertEqual(len(result), 1)
        _, content = result[0]
        self.assertEqual(content, b"content")

    @patch("tarfile.open")
    def test_non_streaming_mode(self, mock_tar_open):
        """Test extraction in non-streaming mode uses moss_resp."""
        # Create non-streaming request
        moss_req_non_streaming = MossReq(
            moss_in=[
                MossIn(obj_name="file1.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".tar",
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
            uuid="test-uuid-123",
        )

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

    @patch("tarfile.open")
    @patch("aistore.sdk.batch.extractor.archive_stream_extractor.logger")
    def test_continue_on_error(self, mock_logger, mock_tar_open):
        """Test extraction continues on error when cont_on_err=True."""
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

        # Execute with cont_on_err=True
        result = list(
            self.tar_extractor.extract(
                self.mock_response,
                BytesIO(b"tar data"),
                self.moss_req,  # cont_on_err=True by default
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
        """Test extraction raises exception when cont_on_err=False."""
        # Create MossReq with cont_on_err=False
        moss_req_no_continue = MossReq(
            moss_in=[
                MossIn(obj_name="file1.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".tar",
            cont_on_err=False,
            streaming_get=True,
        )

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
                    moss_req_no_continue,
                    None,
                )
            )

        # Verify the error message
        self.assertIn(
            "Failed to extract file1.txt from .tar archive", str(context.exception)
        )
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
                    self.moss_req,
                    None,
                )
            )

        self.assertIn("Failed to read tar archive stream", str(context.exception))
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
                self.mock_response, BytesIO(b"tar data"), self.moss_req, None
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

        # Verify response was closed at the end
        self.mock_response.close.assert_called_once()

    @patch("tarfile.open")
    def test_extraction_with_archpath(self, mock_tar_open):
        """Test extraction with archpath in MossIn."""
        # Create MossReq with archpath
        moss_req_with_archpath = MossReq(
            moss_in=[
                MossIn(
                    obj_name="shard.tar",
                    bck="test-bucket",
                    provider="ais",
                    archpath="data/file.txt",
                ),
            ],
            output_format=".tar",
            streaming_get=True,
        )

        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        mock_tarinfo = Mock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "data/file.txt"

        mock_file = Mock()
        mock_file.read.return_value = b"archived content"

        mock_tar_file.__iter__.return_value = [mock_tarinfo]
        mock_tar_file.extractfile.return_value.__enter__.return_value = mock_file

        # Execute
        result = list(
            self.tar_extractor.extract(
                self.mock_response,
                BytesIO(b"tar data"),
                moss_req_with_archpath,
                None,
            )
        )

        # Verify
        self.assertEqual(len(result), 1)
        moss_out, content = result[0]
        self.assertEqual(content, b"archived content")
        self.assertEqual(moss_out.obj_name, "shard.tar")
        self.assertEqual(moss_out.archpath, "data/file.txt")

    @patch("tarfile.open")
    def test_extraction_with_opaque(self, mock_tar_open):
        """Test that opaque data flows through from MossResp."""
        # Create MossReq
        moss_req = MossReq(
            moss_in=[
                MossIn(obj_name="file.txt", bck="test-bucket", provider="ais"),
            ],
            output_format=".tar",
            streaming_get=False,
        )

        # Create MossResp with opaque data
        moss_resp = MossResp(
            out=[
                MossOut(
                    obj_name="file.txt",
                    bucket="test-bucket",
                    provider="ais",
                    opaque=b"user-tracking-id",
                    size=10,
                )
            ],
            uuid="test-uuid",
        )

        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        mock_tarinfo = Mock()
        mock_tarinfo.isfile.return_value = True
        mock_tarinfo.name = "file.txt"

        mock_file = Mock()
        mock_file.read.return_value = b"content"

        mock_tar_file.__iter__.return_value = [mock_tarinfo]
        mock_tar_file.extractfile.return_value.__enter__.return_value = mock_file

        # Execute
        result = list(
            self.tar_extractor.extract(
                self.mock_response, BytesIO(b"tar data"), moss_req, moss_resp
            )
        )

        # Verify opaque data is preserved
        moss_out, _ = result[0]
        self.assertEqual(moss_out.opaque, b"user-tracking-id")

    @patch("tarfile.open")
    def test_empty_tar_file(self, mock_tar_open):
        """Test handling of empty tar files."""
        # Setup mock tarfile
        mock_tar_file = MagicMock()
        mock_tar_open.return_value.__enter__.return_value = mock_tar_file

        # Empty file list
        mock_tar_file.__iter__.return_value = []

        # Execute
        result = list(
            self.tar_extractor.extract(
                self.mock_response, BytesIO(b"tar data"), self.moss_req, None
            )
        )

        # Should return empty result
        self.assertEqual(len(result), 0)
        # Verify response was closed at the end
        self.mock_response.close.assert_called_once()
