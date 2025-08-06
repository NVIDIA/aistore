#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#


import unittest
from unittest.mock import Mock, ANY, patch
from io import BytesIO
import tarfile
import json

from aistore.sdk.batch.multipart_decoder import MultipartDecoder
from aistore.sdk.request_client import RequestClient
from aistore.sdk.batch.batch_loader import BatchLoader
from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.batch.batch_response import BatchResponseItem
from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor


SAMPLE_JSON = {
    "in": [
        {"objname": "test.tar", "bucket": "test-bucket", "provider": "ais"},
        {
            "objname": "file1.txt",
            "bucket": "test-bucket",
            "provider": "ais",
            "archpath": "data/file1.txt",
        },
    ],
    "mime": ".tar",
    "strm": True,
    "coer": True,
    "onob": False,
}


class TestBatchLoader(unittest.TestCase):
    """
    Unit tests for BatchLoader class.


    Tests cover initialization and batch requests with different fields.
    """

    # pylint: disable=arguments-differ
    @patch("aistore.sdk.batch.batch_loader.ExtractorManager")
    def setUp(self, mock_extractor_manager_cls):
        """Set up test fixtures before each test method."""
        self.mock_request_client = Mock(spec=RequestClient)
        self.batch_loader = BatchLoader(self.mock_request_client)

        # Sample batch request using from_json
        self.sample_req = BatchRequest.from_json(json.dumps(SAMPLE_JSON))

        mock_extractor_manager = mock_extractor_manager_cls.return_value
        self.mock_extractor = Mock(spec=ArchiveStreamExtractor)
        mock_extractor_manager.get_extractor.return_value = self.mock_extractor

    def test_get_batch_empty_request(self):
        """Test get_batch with None or empty request raises ValueError."""
        with self.assertRaises(ValueError) as context:
            list(self.batch_loader.get_batch(None))
        self.assertIn("Batch request must not be empty", str(context.exception))

        empty_req = BatchRequest()
        with self.assertRaises(ValueError) as context:
            list(self.batch_loader.get_batch(empty_req))
        self.assertIn("Batch request must not be empty", str(context.exception))

    @patch("aistore.sdk.batch.batch_loader.MultipartDecoder")
    def test_get_batch_streaming(self, mock_decoder_class):
        """Test BatchLoader get_batch in streaming mode."""
        mock_response = Mock()
        mock_response.raw = BytesIO(self._create_test_tar())
        self.mock_request_client.request.return_value = mock_response

        obj_req = {
            "objname": "file1.txt",
            "bucket": "test-bucket",
            "provider": "ais",
            "size": 1,
        }

        # Configure mock instances
        self.mock_extractor.extract.return_value = iter(
            [
                (BatchResponseItem(**obj_req), b"file content 1"),
                (BatchResponseItem(**obj_req), b"file content 2"),
            ]
        )

        mock_decoder = Mock(spec=MultipartDecoder)
        mock_decoder_class.return_value = mock_decoder

        result = list(self.batch_loader.get_batch(self.sample_req))

        # Verify mock calls
        mock_decoder.decode.assert_not_called()
        self.mock_request_client.request.assert_called_once()
        self.mock_extractor.extract.assert_called_once()
        self.mock_extractor.extract.assert_called_with(ANY, ANY, self.sample_req, None)

        self.assertEqual(len(result), 2)
        result_dict = result[0][0].dict(by_alias=True)
        self._assert_req_fields(
            obj_req, result_dict, ["objname", "bucket", "provider", "size"]
        )
        self.assertEqual(result[0][1], b"file content 1")
        self.assertEqual(result[1][1], b"file content 2")

    @patch("aistore.sdk.batch.batch_loader.MultipartDecoder")
    def test_get_batch_non_streaming(self, mock_decoder_class):
        """Test BatchLoader get_batch in non-streaming mode."""
        batch_request = self.sample_req
        batch_request.streaming = False

        mock_response = Mock()
        mock_response.raw = BytesIO(self._create_test_tar())
        mock_response.headers = {"Content-Type": "multipart/mixed; boundary=12345"}
        self.mock_request_client.request.return_value = mock_response

        obj_req = {
            "objname": "file1.txt",
            "bucket": "test-bucket",
            "provider": "ais",
            "size": 1,
        }

        # Create mock decoder parts
        json_part = json.dumps(
            {
                "out": [
                    obj_req,
                    obj_req,
                ],
                "uuid": "",
            }
        ).encode()
        tar_data = self._create_test_tar()
        mock_parts = iter(
            [
                ({"Content-Type": "application/json"}, json_part),
                ({"Content-Type": "application/octet-stream"}, tar_data),
            ]
        )

        # Configure mock instances
        mock_decoder = Mock(spec=MultipartDecoder)
        mock_decoder.decode.return_value = mock_parts
        mock_decoder.encoding = "utf-8"
        mock_decoder.parse_as_stream = False
        mock_decoder_class.return_value = mock_decoder

        self.mock_extractor.extract.return_value = iter(
            [
                (BatchResponseItem(**obj_req), b""),
                (BatchResponseItem(**obj_req), b""),
            ]
        )

        # Execute get_batch
        result = list(self.batch_loader.get_batch(batch_request))

        self.mock_extractor.extract.assert_called()

        self.assertEqual(len(result), 2)
        result_dict = result[0][0].dict(by_alias=True)
        self._assert_req_fields(
            obj_req, result_dict, ["objname", "bucket", "provider", "size"]
        )

    def test_get_batch_no_extractor(self):
        """Test BatchLoader with no extractor."""
        mock_response = Mock()
        mock_response.raw = BytesIO(b"raw tar data")
        self.mock_request_client.request.return_value = mock_response

        result = self.batch_loader.get_batch(self.sample_req, return_raw=True)

        # Should return raw stream, not decoded content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, BytesIO)

    def test_get_batch_extractor(self):
        """Test BatchLoader with extractor."""
        mock_response = Mock()
        mock_response.raw = BytesIO(b"raw tar data")
        self.mock_request_client.request.return_value = mock_response

        self.mock_extractor.extract.return_value = iter(
            [
                (BatchResponseItem.from_batch_request(self.sample_req, 0), b""),
                (BatchResponseItem.from_batch_request(self.sample_req, 1), b""),
            ]
        )

        result = list(self.batch_loader.get_batch(self.sample_req))

        # Should return decoded content from extractor
        self.assertIsNotNone(result)
        self.assertEqual(
            result[0], (BatchResponseItem.from_batch_request(self.sample_req, 0), b"")
        )
        self.assertEqual(
            result[1], (BatchResponseItem.from_batch_request(self.sample_req, 1), b"")
        )

    def _assert_req_fields(self, req, res, fields):
        for field in fields:
            self.assertEqual(req[field], res[field])

    @staticmethod
    def _create_test_tar() -> bytes:
        """Helper method to create a test tar archive."""
        tar_buffer = BytesIO()

        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            # Add test file 1
            file1_data = b"This is test file 1 content"
            file1_info = tarfile.TarInfo(name="file1.txt")
            file1_info.size = len(file1_data)
            tar.addfile(file1_info, BytesIO(file1_data))

            # Add test file 2
            file2_data = b"This is test file 2 content"
            file2_info = tarfile.TarInfo(name="file2.txt")
            file2_info.size = len(file2_data)
            tar.addfile(file2_info, BytesIO(file2_data))

        tar_buffer.seek(0)
        return tar_buffer.read()
