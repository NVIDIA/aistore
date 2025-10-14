#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch
from io import BytesIO
import tarfile

from aistore.sdk.batch.multipart.multipart_decoder import MultipartDecoder
from aistore.sdk.request_client import RequestClient
from aistore.sdk.batch.batch import Batch
from aistore.sdk.batch.types import MossOut, MossResp
from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor
from aistore.sdk.bucket import Bucket
from aistore.sdk.obj.object import Object


# pylint: disable=unsubscriptable-object,too-many-public-methods
class TestBatch(unittest.TestCase):
    """
    Unit tests for Batch class.

    Tests cover initialization, add() method, and get() execution with different modes.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_request_client = Mock(spec=RequestClient)

        # Setup mock bucket
        self.mock_bucket = Mock(spec=Bucket)
        self.mock_bucket.name = "test-bucket"
        self.mock_bucket.provider.value = "ais"

    def test_batch_init_empty(self):
        """Test Batch initialization with no objects."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)

        self.assertEqual(len(batch), 0)
        self.assertIsNotNone(batch.request)
        self.assertEqual(batch.bucket, self.mock_bucket)

    def test_batch_init_with_string(self):
        """Test Batch initialization with a single string object name."""
        batch = Batch(
            self.mock_request_client, objects="file.txt", bucket=self.mock_bucket
        )

        self.assertEqual(len(batch), 1)
        self.assertEqual(batch.request.moss_in[0].obj_name, "file.txt")

    def test_batch_init_with_string_list(self):
        """Test Batch initialization with list of string object names."""
        batch = Batch(
            self.mock_request_client,
            objects=["file1.txt", "file2.txt", "file3.txt"],
            bucket=self.mock_bucket,
        )

        self.assertEqual(len(batch), 3)
        self.assertEqual(batch.request.moss_in[0].obj_name, "file1.txt")
        self.assertEqual(batch.request.moss_in[1].obj_name, "file2.txt")
        self.assertEqual(batch.request.moss_in[2].obj_name, "file3.txt")

    def test_batch_init_with_object(self):
        """Test Batch initialization with Object instance."""
        mock_obj = Mock(spec=Object)
        mock_obj.name = "test.txt"
        mock_obj.bucket_name = "test-bucket"
        mock_obj.bucket_provider.value = "ais"

        batch = Batch(
            self.mock_request_client, objects=mock_obj, bucket=self.mock_bucket
        )

        self.assertEqual(len(batch), 1)
        self.assertEqual(batch.request.moss_in[0].obj_name, "test.txt")
        self.assertEqual(batch.request.moss_in[0].bck, "test-bucket")
        self.assertEqual(batch.request.moss_in[0].provider, "ais")

    def test_batch_init_with_object_list(self):
        """Test Batch initialization with list of Object instances."""
        mock_obj1 = Mock(spec=Object)
        mock_obj1.name = "test1.txt"
        mock_obj1.bucket_name = "bucket1"
        mock_obj1.bucket_provider.value = "ais"

        mock_obj2 = Mock(spec=Object)
        mock_obj2.name = "test2.txt"
        mock_obj2.bucket_name = "bucket2"
        mock_obj2.bucket_provider.value = "s3"

        batch = Batch(
            self.mock_request_client,
            objects=[mock_obj1, mock_obj2],
            bucket=self.mock_bucket,
        )

        self.assertEqual(len(batch), 2)
        self.assertEqual(batch.request.moss_in[0].obj_name, "test1.txt")
        self.assertEqual(batch.request.moss_in[1].obj_name, "test2.txt")

    def test_batch_add_string(self):
        """Test adding object by string name."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)
        result = batch.add("file.txt")

        # Should return self for chaining
        self.assertIs(result, batch)
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch.request.moss_in[0].obj_name, "file.txt")

    def test_batch_add_with_archpath(self):
        """Test adding object with archpath for archive extraction."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)
        batch.add("shard.tar", archpath="images/photo.jpg")

        self.assertEqual(len(batch), 1)
        self.assertEqual(batch.request.moss_in[0].obj_name, "shard.tar")
        self.assertEqual(batch.request.moss_in[0].archpath, "images/photo.jpg")

    @unittest.skip("Not Implemented")
    def test_batch_add_with_byte_range(self):
        """Test adding object with byte range."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)
        batch.add("large.bin", start=1024, length=2048)

        self.assertEqual(len(batch), 1)
        moss_in = batch.request.moss_in[0]
        self.assertEqual(moss_in.obj_name, "large.bin")
        self.assertEqual(moss_in.start, 1024)
        self.assertEqual(moss_in.length, 2048)

    def test_batch_add_with_opaque(self):
        """Test adding object with opaque user data."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)
        batch.add("tracked.txt", opaque=b"user-id-123")

        self.assertEqual(len(batch), 1)
        moss_in = batch.request.moss_in[0]
        self.assertEqual(moss_in.obj_name, "tracked.txt")
        # Opaque should be base64 encoded
        self.assertIsNotNone(moss_in.opaque)

    def test_batch_add_chaining(self):
        """Test method chaining with add()."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)
        batch.add("file1.txt").add("file2.txt").add("file3.txt")

        self.assertEqual(len(batch), 3)

    def test_batch_init_with_options(self):
        """Test Batch initialization with various options."""
        batch = Batch(
            self.mock_request_client,
            bucket=self.mock_bucket,
            output_format=".tar.gz",
            cont_on_err=True,
            only_obj_name=True,
            streaming_get=True,
        )

        self.assertEqual(batch.request.output_format, ".tar.gz")
        self.assertTrue(batch.request.cont_on_err)
        self.assertTrue(batch.request.only_obj_name)
        self.assertTrue(batch.request.streaming_get)

    def test_get_batch_empty_request(self):
        """Test get() with empty batch raises ValueError."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)

        with self.assertRaises(ValueError) as context:
            list(batch.get())
        self.assertIn("No objects added to batch", str(context.exception))

    @patch("aistore.sdk.batch.batch.ExtractorManager")
    def test_get_batch_streaming(self, mock_extractor_manager_cls):
        """Test Batch get() in streaming mode."""
        # Setup extractor manager mock before creating batch
        mock_extractor_manager = mock_extractor_manager_cls.return_value
        mock_extractor = Mock(spec=ArchiveStreamExtractor)
        mock_extractor_manager.get_extractor.return_value = mock_extractor

        batch = Batch(
            self.mock_request_client,
            objects=["file1.txt", "file2.txt"],
            bucket=self.mock_bucket,
            streaming_get=True,
        )

        mock_response = Mock()
        mock_response.raw = BytesIO(self._create_test_tar())
        self.mock_request_client.request.return_value = mock_response

        # Configure mock extractor
        mock_extractor.extract.return_value = iter(
            [
                (
                    MossOut(obj_name="file1.txt", bucket="test-bucket", provider="ais"),
                    b"content1",
                ),
                (
                    MossOut(obj_name="file2.txt", bucket="test-bucket", provider="ais"),
                    b"content2",
                ),
            ]
        )

        result = list(batch.get())

        # Verify
        self.mock_request_client.request.assert_called_once()
        mock_extractor.extract.assert_called_once()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0].obj_name, "file1.txt")
        self.assertEqual(result[0][1], b"content1")
        self.assertEqual(result[1][0].obj_name, "file2.txt")
        self.assertEqual(result[1][1], b"content2")

    @patch("aistore.sdk.batch.batch.ExtractorManager")
    @patch("aistore.sdk.batch.batch.MultipartDecoder")
    def test_get_batch_non_streaming(
        self, mock_decoder_class, mock_extractor_manager_cls
    ):
        """Test Batch get() in non-streaming mode."""
        # Setup extractor manager mock before creating batch
        mock_extractor_manager = mock_extractor_manager_cls.return_value
        mock_extractor = Mock(spec=ArchiveStreamExtractor)
        mock_extractor_manager.get_extractor.return_value = mock_extractor

        batch = Batch(
            self.mock_request_client,
            objects=["file1.txt", "file2.txt"],
            bucket=self.mock_bucket,
            streaming_get=False,
        )

        mock_response = Mock()
        mock_response.raw = BytesIO(self._create_test_tar())
        mock_response.headers = {"Content-Type": "multipart/mixed; boundary=12345"}
        self.mock_request_client.request.return_value = mock_response

        # Create mock MossResp
        moss_resp = MossResp(
            out=[
                MossOut(
                    obj_name="file1.txt", bucket="test-bucket", provider="ais", size=10
                ),
                MossOut(
                    obj_name="file2.txt", bucket="test-bucket", provider="ais", size=10
                ),
            ],
            uuid="test-uuid",
        )

        # Create mock decoder parts
        json_part = moss_resp.json(by_alias=True).encode()
        tar_data = self._create_test_tar()
        mock_parts = iter(
            [
                ({"Content-Type": "application/json"}, json_part),
                ({"Content-Type": "application/octet-stream"}, tar_data),
            ]
        )

        # Configure mock decoder
        mock_decoder = Mock(spec=MultipartDecoder)
        mock_decoder.decode.return_value = mock_parts
        mock_decoder.encoding = "utf-8"
        mock_decoder.parse_as_stream = False
        mock_decoder_class.return_value = mock_decoder

        # Configure mock extractor
        mock_extractor.extract.return_value = iter(
            [
                (moss_resp.out[0], b"content1"),
                (moss_resp.out[1], b"content2"),
            ]
        )

        result = list(batch.get())

        # Verify
        mock_extractor.extract.assert_called()
        self.assertEqual(len(result), 2)

    def test_get_batch_raw(self):
        """Test Batch get() with raw=True returns raw stream."""
        batch = Batch(
            self.mock_request_client, objects=["file.txt"], bucket=self.mock_bucket
        )

        mock_response = Mock()
        mock_response.raw = BytesIO(b"raw tar data")
        self.mock_request_client.request.return_value = mock_response

        result = batch.get(raw=True)

        # Should return raw stream
        self.assertIsInstance(result, BytesIO)
        self.assertEqual(result.read(), b"raw tar data")

    def test_batch_repr(self):
        """Test Batch string representation."""
        batch = Batch(
            self.mock_request_client,
            objects=["file1.txt", "file2.txt"],
            bucket=self.mock_bucket,
            output_format=".tar.gz",
        )

        repr_str = repr(batch)
        self.assertIn("Batch", repr_str)
        self.assertIn("objects=2", repr_str)
        self.assertIn("format=.tar.gz", repr_str)

    def test_batch_init_with_unsupported_object_type(self):
        """Test Batch initialization with unsupported object type raises ValueError."""
        with self.assertRaises(ValueError) as context:
            Batch(
                self.mock_request_client,
                objects=[123, 456],  # Invalid type
                bucket=self.mock_bucket,
            )
        self.assertIn("Unsupported object type", str(context.exception))

    @patch("aistore.sdk.batch.batch.ExtractorManager")
    def test_moss_out_with_error_message(self, mock_extractor_manager_cls):
        """Test handling of MossOut with error message."""
        # Setup extractor manager mock before creating batch
        mock_extractor_manager = mock_extractor_manager_cls.return_value
        mock_extractor = Mock(spec=ArchiveStreamExtractor)
        mock_extractor_manager.get_extractor.return_value = mock_extractor

        batch = Batch(
            self.mock_request_client,
            objects=["missing.txt"],
            bucket=self.mock_bucket,
            streaming_get=False,
        )

        mock_response = Mock()
        mock_response.raw = BytesIO(b"empty tar")
        mock_response.headers = {"Content-Type": "multipart/mixed; boundary=12345"}
        self.mock_request_client.request.return_value = mock_response

        # Create MossResp with error
        moss_resp = MossResp(
            out=[
                MossOut(
                    obj_name="missing.txt",
                    bucket="test-bucket",
                    provider="ais",
                    err_msg="object not found",
                    size=0,
                )
            ],
            uuid="test-uuid",
        )

        json_part = moss_resp.json(by_alias=True).encode()
        tar_data = b"empty tar"
        mock_parts = iter(
            [
                ({"Content-Type": "application/json"}, json_part),
                ({"Content-Type": "application/octet-stream"}, tar_data),
            ]
        )

        mock_decoder = Mock(spec=MultipartDecoder)
        mock_decoder.decode.return_value = mock_parts
        mock_decoder.encoding = "utf-8"

        with patch(
            "aistore.sdk.batch.batch.MultipartDecoder", return_value=mock_decoder
        ):
            mock_extractor.extract.return_value = iter(
                [
                    (moss_resp.out[0], b""),
                ]
            )

            result = list(batch.get())

            # Verify error is preserved
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0][0].err_msg, "object not found")

    @unittest.skip("Not Implemented")
    def test_batch_with_mixed_parameters(self):
        """Test Batch with objects having different parameters."""
        batch = Batch(
            self.mock_request_client, objects=["simple.txt"], bucket=self.mock_bucket
        )

        # Add object with archpath
        batch.add("archive.tar", archpath="data/file.json")

        # Add object with byte range
        batch.add("large.bin", start=1024, length=2048)

        # Add object with opaque
        batch.add("tracked.txt", opaque=b"user-123")

        # Add object with all parameters
        batch.add(
            "complex.tar",
            archpath="nested/data.txt",
            start=0,
            length=512,
            opaque=b"meta",
        )

        self.assertEqual(len(batch), 5)

        # Verify each object has correct parameters
        self.assertEqual(batch.request.moss_in[0].obj_name, "simple.txt")
        self.assertIsNone(batch.request.moss_in[0].archpath)

        self.assertEqual(batch.request.moss_in[1].obj_name, "archive.tar")
        self.assertEqual(batch.request.moss_in[1].archpath, "data/file.json")

        self.assertEqual(batch.request.moss_in[2].obj_name, "large.bin")
        self.assertEqual(batch.request.moss_in[2].start, 1024)
        self.assertEqual(batch.request.moss_in[2].length, 2048)

        self.assertEqual(batch.request.moss_in[3].obj_name, "tracked.txt")
        self.assertIsNotNone(batch.request.moss_in[3].opaque)

        self.assertEqual(batch.request.moss_in[4].obj_name, "complex.tar")
        self.assertEqual(batch.request.moss_in[4].archpath, "nested/data.txt")
        self.assertEqual(batch.request.moss_in[4].start, 0)
        self.assertEqual(batch.request.moss_in[4].length, 512)

    def test_batch_with_different_output_formats(self):
        """Test Batch with different archive output formats."""
        formats = [".tar", ".tar.gz", ".tgz", ".zip"]

        for fmt in formats:
            batch = Batch(
                self.mock_request_client,
                objects=["file.txt"],
                bucket=self.mock_bucket,
                output_format=fmt,
            )
            self.assertEqual(batch.request.output_format, fmt)

    def test_moss_in_dict_serialization(self):
        """Test MossIn serialization excludes defaults and uses aliases."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)
        batch.add("file.txt")

        moss_in_dict = batch.request.moss_in[0].dict()

        # Should have obj_name with alias
        self.assertIn("objname", moss_in_dict)
        self.assertEqual(moss_in_dict["objname"], "file.txt")

        # Should exclude defaults (None values)
        self.assertNotIn("bck", moss_in_dict)
        self.assertNotIn("archpath", moss_in_dict)

    def test_moss_req_dict_serialization(self):
        """Test MossReq serialization."""
        batch = Batch(
            self.mock_request_client,
            objects=["file1.txt", "file2.txt"],
            bucket=self.mock_bucket,
            output_format=".tar.gz",
            cont_on_err=True,
        )

        req_dict = batch.request.dict()

        # Should have aliases
        self.assertIn("in", req_dict)
        self.assertIn("mime", req_dict)
        self.assertIn("coer", req_dict)

        # Should have correct values
        self.assertEqual(len(req_dict["in"]), 2)
        self.assertEqual(req_dict["mime"], ".tar.gz")
        self.assertTrue(req_dict["coer"])

    def test_batch_len(self):
        """Test __len__ method of Batch."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)

        self.assertEqual(len(batch), 0)

        batch.add("file1.txt")
        self.assertEqual(len(batch), 1)

        batch.add("file2.txt")
        batch.add("file3.txt")
        self.assertEqual(len(batch), 3)

    @patch("aistore.sdk.batch.batch.ExtractorManager")
    @patch("aistore.sdk.batch.batch.MultipartDecoder")
    def test_batch_decode_as_stream(
        self, mock_decoder_class, mock_extractor_manager_cls
    ):
        """Test Batch get() with decode_as_stream=True."""
        # Setup extractor manager mock before creating batch
        mock_extractor_manager = mock_extractor_manager_cls.return_value
        mock_extractor = Mock(spec=ArchiveStreamExtractor)
        mock_extractor_manager.get_extractor.return_value = mock_extractor

        batch = Batch(
            self.mock_request_client,
            objects=["file.txt"],
            bucket=self.mock_bucket,
            streaming_get=False,
        )

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "multipart/mixed; boundary=12345"}
        self.mock_request_client.request.return_value = mock_response

        # Create mock streaming parts
        moss_resp = MossResp(
            out=[
                MossOut(
                    obj_name="file.txt", bucket="test-bucket", provider="ais", size=10
                )
            ],
            uuid="test-uuid",
        )

        json_stream = Mock()
        json_stream.read.return_value = moss_resp.json(by_alias=True).encode()

        tar_stream = BytesIO(b"empty tar content")

        mock_parts = iter(
            [
                ({"Content-Type": "application/json"}, json_stream),
                ({"Content-Type": "application/octet-stream"}, tar_stream),
            ]
        )

        mock_decoder = Mock(spec=MultipartDecoder)
        mock_decoder.decode.return_value = mock_parts
        mock_decoder.encoding = "utf-8"
        mock_decoder.parse_as_stream = True
        mock_decoder_class.return_value = mock_decoder

        mock_extractor.extract.return_value = iter(
            [
                (moss_resp.out[0], b"content"),
            ]
        )

        result = list(batch.get(decode_as_stream=True))

        # Verify decode_as_stream was passed
        self.assertEqual(len(result), 1)
        mock_extractor.extract.assert_called()

    def test_batch_with_object_from_different_buckets(self):
        """Test Batch with Object instances from different buckets."""
        mock_obj1 = Mock(spec=Object)
        mock_obj1.name = "file1.txt"
        mock_obj1.bucket_name = "bucket-a"
        mock_obj1.bucket_provider.value = "ais"

        mock_obj2 = Mock(spec=Object)
        mock_obj2.name = "file2.txt"
        mock_obj2.bucket_name = "bucket-b"
        mock_obj2.bucket_provider.value = "s3"

        # Default bucket is set but objects override it
        batch = Batch(
            self.mock_request_client,
            objects=[mock_obj1, mock_obj2],
            bucket=self.mock_bucket,
        )

        self.assertEqual(len(batch), 2)
        self.assertEqual(batch.request.moss_in[0].bck, "bucket-a")
        self.assertEqual(batch.request.moss_in[0].provider, "ais")
        self.assertEqual(batch.request.moss_in[1].bck, "bucket-b")
        self.assertEqual(batch.request.moss_in[1].provider, "s3")

    def test_moss_out_opaque_encoding_decoding(self):
        """Test opaque data is properly base64 encoded/decoded."""
        batch = Batch(self.mock_request_client, bucket=self.mock_bucket)

        opaque_data = b"user-tracking-id-12345"
        batch.add("file.txt", opaque=opaque_data)

        # Opaque should be base64 encoded in MossIn
        moss_in = batch.request.moss_in[0]
        self.assertIsNotNone(moss_in.opaque)
        self.assertIsInstance(moss_in.opaque, str)

        # Simulate MossOut receiving base64 encoded opaque
        moss_out = MossOut(
            obj_name="file.txt",
            bucket="test-bucket",
            provider="ais",
            opaque=moss_in.opaque,  # Pass base64 string
        )

        # Should be decoded back to bytes
        self.assertEqual(moss_out.opaque, opaque_data)

    def test_batch_options_only_obj_name(self):
        """Test only_obj_name option."""
        batch = Batch(
            self.mock_request_client,
            objects=["file.txt"],
            bucket=self.mock_bucket,
            only_obj_name=True,
        )

        self.assertTrue(batch.request.only_obj_name)

    def test_batch_options_cont_on_err(self):
        """Test cont_on_err option."""
        # Default should be False
        batch1 = Batch(
            self.mock_request_client, objects=["file.txt"], bucket=self.mock_bucket
        )
        self.assertFalse(batch1.request.cont_on_err)

        # Explicit True
        batch2 = Batch(
            self.mock_request_client,
            objects=["file.txt"],
            bucket=self.mock_bucket,
            cont_on_err=True,
        )
        self.assertTrue(batch2.request.cont_on_err)

    def test_batch_without_bucket(self):
        """Test Batch can be created without default bucket."""
        batch = Batch(self.mock_request_client)
        self.assertIsNone(batch.bucket)

        # Can still add objects (they need their own bucket info)
        mock_obj = Mock(spec=Object)
        mock_obj.name = "file.txt"
        mock_obj.bucket_name = "specific-bucket"
        mock_obj.bucket_provider.value = "ais"

        batch.add(mock_obj)
        self.assertEqual(len(batch), 1)

    def test_batch_init_with_string_no_bucket_raises_error(self):
        """Test Batch initialization with string object but no bucket raises ValueError."""
        with self.assertRaises(ValueError) as context:
            Batch(self.mock_request_client, objects="file.txt")

        self.assertIn("Bucket must be provided", str(context.exception))
        self.assertIn("raw names", str(context.exception))

    def test_batch_init_with_string_list_no_bucket_raises_error(self):
        """Test Batch initialization with list of strings but no bucket raises ValueError."""
        with self.assertRaises(ValueError) as context:
            Batch(self.mock_request_client, objects=["file1.txt", "file2.txt"])

        self.assertIn("Bucket must be provided", str(context.exception))
        self.assertIn("raw names", str(context.exception))

    def test_batch_add_string_no_bucket_raises_error(self):
        """Test Batch.add() with string object but no bucket raises ValueError."""
        batch = Batch(self.mock_request_client)

        with self.assertRaises(ValueError) as context:
            batch.add("file.txt")

        self.assertIn("Bucket must be provided", str(context.exception))
        self.assertIn("raw names", str(context.exception))

    def test_batch_add_string_with_archpath_no_bucket_raises_error(self):
        """Test Batch.add() with string and archpath but no bucket raises ValueError."""
        batch = Batch(self.mock_request_client)

        with self.assertRaises(ValueError) as context:
            batch.add("archive.tar", archpath="file.txt")

        self.assertIn("Bucket must be provided", str(context.exception))
        self.assertIn("raw names", str(context.exception))

    def test_batch_add_string_with_opaque_no_bucket_raises_error(self):
        """Test Batch.add() with string and opaque but no bucket raises ValueError."""
        batch = Batch(self.mock_request_client)

        with self.assertRaises(ValueError) as context:
            batch.add("file.txt", opaque=b"tracking-data")

        self.assertIn("Bucket must be provided", str(context.exception))
        self.assertIn("raw names", str(context.exception))

    def test_batch_mixed_list_partial_strings_no_bucket_raises_error(self):
        """Test Batch with mixed list (Object + string) fails on string when no bucket."""
        mock_obj = Mock(spec=Object)
        mock_obj.name = "file1.txt"
        mock_obj.bucket_name = "bucket1"
        mock_obj.bucket_provider.value = "ais"

        # Mixed list: first Object (ok), then string (should fail)
        with self.assertRaises(ValueError) as context:
            Batch(self.mock_request_client, objects=[mock_obj, "file2.txt"])

        self.assertIn("Bucket must be provided", str(context.exception))
        self.assertIn("raw names", str(context.exception))

    def test_moss_resp_uuid_tracking(self):
        """Test that MossResp UUID is properly handled."""
        moss_resp = MossResp(
            out=[
                MossOut(
                    obj_name="file.txt", bucket="test-bucket", provider="ais", size=10
                )
            ],
            uuid="request-uuid-12345",
        )

        self.assertEqual(moss_resp.uuid, "request-uuid-12345")

        # Serialize and verify
        resp_dict = moss_resp.dict(by_alias=True)
        self.assertIn("uuid", resp_dict)
        self.assertEqual(resp_dict["uuid"], "request-uuid-12345")

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
