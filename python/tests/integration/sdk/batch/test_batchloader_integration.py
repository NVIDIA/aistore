#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import tarfile
import unittest
from io import BytesIO

from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.integration import REMOTE_SET
from tests.integration.sdk.parallel_test_base import ParallelTestBase

from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.errors import ErrBckNotFound


class TestBatchLoaderIntegration(ParallelTestBase):
    """Integration tests for BatchLoader with cluster."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        super().setUp()
        self.test_bucket = self._create_bucket()
        self.test_bucket_2 = self._create_bucket()
        self.sample_data = self._create_sample_data()

        # Create batch loader
        self.loader = DEFAULT_TEST_CLIENT.batch_loader()

    def _create_sample_data(self):
        """Create sample test data."""
        return {
            "file1.txt": b"This is the content of file 1",
            "file2.txt": b"This is the content of file 2",
            "file3.txt": b"This is the content of file 3",
            "subdir/file4.txt": b"This is file 4 in a subdirectory",
        }

    def _create_tar_archive_data(self):
        """Create a tar archive with sample data."""
        tar_buffer = BytesIO()

        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            for filename, content in self.sample_data.items():
                tarinfo = tarfile.TarInfo(name=filename)
                tarinfo.size = len(content)
                tar.addfile(tarinfo, BytesIO(content))

        return tar_buffer.getvalue()

    def test_batch_loader_single_object(self):
        """Test BatchLoader with a single regular object."""
        # Put test data into bucket
        obj = self.test_bucket.object("test-object.txt")
        obj.get_writer().put_content(self.sample_data["file1.txt"])

        # Create batch request
        batch_req = BatchRequest(streaming=False)
        batch_req.add_object_request(obj)

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Verify results
        self.assertEqual(len(results), 1)
        resp_item, data = results[0]
        self.assertEqual(resp_item.obj_name, obj.name)
        self.assertEqual(data, self.sample_data["file1.txt"])

    def test_batch_loader_multiple_objects(self):
        """Test BatchLoader with multiple regular objects."""
        # Put multiple objects into bucket
        object_names = ["obj1.txt", "obj2.txt", "obj3.txt"]
        test_contents = [
            self.sample_data["file1.txt"],
            self.sample_data["file2.txt"],
            self.sample_data["file3.txt"],
        ]

        for obj_name, content in zip(object_names, test_contents):
            self.test_bucket.object(obj_name).get_writer().put_content(content)

        # Create batch request
        batch_req = BatchRequest(streaming=False)

        for obj_name in object_names:
            batch_req.add_object_request(self.test_bucket.object(obj_name))

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Verify results
        self.assertEqual(len(results), 3)

        for obj_name, expected_content in zip(object_names, test_contents):
            resp_item, content = results.pop(0)
            self.assertEqual(resp_item.obj_name, obj_name)
            self.assertEqual(content, expected_content)

    def test_batch_loader_multiple_buckets(self):
        """Test BatchLoader with multiple buckets."""
        # Put multiple objects into bucket
        obj1 = self.test_bucket.object("obj1.txt")
        obj1.get_writer().put_content(self.sample_data["file1.txt"])

        obj2 = self.test_bucket_2.object("obj2.txt")
        obj2.get_writer().put_content(self.sample_data["file2.txt"])

        # Create batch request
        batch_req = BatchRequest(streaming=False)

        batch_req.add_object_request(obj1)
        batch_req.add_object_request(obj2)

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Verify results
        self.assertEqual(len(results), 2)

        resp_item_1, content_1 = results.pop(0)
        resp_item_2, content_2 = results.pop(0)

        self.assertEqual(resp_item_1.obj_name, obj1.name)
        self.assertEqual(content_1, self.sample_data["file1.txt"])

        self.assertEqual(resp_item_2.obj_name, obj2.name)
        self.assertEqual(content_2, self.sample_data["file2.txt"])

    def test_batch_loader_tar_archive(self):
        """Test BatchLoader with tar archive extraction."""
        # Put tar archive into bucket
        obj = self.test_bucket.object("test-archive.tar")
        obj.get_writer().put_content(self._create_tar_archive_data())

        # Create batch request for specific files in archive
        batch_req = BatchRequest(streaming=False)
        batch_req.add_object_request(obj, archpath="file1.txt")
        batch_req.add_object_request(obj, archpath="subdir/file4.txt")

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Verify results
        self.assertEqual(len(results), 2)

        _, content_1 = results.pop(0)
        _, content_2 = results.pop(0)

        self.assertEqual(content_1, self.sample_data["file1.txt"])
        self.assertEqual(content_2, self.sample_data["subdir/file4.txt"])

    def test_batch_loader_streaming_mode(self):
        """Test BatchLoader in streaming mode."""
        # Put test objects into bucket
        obj1 = self.test_bucket.object("stream1.txt")
        obj1.get_writer().put_content(self.sample_data["file1.txt"])

        obj2 = self.test_bucket.object("stream2.txt")
        obj2.get_writer().put_content(self.sample_data["file2.txt"])

        # Create streaming batch request
        batch_req = BatchRequest(streaming=True)
        batch_req.add_object_request(obj1)
        batch_req.add_object_request(obj2)

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Verify results
        self.assertEqual(len(results), 2)

        resp_item_1, content_1 = results.pop(0)
        self.assertEqual(resp_item_1.obj_name, obj1.name)
        self.assertEqual(content_1, self.sample_data["file1.txt"])

        resp_item_2, content_2 = results.pop(0)
        self.assertEqual(resp_item_2.obj_name, obj2.name)
        self.assertEqual(content_2, self.sample_data["file2.txt"])

    def test_batch_loader_missing_data_detection(self):
        """Test BatchLoader detection of missing objects."""
        # Create batch request with non-existent object
        batch_req = BatchRequest(streaming=False, continue_on_err=True)
        batch_req.add_object_request(self.test_bucket.object("non-existent-object.txt"))

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Check if missing data is detected
        for batch_resp, _ in results:
            self.assertTrue(batch_resp.is_missing)

    def test_batch_loader_with_opaque_data(self):
        """Test BatchLoader with opaque data in requests."""
        # Put test data into bucket
        obj = self.test_bucket.object("opaque-test.txt")
        obj.get_writer().put_content(self.sample_data["file1.txt"])

        # Create batch request with opaque data
        opaque_data = b"custom metadata"
        batch_req = BatchRequest(streaming=False)
        batch_req.add_object_request(obj, opaque=opaque_data)

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Verify results (opaque data should be handled internally)
        self.assertEqual(len(results), 1)
        resp_item, data = results[0]
        self.assertEqual(resp_item.obj_name, obj.name)
        self.assertEqual(data, self.sample_data["file1.txt"])
        self.assertEqual(resp_item.opaque, opaque_data)

    def test_batch_loader_error_handling(self):
        """Test BatchLoader error handling with invalid requests."""
        # Test with None request
        with self.assertRaisesRegex(ValueError, "Batch request must not be empty"):
            list(self.loader.get_batch(None))

        # Test with empty request
        empty_req = BatchRequest()
        with self.assertRaisesRegex(ValueError, "Batch request must not be empty"):
            list(self.loader.get_batch(empty_req))

    def test_batch_loader_decoder(self):
        """Test BatchLoader with decoder."""
        # Put test data into bucket
        obj = self.test_bucket.object("decode-test.txt")
        obj.get_writer().put_content(self.sample_data["file1.txt"])

        # Create batch request
        batch_req = BatchRequest(streaming=False)
        batch_req.add_object_request(obj)

        # Load data with decoder
        results = list(self.loader.get_batch(batch_req))
        self.assertEqual(len(results), 1)
        _, data = results[0]
        self.assertIsInstance(data, bytes)

    def test_batch_loader_no_extractor(self):
        """Test BatchLoader with no extractor."""
        # Put test data into bucket
        obj = self.test_bucket.object("decode-test-false.txt")
        obj.get_writer().put_content(self.sample_data["file1.txt"])

        # Create batch request
        batch_req = BatchRequest(streaming=False)
        batch_req.add_object_request(obj)

        # Load data with no extractor
        result = self.loader.get_batch(batch_req, return_raw=True)
        # When decode_output=False, should return raw stream
        self.assertTrue(hasattr(result, "read") or isinstance(result, bytes))

    def test_batch_loader_bucket_not_exist(self):
        """Test BatchLoader when the bucket does not exist."""
        # Create object in non-existent bucket
        obj = DEFAULT_TEST_CLIENT.bucket("non-existent-bucket").object("test.txt")

        # Create batch request
        batch_req = BatchRequest(streaming=False)
        batch_req.add_object_request(obj)

        # Verify the request fails with expected error
        with self.assertRaises(ErrBckNotFound) as context:
            list(self.loader.get_batch(batch_req))

        # Check error status code and message
        self.assertIn("STATUS:404", str(context.exception))
        self.assertIn(
            'bucket \\"ais://non-existent-bucket\\" does not exist',
            str(context.exception).lower(),
        )

    @unittest.skipUnless(
        REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_batch_loader_remote_bck(self):
        """Test BatchLoader with a single regular object in a remote bucket."""
        # Put test data into bucket
        obj, content = self._create_object_with_content()

        # Create batch request
        batch_req = BatchRequest(streaming=False)
        batch_req.add_object_request(obj)

        # Load data using BatchLoader
        results = list(self.loader.get_batch(batch_req))

        # Verify results
        self.assertEqual(len(results), 1)
        resp_item, data = results[0]
        self.assertEqual(resp_item.obj_name, obj.name)
        self.assertEqual(data, content)
