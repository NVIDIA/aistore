#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import tarfile
import unittest
from io import BytesIO

from aistore.sdk.errors import AISError

from tests.integration import REMOTE_SET
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.utils import case_matrix


class TestBatch(ParallelTestBase):
    """Integration tests for Batch API with various modes and configurations."""

    @case_matrix(
        [("multipart", False), ("multipart", True), ("stream", False), ("stream", True)]
    )
    def test_batch_single_object(self, case):
        """
        Test batch retrieval of a single object in different modes.

        Tests both multipart and streaming modes, with and without bucket in URL.
        """
        mode, bucket_in_url = case

        # Create test object with content
        obj, expected_content = self._create_object_with_content()

        # Configure batch mode
        stream_get = mode == "stream"
        bucket = self.bucket if bucket_in_url else None

        # Create and execute batch request
        batch = self.client.batch(
            objects=[obj], bucket=bucket, streaming_get=stream_get
        )
        results = list(batch.get())

        # Verify results
        self.assertEqual(len(results), 1, f"Expected 1 result, got {len(results)}")

        batch_response, actual_content = results[0]
        self.assertEqual(
            batch_response.obj_name,
            obj.name,
            f"Object name mismatch: expected {obj.name}, got {batch_response.obj_name}",
        )
        self.assertEqual(
            actual_content,
            expected_content,
            f"Content mismatch: expected {len(expected_content)} bytes, got {len(actual_content)} bytes",
        )

        # Verify response metadata
        self.assertIsNotNone(batch_response.bucket)
        self.assertIsNotNone(batch_response.provider)
        self.assertGreater(batch_response.size, 0, "Size should be greater than 0")

        # In non-streaming mode, we should have error info available
        if not stream_get:
            # No error expected for successful retrieval
            self.assertIsNone(
                batch_response.err_msg,
                f"Unexpected error in response: {batch_response.err_msg}",
            )

    # pylint: disable=too-many-locals
    @case_matrix(
        [("multipart", False), ("multipart", True), ("stream", False), ("stream", True)]
    )
    def test_batch_multiple_objects(self, case):
        """
        Test batch retrieval of multiple objects in different modes.

        Verifies that all objects are retrieved correctly and in order.
        """
        mode, bucket_in_url = case

        # Create multiple test objects
        obj1, content1 = self._create_object_with_content()
        obj2, content2 = self._create_object_with_content()
        obj3, content3 = self._create_object_with_content()

        expected_objects = [
            (obj1, content1),
            (obj2, content2),
            (obj3, content3),
        ]

        # Configure batch mode
        stream_get = mode == "stream"
        bucket = self.bucket if bucket_in_url else None

        # Create and execute batch request
        batch = self.client.batch(
            objects=[obj1, obj2, obj3], bucket=bucket, streaming_get=stream_get
        )
        results = list(batch.get())

        # Verify number of results
        self.assertEqual(len(results), 3, f"Expected 3 results, got {len(results)}")

        # Verify each result
        for i, (batch_response, actual_content) in enumerate(results):
            expected_obj, expected_content = expected_objects[i]

            self.assertEqual(
                batch_response.obj_name, expected_obj.name, f"Object {i}: name mismatch"
            )
            self.assertEqual(
                actual_content, expected_content, f"Object {i}: content mismatch"
            )
            self.assertGreater(
                batch_response.size, 0, f"Object {i}: size should be > 0"
            )

    def test_batch_with_archpath(self):
        """Test batch retrieval with archpath to extract files from tar archive."""
        # Create sample data
        sample_files = {
            "file1.txt": b"Content of file 1",
            "dir/file2.txt": b"Content of file 2 in directory",
            "dir/subdir/file3.txt": b"Content of file 3 in subdirectory",
        }

        # Create tar archive
        tar_buffer = BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            for filename, content in sample_files.items():
                tarinfo = tarfile.TarInfo(name=filename)
                tarinfo.size = len(content)
                tar.addfile(tarinfo, BytesIO(content))

        # Upload tar archive
        archive_obj = self.bucket.object("test-archive.tar")
        archive_obj.get_writer().put_content(tar_buffer.getvalue())

        # Create batch request to extract specific files
        batch = self.client.batch(bucket=self.bucket, streaming_get=True)
        batch.add(archive_obj, archpath="file1.txt")
        batch.add(archive_obj, archpath="dir/file2.txt")
        batch.add(archive_obj, archpath="dir/subdir/file3.txt")

        # Execute and verify
        results = list(batch.get())
        self.assertEqual(len(results), 3, "Expected 3 extracted files")

        # Verify each extracted file
        for i, (batch_response, actual_content) in enumerate(results):
            expected_filename = list(sample_files.keys())[i]
            expected_content = sample_files[expected_filename]
            self.assertEqual(batch_response.obj_name, archive_obj.name)
            self.assertEqual(batch_response.archpath, expected_filename)
            self.assertEqual(
                actual_content,
                expected_content,
                f"Content mismatch for {expected_filename}",
            )

    def test_batch_with_opaque_data(self):
        """Test batch request with opaque user data."""
        obj, content = self._create_object_with_content()

        # User-provided tracking data
        opaque_data = b"user-tracking-id-12345"

        batch = self.client.batch(bucket=self.bucket, streaming_get=False)
        batch.add(obj, opaque=opaque_data)

        results = list(batch.get())
        self.assertEqual(len(results), 1)

        batch_response, actual_content = results[0]
        self.assertEqual(batch_response.obj_name, obj.name)
        self.assertEqual(actual_content, content)

        # Verify opaque data is preserved
        self.assertEqual(
            batch_response.opaque,
            opaque_data,
            "Opaque data should be returned unchanged",
        )

    def test_batch_continue_on_error(self):
        """Test batch with cont_on_err flag for missing objects."""
        # Create one valid object
        valid_obj, valid_content = self._create_object_with_content()

        # Create reference to non-existent object
        missing_obj = self.bucket.object("non-existent-file.txt")

        # Create batch with continue on error
        batch = self.client.batch(
            bucket=self.bucket, streaming_get=False, cont_on_err=True
        )
        batch.add(missing_obj)
        batch.add(valid_obj)

        results = list(batch.get())

        # Should get 2 results (one with error, one successful)
        self.assertEqual(len(results), 2, "Expected 2 results with cont_on_err=True")

        # First result should indicate missing file
        missing_response, _ = results[0]
        self.assertIsNotNone(
            missing_response.err_msg, "Missing object should have error message"
        )

        # Second result should be successful
        valid_response, actual_content = results[1]
        self.assertEqual(valid_response.obj_name, valid_obj.name)
        self.assertEqual(actual_content, valid_content)
        self.assertIsNone(valid_response.err_msg, "Valid object should not have error")

    def test_batch_empty_error(self):
        """Test that empty batch raises appropriate error."""
        batch = self.client.batch(bucket=self.bucket, streaming_get=False)

        with self.assertRaisesRegex(ValueError, "No objects added to batch"):
            list(batch.get())

    def test_batch_raw_mode(self):
        """Test batch retrieval in raw mode (no extraction)."""
        obj, _ = self._create_object_with_content()

        batch = self.client.batch(
            objects=[obj], bucket=self.bucket, streaming_get=False
        )
        raw_result = batch.get(raw=True)

        # Raw mode should return a stream
        self.assertTrue(
            hasattr(raw_result, "read"), "Raw mode should return a file-like object"
        )

        raw_result.close()

    def test_batch_method_chaining(self):
        """Test that add() method supports chaining."""
        obj1, _ = self._create_object_with_content()
        obj2, _ = self._create_object_with_content()
        obj3, _ = self._create_object_with_content()

        # Test method chaining
        batch = self.client.batch(bucket=self.bucket, streaming_get=False)
        result = batch.add(obj1).add(obj2).add(obj3)

        # Verify chaining returns self
        self.assertIs(result, batch, "add() should return self for chaining")

        # Verify all objects were added
        self.assertEqual(len(batch), 3, "Should have 3 objects in batch")

        # Execute and verify
        results = list(batch.get())
        self.assertEqual(len(results), 3, "Should retrieve 3 objects")

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    def test_batch_remote_bucket(self):
        """Test batch with remote bucket objects."""
        obj, content = self._create_object_with_content()

        batch = self.client.batch(streaming_get=False)
        batch.add(obj)

        results = list(batch.get())

        self.assertEqual(len(results), 1)
        batch_response, actual_content = results[0]
        self.assertEqual(batch_response.obj_name, obj.name)
        self.assertEqual(actual_content, content)

    def test_batch_length(self):
        """Test batch __len__ method."""
        batch = self.client.batch(bucket=self.bucket, streaming_get=False)

        self.assertEqual(len(batch), 0, "Empty batch should have length 0")

        obj1, _ = self._create_object_with_content()
        batch.add(obj1)
        self.assertEqual(len(batch), 1, "Batch with 1 object should have length 1")

        obj2, _ = self._create_object_with_content()
        batch.add(obj2)
        self.assertEqual(len(batch), 2, "Batch with 2 objects should have length 2")

    def test_batch_repr(self):
        """Test batch string representation."""
        batch = self.client.batch(
            bucket=self.bucket, streaming_get=False, output_format=".tar.gz"
        )

        obj, _ = self._create_object_with_content()
        batch.add(obj)

        repr_str = repr(batch)
        self.assertIn("Batch", repr_str, "Repr should contain 'Batch'")
        self.assertIn("1", repr_str, "Repr should show number of objects")
        self.assertIn(".tar.gz", repr_str, "Repr should show output format")

    def test_batch_multiple_buckets(self):
        """
        Test batch retrieval from multiple different buckets.

        Verifies that objects from different buckets can be retrieved
        in a single batch request.
        """
        # Create second bucket
        bucket2 = self._create_bucket()

        # Create objects in different buckets
        obj1, content1 = self._create_object_with_content()

        # Create object in second bucket
        obj2, content2 = self._create_object_with_content()
        # Copy to bucket2 by creating a new object there
        obj2_in_bucket2 = bucket2.object(obj2.name)
        obj2_in_bucket2.get_writer().put_content(content2)

        # Create batch with objects from both buckets
        batch = self.client.batch(streaming_get=False)
        batch.add(obj1)
        batch.add(obj2_in_bucket2)

        results = list(batch.get())

        # Verify both objects retrieved
        self.assertEqual(len(results), 2, "Should retrieve objects from both buckets")

        # Verify first object
        resp1, data1 = results[0]
        self.assertEqual(resp1.obj_name, obj1.name)
        self.assertEqual(resp1.bucket, obj1.bucket_name)
        self.assertEqual(data1, content1)

        # Verify second object
        resp2, data2 = results[1]
        self.assertEqual(resp2.obj_name, obj2_in_bucket2.name)
        self.assertEqual(resp2.bucket, obj2_in_bucket2.bucket_name)
        self.assertEqual(data2, content2)

    def test_batch_nonexistent_bucket_error(self):
        """
        Test batch error handling when bucket doesn't exist.

        Verifies that appropriate error is raised when trying to
        retrieve from a non-existent bucket.
        """

        # Create object reference to non-existent bucket
        fake_bucket = self.client.bucket("non-existent-bucket-12345")

        # Create batch request
        batch = self.client.batch(objects=["obj"], bucket=fake_bucket)

        # Should raise AISError
        with self.assertRaises(AISError) as context:
            list(batch.get())

        # Verify error details
        error_str = str(context.exception)
        self.assertIn("404", error_str, "Should be a 400 error")
        self.assertIn(
            "does not exist", error_str.lower(), "Error should mention bucket not found"
        )

    @unittest.skip("Not Implemented")
    def test_batch_byte_ranges(self):
        """
        Test batch retrieval with byte range requests.

        Verifies that specific byte ranges can be requested from objects.
        """
        # Create object with known content
        obj, content = self._create_object_with_content(obj_size=100)

        # Request specific byte ranges
        batch = self.client.batch(bucket=self.bucket, streaming_get=False)
        batch.add(obj, start=0, length=10)  # First 10 bytes
        batch.add(obj, start=10, length=10)  # Next 10 bytes
        batch.add(obj, start=90, length=10)  # Last 10 bytes

        results = list(batch.get())
        self.assertEqual(len(results), 3, "Should retrieve 3 byte ranges")

        # Verify byte ranges
        _, data1 = results[0]
        self.assertEqual(data1, content[:10], "First 10 bytes mismatch")

        _, data2 = results[1]
        self.assertEqual(data2, content[10:20], "Second 10 bytes mismatch")

        _, data3 = results[2]
        self.assertEqual(data3, content[90:100], "Last 10 bytes mismatch")

    def test_batch_no_default_bucket(self):
        """Test batch retrieval with no default bucket."""
        batch = self.client.batch()

        with self.assertRaises(ValueError) as context:
            batch.add("obj1")

        self.assertIn(
            "Bucket must be provided when objects are specified as raw names",
            str(context.exception),
            "Should raise ValueError with 'No bucket provided' message",
        )
