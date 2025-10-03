#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from aistore.sdk.errors import AISError
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.integration import AWS_BUCKET
from tests.utils import random_string


class TestObjectMultipartOps(ParallelTestBase):
    """Integration tests for multipart upload operations."""

    def setUp(self) -> None:
        """Set up test with a local AIS bucket for multipart upload testing."""
        super().setUp()
        self.multipart_bucket = self._create_bucket(prefix="multipart-test-bck")

    def _create_local_object(self, suffix=""):
        """Helper method to create a test object in the local AIS multipart bucket."""
        obj_name = f"{self.obj_prefix}-multipart-test"
        if suffix:
            obj_name += f"-{suffix}"
        obj = self.multipart_bucket.object(obj_name)
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)
        return obj

    def test_multipart_upload_basic(self):
        """Test basic multipart upload workflow: create, add parts, complete."""
        obj = self._create_local_object()

        # Create multipart upload
        mpu = obj.multipart_upload().create()
        self.assertIsNotNone(mpu.upload_id)
        self.assertEqual(mpu.parts, [])

        # Add parts with content
        part1_content = b"Part 1 content: " + random_string(100).encode()
        part2_content = b"Part 2 content: " + random_string(100).encode()
        part3_content = b"Part 3 content: " + random_string(100).encode()

        writer1 = mpu.add_part(1)
        writer1.put_content(part1_content)

        writer2 = mpu.add_part(2)
        writer2.put_content(part2_content)

        writer3 = mpu.add_part(3)
        writer3.put_content(part3_content)

        # Verify parts were tracked
        self.assertEqual(mpu.parts, [1, 2, 3])

        # Complete the upload
        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify the final object contains all parts
        final_content = obj.get_reader().read_all()
        expected_content = part1_content + part2_content + part3_content
        self.assertEqual(final_content, expected_content)

    def test_multipart_upload_single_part(self):
        """Test multipart upload with only one part."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload().create()
        content = b"Single part content: " + random_string(200).encode()

        writer = mpu.add_part(1)
        writer.put_content(content)

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        final_content = obj.get_reader().read_all()
        self.assertEqual(final_content, content)

    def test_multipart_upload_large_parts(self):
        """Test multipart upload with larger parts."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload().create()

        # Create larger parts (1KB each)
        part_size = 1024
        num_parts = 5
        parts_content = []

        for i in range(1, num_parts + 1):
            content = f"Part {i}: ".encode() + b"x" * (part_size - len(f"Part {i}: "))
            parts_content.append(content)

            writer = mpu.add_part(i)
            writer.put_content(content)

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify final content
        final_content = obj.get_reader().read_all()
        expected_content = b"".join(parts_content)
        self.assertEqual(final_content, expected_content)
        self.assertEqual(len(final_content), part_size * num_parts)

    def test_multipart_upload_out_of_order_parts(self):
        """Test adding parts out of order (3, 1, 2) - parts are assembled by part number."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload().create()

        # Add parts out of order
        part1_content = b"This should be first"
        part2_content = b"This should be second"
        part3_content = b"This should be third"

        # Add part 3 first
        writer3 = mpu.add_part(3)
        writer3.put_content(part3_content)

        # Add part 1
        writer1 = mpu.add_part(1)
        writer1.put_content(part1_content)

        # Add part 2
        writer2 = mpu.add_part(2)
        writer2.put_content(part2_content)

        # Parts should be tracked in the order they were added
        self.assertEqual(mpu.parts, [3, 1, 2])

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Final content should be in part number order (1, 2, 3), not upload order
        final_content = obj.get_reader().read_all()
        expected_content = part1_content + part2_content + part3_content
        self.assertEqual(final_content, expected_content)

    def test_multipart_upload_abort(self):
        """Test aborting a multipart upload."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload().create()
        self.assertIsNotNone(mpu.upload_id)

        # Add some parts
        writer1 = mpu.add_part(1)
        writer1.put_content(b"Part 1 content")

        writer2 = mpu.add_part(2)
        writer2.put_content(b"Part 2 content")

        # Abort the upload
        response = mpu.abort()
        self.assertEqual(response.status_code, 200)

        # Object should not exist
        try:
            obj.get_reader().read_all()
        except AISError:
            pass

    def test_multipart_upload_file_parts(self):
        """Test multipart upload using file content for parts."""
        obj = self._create_local_object()

        # Create test files
        test_files = []
        expected_content = b""

        for i in range(1, 4):
            filename = self.local_test_files.joinpath(f"part_{i}.txt")
            content = f"File part {i} content: {random_string(50)}".encode()

            with open(filename, "wb") as f:
                f.write(content)

            test_files.append(filename)
            expected_content += content

        mpu = obj.multipart_upload().create()

        # Upload each file as a part
        for i, filename in enumerate(test_files, 1):
            writer = mpu.add_part(i)
            writer.put_file(filename)

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify final content
        final_content = obj.get_reader().read_all()
        self.assertEqual(final_content, expected_content)

    def test_multipart_upload_empty_parts_not_allowed(self):
        """Test that empty parts are not allowed in AIS multipart upload."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload().create()

        # Try to add empty part - should fail
        writer1 = mpu.add_part(1)
        with self.assertRaises(AISError) as context:
            writer1.put_content(b"")

        # Verify the error message
        self.assertIn("invalid size (0)", context.exception.message)

    def test_multipart_upload_no_parts_complete_not_allowed(self):
        """Test that completing multipart upload without parts is not allowed."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload().create()

        # Try to complete without adding parts - should fail
        with self.assertRaises(AISError) as context:
            mpu.complete()

        # Verify the error message
        self.assertIn("no chunks", context.exception.message)

    def test_multipart_upload_add_part_without_create(self):
        """Test that adding a part without creating upload raises error."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload()

        # Try to add part without creating upload
        with self.assertRaises(ValueError) as context:
            mpu.add_part(1)

        self.assertEqual(str(context.exception), "Multipart upload not created")

    def test_multipart_upload_invalid_part_numbers(self):
        """Test that invalid part numbers are rejected."""
        obj = self._create_local_object()
        mpu = obj.multipart_upload().create()

        # Test zero part number
        with self.assertRaises(ValueError) as context:
            mpu.add_part(0)
        self.assertEqual(
            str(context.exception), "Part number must be a positive integer"
        )

        # Test negative part number
        with self.assertRaises(ValueError) as context:
            mpu.add_part(-1)
        self.assertEqual(
            str(context.exception), "Part number must be a positive integer"
        )

        # Verify no parts were added due to validation failures
        self.assertEqual(mpu.parts, [])

        # Clean up
        mpu.abort()

    def test_multipart_upload_duplicate_part_numbers_not_allowed(self):
        """Test that duplicate part numbers cause completion issues."""
        obj = self._create_local_object()

        mpu = obj.multipart_upload().create()

        # Add part 1 twice with different content
        writer1a = mpu.add_part(1)
        writer1a.put_content(b"First version of part 1")

        writer1b = mpu.add_part(1)
        writer1b.put_content(b"Second version of part 1")

        # Parts list should contain both entries
        self.assertEqual(mpu.parts, [1, 1])

        # Completion should fail due to duplicate parts
        with self.assertRaises(AISError) as context:
            mpu.complete()

        # Verify the error mentions partial completion
        self.assertIn("partial completion", context.exception.message)

    def test_multipart_upload_concurrent_operations(self):
        """Test multiple concurrent multipart uploads on different objects."""
        num_objects = 3
        mpus = []
        expected_contents = []

        # Create multiple multipart uploads with unique object names
        for i in range(num_objects):
            obj = self._create_local_object(suffix=f"concurrent-{i}")
            mpu = obj.multipart_upload().create()
            mpus.append((obj, mpu))

            # Add parts to each upload
            content = f"Concurrent upload {i} content: {random_string(50)}".encode()
            expected_contents.append(content)

            writer = mpu.add_part(1)
            writer.put_content(content)

        # Complete all uploads
        for obj, mpu in mpus:
            response = mpu.complete()
            self.assertEqual(response.status_code, 200)

        # Verify all objects have correct content
        for i, (obj, mpu) in enumerate(mpus):
            final_content = obj.get_reader().read_all()
            self.assertEqual(final_content, expected_contents[i])

    def test_multipart_upload_overwrite_existing_object(self):
        """Test multipart upload that overwrites an existing object."""
        obj = self._create_local_object()

        # First, create an object with initial content
        initial_content = b"Initial object content"
        obj.get_writer().put_content(initial_content)

        # Verify initial content
        content = obj.get_reader().read_all()
        self.assertEqual(content, initial_content)

        # Now use multipart upload to overwrite
        mpu = obj.multipart_upload().create()

        new_content = b"New content from multipart upload"
        writer = mpu.add_part(1)
        writer.put_content(new_content)

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify the object was overwritten
        final_content = obj.get_reader().read_all()
        self.assertEqual(final_content, new_content)
        self.assertNotEqual(final_content, initial_content)

    def test_overwrite_multipart_object_with_normal_put(self):
        """Test overwriting a multipart uploaded object with a normal put operation."""
        obj = self._create_local_object()

        # First, create object using multipart upload
        mpu = obj.multipart_upload().create()

        multipart_content = b"Content from multipart upload"
        writer = mpu.add_part(1)
        writer.put_content(multipart_content)

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify multipart content
        content = obj.get_reader().read_all()
        self.assertEqual(content, multipart_content)

        # Now overwrite with normal put
        normal_content = b"Content from normal put operation"
        obj.get_writer().put_content(normal_content)

        # Verify the object was overwritten with normal put content
        final_content = obj.get_reader().read_all()
        self.assertEqual(final_content, normal_content)
        self.assertNotEqual(final_content, multipart_content)

    def test_multipart_upload_with_as_file_method(self):
        """Test multipart upload using ObjectWriter.as_file() and put_file() methods."""
        obj = self._create_local_object()

        # Create test files for parts
        test_files = []
        expected_content = b""

        for i in range(1, 4):
            filename = self.local_test_files.joinpath(f"as_file_part_{i}.txt")
            content = f"As file part {i} content: {random_string(100)}".encode()

            with open(filename, "wb") as f:
                f.write(content)

            test_files.append(filename)
            expected_content += content

        mpu = obj.multipart_upload().create()

        # Upload parts using both as_file() and put_file() methods
        for i, filename in enumerate(test_files, 1):
            writer = mpu.add_part(i)

            if i == 1:
                # Test as_file() method for first part
                with writer.as_file() as f:
                    with open(filename, "rb") as source:
                        f.write(source.read())
            else:
                # Test put_file() method for other parts
                writer.put_file(filename)

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify final content matches expected
        final_content = obj.get_reader().read_all()
        self.assertEqual(final_content, expected_content)

    def test_multipart_upload_parallel_parts(self):
        """Test multipart upload with parts uploaded in parallel using threading."""
        obj = self._create_local_object()
        mpu = obj.multipart_upload().create()
        self.assertIsNotNone(mpu.upload_id)

        # Prepare parts data and expected content
        parts_data = [
            (i, f"Parallel part {i} content: {random_string(200)}".encode())
            for i in range(1, 6)
        ]
        expected_content = b"".join(content for _, content in parts_data)

        def upload_part(part_data):
            """Upload a single part and return success status."""
            part_number, content = part_data
            try:
                mpu.add_part(part_number).put_content(content)
                return part_number, True, None
            except Exception as e:
                return part_number, False, str(e)

        # Upload parts in parallel and collect results
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(upload_part, data) for data in parts_data]
            results = {
                future.result()[0]: future.result()[1:]
                for future in as_completed(futures)
            }

        # Verify all parts uploaded successfully
        for part_num in range(1, 6):
            success, error = results[part_num]
            self.assertTrue(success, f"Part {part_num} failed to upload: {error}")

        # Verify parts were tracked and complete the upload
        self.assertEqual(len(mpu.parts), 5)
        self.assertEqual(set(mpu.parts), {1, 2, 3, 4, 5})

        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify final content is assembled in part number order
        self.assertEqual(obj.get_reader().read_all(), expected_content)

    def test_multipart_upload_complete_already_completed_fails(self):
        """Test that completing an already completed multipart upload fails."""
        obj = self._create_local_object()

        # Create and complete a multipart upload
        mpu = obj.multipart_upload().create()
        writer = mpu.add_part(1)
        writer.put_content(b"Test content")

        # Complete the upload - should succeed
        response1 = mpu.complete()
        self.assertEqual(response1.status_code, 200)

        # Try to complete the same upload again - should fail
        with self.assertRaises(AISError) as context:
            mpu.complete()

        # Verify the error indicates the upload no longer exists
        self.assertIn("does not exist", context.exception.message)

    def test_multipart_upload_complete_after_abort_fails(self):
        """Test that completing an aborted multipart upload fails."""
        obj = self._create_local_object()

        # Create multipart upload and add parts
        mpu = obj.multipart_upload().create()
        writer = mpu.add_part(1)
        writer.put_content(b"Test content")

        # Abort the upload
        abort_response = mpu.abort()
        self.assertEqual(abort_response.status_code, 200)

        # Try to complete the aborted upload - should fail
        with self.assertRaises(AISError) as context:
            mpu.complete()

        # Verify the error indicates the upload no longer exists
        self.assertIn("does not exist", context.exception.message)

    @unittest.skipUnless(AWS_BUCKET, "AWS bucket is not set")
    def test_multipart_upload_aws_minimum_part_size(self):
        """Test multipart upload on AWS bucket with minimum 5MB part size requirement."""
        obj = (
            self._create_object()
        )  # This uses the original bucket from ParallelTestBase

        # Create multipart upload
        mpu = obj.multipart_upload().create()
        self.assertIsNotNone(mpu.upload_id)

        # Create a part with exactly 5MB (AWS minimum)
        part_size = 5 * 1024 * 1024  # 5MB
        large_content = b"A" * part_size

        # Add the large part
        writer = mpu.add_part(1)
        writer.put_content(large_content)

        # Complete the upload
        response = mpu.complete()
        self.assertEqual(response.status_code, 200)

        # Verify the content
        final_content = obj.get_reader().read_all()
        self.assertEqual(final_content, large_content)
        self.assertEqual(len(final_content), part_size)
