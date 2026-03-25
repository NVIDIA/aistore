#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

import os
import unittest
from io import BytesIO

import pytest

from aistore.sdk import Bucket, Object
from aistore.sdk.client import Client
from aistore.sdk.const import DEFAULT_CHUNK_SIZE, AIS_CHECKSUM_VALUE, MIB
from aistore.sdk.etl.etl_config import ETLConfig
from aistore.sdk.obj.content_iterator.buffer import ParallelBuffer
from tests.integration import AWS_BUCKET
from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.utils import create_and_put_object, random_string


class TestObjectReaderOps(unittest.TestCase):
    client: Client
    bucket: Bucket
    object: Object
    object_size = DEFAULT_CHUNK_SIZE * 2

    @classmethod
    def setUpClass(cls):
        cls.client = DEFAULT_TEST_CLIENT
        bck_name = f"test-bck-{random_string(8)}"
        cls.bucket = cls.client.bucket(bck_name).create(exist_ok=True)
        object_name = "test-object"
        cls.object, cls.object_content = create_and_put_object(
            client=cls.client,
            bck=cls.bucket.as_model(),
            obj_name=object_name,
            obj_size=cls.object_size,
        )
        cls.object_reader = cls.object.get_reader()

    @classmethod
    def tearDownClass(cls):
        cls.bucket.delete()

    def test_head(self):
        attributes = self.object_reader.head()
        self.assertEqual(attributes.size, self.object_size)
        self.assertEqual(
            self.object.head()[AIS_CHECKSUM_VALUE],
            attributes.checksum_value,
        )

    def test_read_all(self):
        content = self.object_reader.read_all()
        self.assertEqual(content, self.object_content)

    def test_raw(self):
        raw_stream = self.object_reader.raw()

        content_stream = BytesIO()
        for chunk in raw_stream:
            content_stream.write(chunk)

        raw_content = content_stream.getvalue()
        self.assertEqual(raw_content, self.object_content)

    def test_iter(self):
        chunks = list(self.object_reader)
        combined_content = b"".join(chunks)
        self.assertEqual(combined_content, self.object_content)
        self.assertEqual(len(chunks), 2)


# pylint: disable=too-many-public-methods
class TestParallelObjectReaderOps(unittest.TestCase):
    """Integration tests for parallel object download using num_workers."""

    client: Client
    bucket: Bucket
    object_size = DEFAULT_CHUNK_SIZE * 4  # 4 chunks

    @classmethod
    def setUpClass(cls):
        cls.client = DEFAULT_TEST_CLIENT
        bck_name = f"test-parallel-bck-{random_string(8)}"
        cls.bucket = cls.client.bucket(bck_name).create(exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        cls.bucket.delete()

    def test_parallel_download_matches_sequential(self):
        """Test that parallel download produces same content as sequential."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        sequential_content = b"".join(obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE))
        parallel_content = b"".join(
            obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4)
        )

        self.assertEqual(sequential_content, expected_content)
        self.assertEqual(parallel_content, expected_content)

    def test_parallel_download_many_workers(self):
        """Test parallel download with more workers than chunks."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        content = b"".join(
            obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=16)
        )
        self.assertEqual(content, expected_content)

    def test_parallel_download_small_object(self):
        """Test parallel download on object smaller than chunk size."""
        obj, expected_content = create_and_put_object(
            self.client,
            self.bucket.as_model(),
            random_string(),
            DEFAULT_CHUNK_SIZE // 2,
        )

        content = b"".join(obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4))
        self.assertEqual(content, expected_content)

    def test_parallel_download_small_chunks(self):
        """Test parallel download with small chunk size creating many chunks."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        content = b"".join(
            obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE // 8, num_workers=8)
        )
        self.assertEqual(content, expected_content)

    def test_parallel_download_invalid_num_workers(self):
        """Test that invalid num_workers raises ValueError."""
        obj, _ = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        with self.assertRaises(ValueError):
            obj.get_reader(num_workers=0)
        with self.assertRaises(ValueError):
            obj.get_reader(num_workers=-1)

    def test_parallel_download_with_byte_range_raises(self):
        """Test that combining num_workers with byte_range raises ValueError."""
        obj, _ = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        with self.assertRaises(ValueError):
            obj.get_reader(byte_range="0-100", num_workers=4)

    def test_parallel_download_with_etl_raises(self):
        """Test that combining num_workers with etl raises ValueError."""
        obj, _ = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        with self.assertRaises(ValueError):
            obj.get_reader(etl=ETLConfig("some-etl"), num_workers=4)

    def test_parallel_download_with_offset(self):
        """Test that parallel iterator correctly handles offset parameter."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        # Get reader and access the content provider directly
        reader = obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4)

        # Test with offset at chunk boundary
        offset = DEFAULT_CHUNK_SIZE
        # pylint: disable=protected-access
        content_from_offset = b"".join(reader._content_provider.create_iter(offset))
        self.assertEqual(content_from_offset, expected_content[offset:])

        # Test with offset in middle of a chunk
        offset = DEFAULT_CHUNK_SIZE + 100
        content_from_offset = b"".join(reader._content_provider.create_iter(offset))
        self.assertEqual(content_from_offset, expected_content[offset:])

    def test_parallel_download_as_file(self):
        """Test as_file() works correctly with parallel download."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        reader = obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4)
        object_file = reader.as_file()

        # Read in chunks using file interface
        read_data = bytearray()
        while True:
            chunk = object_file.read(DEFAULT_CHUNK_SIZE // 2)
            if not chunk:
                break
            read_data.extend(chunk)

        self.assertEqual(bytes(read_data), expected_content)

    def test_parallel_download_as_file_read_all(self):
        """Test as_file().read(-1) reads entire content with parallel download."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        reader = obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4)
        object_file = reader.as_file()

        content = object_file.read(-1)
        self.assertEqual(content, expected_content)

    def test_parallel_download_with_writer(self):
        """Test writer parameter works with parallel download."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        # Use BytesIO as writer
        output = BytesIO()
        obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4, writer=output)

        self.assertEqual(output.getvalue(), expected_content)

    def test_parallel_download_as_file_resume_simulation(self):
        """Test that parallel iterator supports resume via offset (used by ObjectFileReader).

        This simulates what ObjectFileReader does when resuming after a stream interruption:
        it creates a new iterator starting from the last successfully read position.
        """
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        reader = obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4)
        object_file = reader.as_file()

        # Read first portion (simulating successful read before interruption)
        first_read_size = DEFAULT_CHUNK_SIZE + 500  # Read past first chunk boundary
        first_portion = object_file.read(first_read_size)
        self.assertEqual(first_portion, expected_content[:first_read_size])

        # Simulate resume: create new iterator from current position
        # This is what ObjectFileReader._handle_broken_stream does
        resume_position = first_read_size
        # pylint: disable=protected-access
        resumed_iter = reader._content_provider.create_iter(offset=resume_position)
        remaining_content = b"".join(resumed_iter)

        # Verify resumed content matches expected remaining bytes
        self.assertEqual(remaining_content, expected_content[resume_position:])

        # Verify combining both portions gives complete content
        self.assertEqual(first_portion + remaining_content, expected_content)

    def test_parallel_download_as_file_multiple_resumes(self):
        """Test multiple resume points work correctly with parallel iterator."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )

        reader = obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4)

        # Simulate multiple resume points at various offsets
        resume_points = [
            0,  # Start
            DEFAULT_CHUNK_SIZE // 2,  # Middle of first chunk
            DEFAULT_CHUNK_SIZE,  # Chunk boundary
            DEFAULT_CHUNK_SIZE * 2 + 100,  # Middle of third chunk
        ]

        for offset in resume_points:
            # pylint: disable=protected-access
            content_from_offset = b"".join(
                reader._content_provider.create_iter(offset=offset)
            )
            self.assertEqual(
                content_from_offset,
                expected_content[offset:],
                f"Content mismatch when resuming from offset {offset}",
            )

    # --- read_all() tests (direct-to-destination / ParallelBuffer path) ---

    def test_read_all_returns_parallel_buffer(self):
        """read_all() with num_workers returns a ParallelBuffer with the correct content."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )
        result = obj.get_reader(chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4).read_all()
        try:
            self.assertIsInstance(result, ParallelBuffer)
            self.assertEqual(result.tobytes(), expected_content)
        finally:
            result.close()

    def test_read_all_content_matches_sequential(self):
        """read_all() with num_workers produces the same bytes as sequential read_all()."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )
        sequential = obj.get_reader().read_all()
        with obj.get_reader(
            chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4
        ).read_all() as result:
            self.assertEqual(result.tobytes(), sequential)
            self.assertEqual(result.tobytes(), expected_content)

    def test_read_all_context_manager_releases_shm(self):
        """ParallelBuffer.buf is unusable after exiting the context manager."""
        obj, _ = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )
        with obj.get_reader(
            chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4
        ).read_all() as result:
            _ = result.tobytes()  # access is fine inside the block

        # After exit, the underlying shm is released; accessing buf must raise.
        with self.assertRaises(Exception):
            _ = bytes(result.buf)

    def test_read_all_len(self):
        """len(ParallelBuffer) equals the object size."""
        obj, _ = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )
        with obj.get_reader(
            chunk_size=DEFAULT_CHUNK_SIZE, num_workers=4
        ).read_all() as result:
            self.assertEqual(len(result), self.object_size)

    def test_read_all_small_chunks(self):
        """read_all() with small chunk size (many chunks) produces correct content."""
        obj, expected_content = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), self.object_size
        )
        with obj.get_reader(
            chunk_size=DEFAULT_CHUNK_SIZE // 8, num_workers=8
        ).read_all() as result:
            self.assertEqual(result.tobytes(), expected_content)

    def test_read_all_empty_object_raises(self):
        """Parallel download on a zero-size object raises ValueError."""
        obj_name = f"empty-{random_string(8)}"
        obj = self.bucket.object(obj_name)
        obj.get_writer().put_content(b"")

        with self.assertRaises(ValueError):
            obj.get_reader(num_workers=4)

    def test_read_all_single_byte_object(self):
        """read_all() with num_workers on a 1-byte object returns correct content."""
        obj, expected = create_and_put_object(
            self.client, self.bucket.as_model(), random_string(), 1
        )
        with obj.get_reader(num_workers=4).read_all() as result:
            self.assertEqual(len(result), 1)
            self.assertEqual(result.tobytes(), expected)

    @pytest.mark.extended
    def test_parallel_download_large_object(self):
        """Test parallel download with a large object (256 MiB)."""
        large_size = 256 * 1024 * 1024  # 256 MiB
        chunk_size = 8 * 1024 * 1024  # 8 MiB chunks

        # Create large object with random data
        obj_name = f"large-parallel-{random_string(8)}"
        large_data = os.urandom(large_size)
        obj = self.bucket.object(obj_name)
        obj.get_writer().put_content(large_data)

        # Parallel download
        reader = obj.get_reader(chunk_size=chunk_size, num_workers=8)
        downloaded = b"".join(reader)

        self.assertEqual(len(downloaded), large_size)
        self.assertEqual(downloaded, large_data)


class TestParallelColdGetOps(ParallelTestBase):
    """Parallel download on non-cached (cold) remote objects."""

    COLD_OBJ_DATA = os.urandom(4 * MIB)

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_cold_parallel_create_iter(self):
        """Parallel iterator works on a non-cached remote object."""
        obj_name = f"{self.obj_prefix}-cold-iter"
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)
        self.s3_client.put_object(
            Bucket=self.bucket.name, Key=obj_name, Body=self.COLD_OBJ_DATA
        )

        content = b"".join(self.bucket.object(obj_name).get_reader(num_workers=4))
        self.assertEqual(content, self.COLD_OBJ_DATA)

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_cold_parallel_read_all(self):
        """Parallel read_all() works on a non-cached remote object."""
        obj_name = f"{self.obj_prefix}-cold-readall"
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)
        self.s3_client.put_object(
            Bucket=self.bucket.name, Key=obj_name, Body=self.COLD_OBJ_DATA
        )

        with self.bucket.object(obj_name).get_reader(
            num_workers=4
        ).read_all() as result:
            self.assertIsInstance(result, ParallelBuffer)
            self.assertEqual(result.tobytes(), self.COLD_OBJ_DATA)
