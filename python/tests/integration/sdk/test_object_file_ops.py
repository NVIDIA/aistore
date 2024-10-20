#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest

from aistore.sdk import Bucket
from aistore.sdk.client import Client
from aistore.sdk.obj.object_reader import ObjectReader
from aistore.sdk.const import DEFAULT_CHUNK_SIZE
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import create_and_put_object, random_string, test_cases_combinations


class TestObjectFileOps(unittest.TestCase):
    OBJECT_NAME = "test-object"
    BUCKET_NAME = f"test-bucket-{random_string(8)}"
    OBJECT_SIZE = 5242880
    BUFFER_SIZES = [DEFAULT_CHUNK_SIZE // 2, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE * 2]
    client: Client = None
    bucket: Bucket = None
    object_reader: ObjectReader = None

    @classmethod
    def setUpClass(cls):
        cls.client = Client(CLUSTER_ENDPOINT)
        cls.bucket = cls.client.bucket(cls.BUCKET_NAME).create()

        cls.test_data = create_and_put_object(
            cls.client, cls.BUCKET_NAME, cls.OBJECT_NAME, obj_size=cls.OBJECT_SIZE
        )
        cls.object_reader = cls.bucket.object(cls.OBJECT_NAME).get()

    @classmethod
    def tearDownClass(cls):
        cls.bucket.delete(missing_ok=True)

    @test_cases_combinations(
        [
            DEFAULT_CHUNK_SIZE // 2,
            DEFAULT_CHUNK_SIZE,
            DEFAULT_CHUNK_SIZE * 2,
        ],  # Buffer sizes
        [
            DEFAULT_CHUNK_SIZE // 2,  # Read less than a chunk
            DEFAULT_CHUNK_SIZE,  # Read exactly one chunk
            DEFAULT_CHUNK_SIZE + (DEFAULT_CHUNK_SIZE // 2),  # More than a chunk
            DEFAULT_CHUNK_SIZE * 2
            + (DEFAULT_CHUNK_SIZE // 4),  # Multiple chunks + partial
            -1,  # Entire file
            0,  # Zero-size read
        ],  # Read sizes
    )
    def test_read(self, buffer_size, read_size):
        """
        Test reading with various sizes and buffer sizes.
        """
        object_file = self.object_reader.as_file(buffer_size=buffer_size)

        # Expected size based on read_size
        expected_size = len(self.test_data) if read_size == -1 else read_size

        # Read the file with the specified read_size
        data = object_file.read(read_size)

        # Validate the read data
        self.assertEqual(len(data), expected_size)
        self.assertEqual(object_file.tell(), expected_size)
        self.assertEqual(data, self.test_data[:expected_size])

    @test_cases_combinations(
        [
            DEFAULT_CHUNK_SIZE // 2,
            DEFAULT_CHUNK_SIZE,
            DEFAULT_CHUNK_SIZE * 2,
        ],  # Buffer sizes
        [
            DEFAULT_CHUNK_SIZE // 2,  # Read less than a chunk
            DEFAULT_CHUNK_SIZE,  # Read exactly one chunk
            DEFAULT_CHUNK_SIZE + (DEFAULT_CHUNK_SIZE // 2),  # More than a chunk
            DEFAULT_CHUNK_SIZE * 2
            + (DEFAULT_CHUNK_SIZE // 4),  # Multiple chunks + partial
        ],  # Read sizes
    )
    def test_multiple_reads_til_eof(self, buffer_size, read_size):
        """
        Test reading with various sizes and buffer sizes until EOF.
        """
        object_file = self.object_reader.as_file(buffer_size=buffer_size)

        read_data = bytearray()

        while True:
            data = object_file.read(read_size)
            if not data:
                break
            read_data.extend(data)

        # Validate the read data
        self.assertEqual(len(read_data), len(self.test_data))
        self.assertEqual(object_file.tell(), len(self.test_data))
        self.assertEqual(bytes(read_data), self.test_data)

    def test_context_manager(self):
        """Test the context manager (__enter__ and __exit__) functionality."""
        # First read using context manager
        object_file = self.object_reader.as_file()

        with object_file as f:
            # Ensure the file is readable
            self.assertTrue(f.readable())
            # Read first part of the data
            read_size = DEFAULT_CHUNK_SIZE
            data1 = f.read(read_size)
            self.assertEqual(data1, self.test_data[:read_size])
            self.assertEqual(f.tell(), read_size)

        # After exiting the context, the file should be closed
        self.assertFalse(object_file.readable())
        self.assertFalse(object_file.seekable())
        with self.assertRaises(ValueError):
            object_file.tell()
        with self.assertRaises(ValueError):
            object_file.read()

        # Re-open and read from the beginning using context manager
        with object_file as f:
            # Ensure the file is readable
            self.assertTrue(f.readable())
            # Read the entire data
            data2 = f.read()
            self.assertEqual(data2, self.test_data)
            self.assertEqual(f.tell(), len(self.test_data))
