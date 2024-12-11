#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest

from aistore.sdk import Bucket
from aistore.sdk.client import Client
from aistore.sdk.obj.object_reader import ObjectReader
from aistore.sdk.const import DEFAULT_CHUNK_SIZE
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import create_and_put_object, random_string, case_matrix


class TestObjectFileReaderOps(unittest.TestCase):
    OBJECT_NAME = "test-object"
    BUCKET_NAME = f"test-bucket-{random_string(8)}"
    OBJECT_SIZE = 5242880
    client: Client = None
    bucket: Bucket = None
    object_reader: ObjectReader = None

    @classmethod
    def setUpClass(cls):
        cls.client = Client(CLUSTER_ENDPOINT)
        cls.bucket = cls.client.bucket(cls.BUCKET_NAME).create()

        create_and_put_object(
            cls.client, cls.BUCKET_NAME, cls.OBJECT_NAME, obj_size=cls.OBJECT_SIZE
        )
        cls.object_reader = cls.bucket.object(cls.OBJECT_NAME).get_reader(
            byte_range="bytes=5-"
        )

    @classmethod
    def tearDownClass(cls):
        cls.bucket.delete(missing_ok=True)

    @case_matrix(
        [
            DEFAULT_CHUNK_SIZE - 1,  # Slightly less than a chunk
            DEFAULT_CHUNK_SIZE,  # Exactly a chunk
            DEFAULT_CHUNK_SIZE + 1,  # Slightly more than a chunk
            0,
        ]
    )
    def test_read(self, read_size):
        """
        Test reading with various sizes using the default buffer size.
        """
        object_file = self.object_reader.as_file()

        # Read the file with the specified read_size
        data = object_file.read(read_size)

        # Validate the read data
        self.assertEqual(len(data), read_size)
        self.assertEqual(data, self.object_reader.raw().read(read_size))

    @case_matrix(
        [
            -1,  # Read all
            DEFAULT_CHUNK_SIZE - 1,  # Slightly less than a chunk
            DEFAULT_CHUNK_SIZE,  # Exactly a chunk
            DEFAULT_CHUNK_SIZE + 1,  # Slightly more than a chunk
        ],
    )
    def test_read_til_eof(self, read_size):
        """Test reading with various sizes and buffer sizes until EOF."""
        object_file = self.object_reader.as_file()

        read_data = bytearray()

        while True:
            data = object_file.read(read_size)
            if not data:
                break
            read_data.extend(data)

        # Validate the read data
        expected_data = self.object_reader.raw().read()
        self.assertEqual(len(read_data), len(expected_data))
        self.assertEqual(bytes(read_data), expected_data)

        # Attempt to read again after EOF
        extra_data = object_file.read()
        self.assertEqual(extra_data, b"")

    def test_context_manager(self):
        """Test the context manager functionality."""
        object_file = self.object_reader.as_file()
        with object_file as f:
            self.assertTrue(f.readable())
            data = f.read(DEFAULT_CHUNK_SIZE)
            self.assertEqual(data, self.object_reader.raw().read(DEFAULT_CHUNK_SIZE))
        # After exiting the context, the file should be closed
        self.assertFalse(object_file.readable())
        with self.assertRaises(ValueError):
            object_file.read()
