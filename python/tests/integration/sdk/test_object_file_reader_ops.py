#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#

from aistore.sdk.const import DEFAULT_CHUNK_SIZE
from tests.const import MB
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.utils import random_string, case_matrix

OBJECT_NAME = "test-object"
BUCKET_NAME = f"test-bucket-{random_string(8)}"
OBJECT_SIZE = 2 * MB


class TestObjectFileReaderOps(ParallelTestBase):
    def setUp(self) -> None:
        super().setUp()
        obj, _ = self._create_object_with_content(obj_size=OBJECT_SIZE)
        self.object_reader = obj.get_reader(byte_range="bytes=5-")

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
