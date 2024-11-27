#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest

from aistore.sdk.bucket import Bucket
from aistore.sdk.client import Client
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import random_string


class TestObjectFileWriterOps(unittest.TestCase):
    BUCKET_NAME = f"test-bucket-{random_string(8)}"
    OBJECT_NAME = "test-object"
    client: Client = None
    bucket: Bucket = None
    data_to_write = [b"new-data1", b"new-data2", b"new-data3"]

    @classmethod
    def setUpClass(cls):
        cls.client = Client(CLUSTER_ENDPOINT)
        cls.bucket = cls.client.bucket(cls.BUCKET_NAME)
        cls.bucket.create()
        cls.object = cls.bucket.object(cls.OBJECT_NAME)

    def tearDown(self):
        # Ensure the object does not exist before each test
        self.object.delete()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.bucket.delete(missing_ok=True)

    def test_write_mode_w_existing_object(self):
        """Test writing in 'w' mode to an existing object with multiple writes."""
        # Ensure the object exists with some content
        self.object.put_content(b"existing-content")

        # Open ObjectFileWriter in 'w' mode
        object_file_w = self.object.get_writer().as_file(mode="w")

        # The object should be truncated
        current_content = self.object.get().read_all()
        self.assertEqual(current_content, b"")

        # Write data
        for data_chunk in self.data_to_write:
            object_file_w.write(data_chunk)

            # Attempt to read the object before flushing
            current_content = self.object.get().read_all()
            self.assertEqual(current_content, b"")

        # Flush the data
        object_file_w.flush()

        # Now the data should be visible
        expected_content = b"".join(self.data_to_write)
        current_content = self.object.get().read_all()
        self.assertEqual(current_content, expected_content)

    def test_write_mode_w_non_existing_object(self):
        """Test writing in 'w' mode to a non-existing object with multiple writes."""
        # Open ObjectFileWriter in 'w' mode
        object_file_w = self.object.get_writer().as_file(mode="w")

        # The object should now exist but be empty
        current_content = self.object.get().read_all()
        self.assertEqual(current_content, b"")

        # Write data
        for data_chunk in self.data_to_write:
            object_file_w.write(data_chunk)

            # Attempt to read the object before flushing
            current_content = self.object.get().read_all()
            self.assertEqual(current_content, b"")

        # Flush the data
        object_file_w.flush()

        # Now the data should be visible
        expected_content = b"".join(self.data_to_write)
        current_content = self.object.get().read_all()
        self.assertEqual(current_content, expected_content)

    def test_write_mode_a_existing_object(self):
        """Test writing in 'a' mode to an existing object with multiple writes."""
        # Ensure the object exists with some content
        existing_content = b"existing-content"
        self.object.put_content(existing_content)

        # Open ObjectFileWriter in 'a' mode
        object_file_w = self.object.get_writer().as_file(mode="a")

        # The object should retain existing content
        current_content = self.object.get().read_all()
        self.assertEqual(current_content, existing_content)

        # Write data
        for data_chunk in self.data_to_write:
            object_file_w.write(data_chunk)

            # Attempt to read the object before flushing
            current_content = self.object.get().read_all()
            self.assertEqual(current_content, existing_content)

        # Flush the data
        object_file_w.flush()

        # Now the new data should be appended
        expected_content = existing_content + b"".join(self.data_to_write)
        current_content = self.object.get().read_all()
        self.assertEqual(current_content, expected_content)

    def test_write_mode_a_non_existing_object(self):
        """Test writing in 'a' mode to a non-existing object with multiple writes."""
        # Open ObjectFileWriter in 'a' mode
        object_file_w = self.object.get_writer().as_file(mode="a")

        # Attempting to read the object before flushing will raise an exception
        # as the object does not exist
        with self.assertRaises(Exception):
            self.object.get().read_all()

        # Write data
        for data_chunk in self.data_to_write:
            object_file_w.write(data_chunk)

            # Attempting to read the object before flushing will still raise an exception
            # as until flushed, the object does not exist
            with self.assertRaises(Exception):
                self.object.get().read_all()

        # Flush the data
        object_file_w.flush()

        # Now the data should be visible
        expected_content = b"".join(self.data_to_write)
        current_content = self.object.get().read_all()
        self.assertEqual(current_content, expected_content)
