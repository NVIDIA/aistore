#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from io import BytesIO
from aistore.sdk.client import Client
from aistore.sdk.const import DEFAULT_CHUNK_SIZE, AIS_CHECKSUM_VALUE
from tests.utils import create_and_put_object
from tests.integration import CLUSTER_ENDPOINT


class TestObjectReaderOps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(CLUSTER_ENDPOINT)
        cls.bucket_name = "test-bucket"
        cls.bucket = cls.client.bucket(cls.bucket_name).create(exist_ok=True)
        cls.object_name = "test-object"

        cls.object_size = DEFAULT_CHUNK_SIZE * 2
        cls.object_content = create_and_put_object(
            client=cls.client,
            bck_name=cls.bucket_name,
            obj_name=cls.object_name,
            obj_size=cls.object_size,
        )
        cls.object_reader = cls.bucket.object(cls.object_name).get()

    @classmethod
    def tearDownClass(cls):
        cls.bucket.object(cls.object_name).delete()
        cls.bucket.delete()

    def test_head(self):
        attributes = self.object_reader.head()
        self.assertEqual(attributes.size, self.object_size)
        self.assertEqual(
            self.bucket.object(self.object_name).head()[AIS_CHECKSUM_VALUE],
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

    def test_iter_from_position(self):
        start_position = DEFAULT_CHUNK_SIZE
        chunks = list(self.object_reader.iter_from_position(start_position))

        combined_content = b"".join(chunks)
        expected_content = self.object_content[start_position:]
        self.assertEqual(combined_content, expected_content)

        self.assertEqual(len(chunks), 1)
