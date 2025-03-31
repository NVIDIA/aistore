#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from io import BytesIO

from aistore.sdk import Bucket, Object
from aistore.sdk.client import Client
from aistore.sdk.const import DEFAULT_CHUNK_SIZE, AIS_CHECKSUM_VALUE
from tests.integration.sdk import DEFAULT_TEST_CLIENT
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
        cls.object_content = create_and_put_object(
            client=cls.client,
            bck_name=cls.bucket.name,
            obj_name=object_name,
            obj_size=cls.object_size,
        )
        cls.object = cls.bucket.object(object_name)
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
