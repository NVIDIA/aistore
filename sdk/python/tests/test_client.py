#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# Default provider is AIS, so all Cloud-related tests are skipped.
# To run all the test, provide full bucket name in the commad line, e.g.:
#    BUCKET=gcp://ais-bck python3 tests/test_client.py

import random
import string
import unittest
import os
import requests
import tempfile

from aistore.client.api import Client

CLUSTER_ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
REMOTE_BUCKET = os.environ.get("BUCKET", "")

OBJ_READ_TYPE_ALL = "read_all"
OBJ_READ_TYPE_CHUNK = "chunk"


class TestBasicOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        letters = string.ascii_lowercase
        self.bck_name = ''.join(random.choice(letters) for _ in range(10))

        self.client = Client(CLUSTER_ENDPOINT)

    def tearDown(self) -> None:
        # Try to destroy bucket if there is one left.
        try:
            self.client.destroy_bucket(self.bck_name)
        except requests.exceptions.HTTPError:
            pass

    def test_bucket(self):
        res = self.client.list_buckets()
        count = len(res)
        self.client.create_bucket(self.bck_name)
        res = self.client.list_buckets()
        count_new = len(res)
        self.assertEqual(count + 1, count_new)

    def test_head_bucket(self):
        self.client.create_bucket(self.bck_name)
        self.client.head_bucket(self.bck_name)
        self.client.destroy_bucket(self.bck_name)
        try:
            self.client.head_bucket(self.bck_name)
        except requests.exceptions.HTTPError as e:
            self.assertEqual(e.response.status_code, 404)

    def _test_get_obj(self, read_type, obj_name, exp_content):
        chunk_size = random.randrange(1, len(exp_content) + 10)
        stream = self.client.get_object(self.bck_name, obj_name, chunk_size=chunk_size)
        self.assertEqual(stream.content_length, len(exp_content))
        self.assertTrue(stream.e_tag != "")
        if read_type == OBJ_READ_TYPE_ALL:
            obj = stream.read_all()
        else:
            obj = b''
            for chunk in stream:
                obj += chunk
        self.assertEqual(obj, exp_content)

    def test_put_head_get(self):
        self.client.create_bucket(self.bck_name)
        num_objs = 10

        for i in range(num_objs):
            s = "test string" * random.randrange(1, 10)
            content = s.encode('utf-8')
            obj_name = f"obj{ i }"
            with tempfile.NamedTemporaryFile() as f:
                f.write(content)
                f.flush()
                self.client.put_object(self.bck_name, obj_name, f.name)

            properties = self.client.head_object(self.bck_name, obj_name)
            self.assertEqual(properties['ais-version'], '1')
            self.assertEqual(properties['content-length'], str(len(content)))
            for option in [OBJ_READ_TYPE_ALL, OBJ_READ_TYPE_CHUNK]:
                self._test_get_obj(option, obj_name, content)

    def test_cluster_map(self):
        smap = self.client.get_cluster_info()

        self.assertIsNotNone(smap)
        self.assertIsNotNone(smap.proxy_si)
        self.assertNotEqual(len(smap.pmap), 0)
        self.assertNotEqual(len(smap.tmap), 0)
        self.assertNotEqual(smap.version, 0)
        self.assertIsNot(smap.uuid, "")

    def test_list_object_page(self):
        bucket_size = 110
        tests = [
            {
                "page_size": None, "resp_size": bucket_size
            },
            {
                "page_size": 7, "resp_size": 7
            },
            {
                "page_size": bucket_size * 2, "resp_size": bucket_size
            },
        ]
        self.client.create_bucket(self.bck_name)
        content = "test".encode("utf-8")
        with tempfile.NamedTemporaryFile() as f:
            f.write(content)
            f.flush()
            for obj_id in range(bucket_size):
                self.client.put_object(self.bck_name, f"obj-{ obj_id }", f.name)
        for test in list(tests):
            resp = self.client.list_objects(self.bck_name, page_size=test["page_size"])
            self.assertEqual(len(resp.entries), test["resp_size"])

    def test_list_all_objects(self):
        bucket_size = 110
        short_page_len = 17
        self.client.create_bucket(self.bck_name)
        content = "test".encode("utf-8")
        with tempfile.NamedTemporaryFile() as f:
            f.write(content)
            f.flush()
            for obj_id in range(bucket_size):
                self.client.put_object(self.bck_name, f"obj-{ obj_id }", f.name)
        objects = self.client.list_all_objects(self.bck_name)
        self.assertEqual(len(objects), bucket_size)
        objects = self.client.list_all_objects(self.bck_name, page_size=short_page_len)
        self.assertEqual(len(objects), bucket_size)

    @unittest.skipIf(REMOTE_BUCKET == "" or REMOTE_BUCKET.startswith("ais:"), "Remote bucket is not set")
    def test_evict_bucket(self):
        obj_name = "evict_obj"
        parts = REMOTE_BUCKET.split("://")  # must be in format '<provider>://<bck>'
        self.assertTrue(len(parts) > 1)
        provider, self.bck_name = parts[0], parts[1]
        content = "test".encode("utf-8")
        with tempfile.NamedTemporaryFile() as f:
            f.write(content)
            f.flush()
            self.client.put_object(self.bck_name, obj_name, f.name, provider=provider)

        objects = self.client.list_objects(self.bck_name, provider=provider, props="name,cached", prefix=obj_name)
        self.assertTrue(len(objects) > 0)
        for obj in objects:
            if obj.name == obj_name:
                self.assertTrue(obj.is_ok())
                self.assertTrue(obj.is_cached())

        self.client.evict_bucket(self.bck_name, provider=provider)
        objects = self.client.list_objects(self.bck_name, provider=provider, props="name,cached", prefix=obj_name)
        self.assertTrue(len(objects) > 0)
        for obj in objects:
            if obj.name == obj_name:
                self.assertTrue(obj.is_ok())
                self.assertFalse(obj.is_cached())
        self.client.delete_object(self.bck_name, obj_name, provider=provider)

    def test_obj_delete(self):
        bucket_size = 10
        delete_cnt = 7
        self.client.create_bucket(self.bck_name)
        content = "test".encode("utf-8")
        with tempfile.NamedTemporaryFile() as f:
            f.write(content)
            f.flush()
            for obj_id in range(bucket_size):
                self.client.put_object(self.bck_name, f"obj-{ obj_id }", f.name)
        objects = self.client.list_objects(self.bck_name)
        self.assertEqual(len(objects.entries), bucket_size)

        for obj_id in range(delete_cnt):
            self.client.delete_object(self.bck_name, f"obj-{ obj_id + 1 }")
        objects = self.client.list_objects(self.bck_name)
        self.assertEqual(len(objects.entries), bucket_size - delete_cnt)


if __name__ == '__main__':
    unittest.main()
