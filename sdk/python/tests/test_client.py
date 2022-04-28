#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

import random
import string
import unittest
import os
import requests

from aistore.client.api import Client

CLUSTER_ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")


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

    def test_put_get(self):
        self.client.create_bucket(self.bck_name)

        tmpfile = "/tmp/py-sdk-test"
        orig_cont = "test string"
        with open(tmpfile, mode="w", encoding="utf-8") as fdata:
            fdata.write(orig_cont)

        self.client.put_object(self.bck_name, "obj1", tmpfile)
        os.remove(tmpfile)

        objects = self.client.list_objects(self.bck_name)
        self.assertFalse(objects is None)

        obj = self.client.get_object(self.bck_name, "obj1")
        self.assertEqual(obj.decode("utf-8"), orig_cont)

    def test_cluster_map(self):
        smap = self.client.get_cluster_info()

        self.assertIsNotNone(smap)
        self.assertIsNotNone(smap.proxy_si)
        self.assertNotEqual(len(smap.pmap), 0)
        self.assertNotEqual(len(smap.tmap), 0)
        self.assertNotEqual(smap.version, 0)
        self.assertIsNot(smap.uuid, "")


if __name__ == '__main__':
    unittest.main()
