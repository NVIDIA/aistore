import unittest
import os

from aistore.client.api import Client

CLUSTER_ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")


class TestBasicOps(unittest.TestCase):  #pylint: disable=unused-variable
    def test_bucket(self):
        client = Client(CLUSTER_ENDPOINT)
        bck_name = "test"

        res = client.list_buckets()
        count = len(res)
        client.create_bucket(bck_name)
        res = client.list_buckets()
        count_new = len(res)
        self.assertEqual(count + 1, count_new)

        client.destroy_bucket(bck_name)

    def test_put_get(self):
        client = Client(CLUSTER_ENDPOINT)
        bck_name = "test"
        client.create_bucket(bck_name)

        tmpfile = "/tmp/py-sdk-test"
        orig_cont = "test string"
        with open(tmpfile, mode="w", encoding="utf-8") as fdata:
            fdata.write(orig_cont)

        client.put_object(bck_name, "obj1", tmpfile)
        os.remove(tmpfile)

        objects = client.list_objects(bck_name)
        self.assertFalse(objects is None)

        obj = client.get_object(bck_name, "obj1")
        self.assertEqual(obj.decode("utf-8"), orig_cont)

        client.destroy_bucket(bck_name)

    def test_cluster_map(self):
        client = Client(CLUSTER_ENDPOINT)
        smap = client.get_cluster_info()

        self.assertIsNotNone(smap)
        self.assertIsNotNone(smap.proxy_si)
        self.assertNotEqual(len(smap.pmap), 0)
        self.assertNotEqual(len(smap.tmap), 0)
        self.assertNotEqual(smap.version, 0)
        self.assertIsNot(smap.uuid, "")


if __name__ == '__main__':
    unittest.main()
