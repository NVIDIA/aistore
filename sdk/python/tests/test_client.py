import unittest
import os

from aistore.client.api import Client


class TestBasicOps(unittest.TestCase):  #pylint: disable=unused-variable
    def test_bucket(self):
        client = Client('http://localhost:8080')
        bck_name = "test"

        res = client.list_buckets()
        count = len(res)
        res = client.create_bucket(bck_name)
        self.assertEqual(res.status_code, 200)
        res = client.list_buckets()
        count_new = len(res)
        self.assertEqual(count + 1, count_new)

        res = client.destroy_bucket(bck_name)
        self.assertEqual(res.status_code, 200)

    def test_put_get(self):
        client = Client('http://localhost:8080')
        bck_name = "test"

        res = client.list_buckets()
        res = client.create_bucket(bck_name)
        self.assertEqual(res.status_code, 200)

        tmpfile = "/tmp/py-sdk-test"
        orig_cont = "test string"
        with open(tmpfile, mode="w", encoding="utf-8") as fdata:
            fdata.write(orig_cont)

        res = client.put_object(bck_name, "obj1", tmpfile)
        os.remove(tmpfile)
        self.assertEqual(res.status_code, 200)
        res.close()

        objects = client.list_objects(bck_name)
        self.assertFalse(objects is None)

        res = client.get_object(bck_name, "obj1")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.content.decode("utf-8"), orig_cont)
        res.close()

        res = client.destroy_bucket(bck_name)
        self.assertEqual(res.status_code, 200)

    def test_cluster_map(self):
        client = Client('http://localhost:8080')
        res = client.get_cluster_info()
        self.assertEqual(res.status_code, 200)


if __name__ == '__main__':
    unittest.main()
