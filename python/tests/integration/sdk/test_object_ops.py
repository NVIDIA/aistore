#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# Default provider is AIS, so all Cloud-related tests are skipped.

import random
import unittest
from aistore.sdk.errors import AISError, ErrBckNotFound

from aistore.sdk import Client
from tests.utils import create_and_put_object, random_string
from tests.integration import CLUSTER_ENDPOINT

OBJ_READ_TYPE_ALL = "read_all"
OBJ_READ_TYPE_CHUNK = "chunk"


class TestObjectOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.bck_name = random_string()

        self.client = Client(CLUSTER_ENDPOINT)

    def tearDown(self) -> None:
        # Try to destroy bucket if there is one left.
        try:
            self.client.bucket(self.bck_name).delete()
        except ErrBckNotFound:
            pass

    def _test_get_obj(self, read_type, obj_name, exp_content):
        chunk_size = random.randrange(1, len(exp_content) + 10)
        stream = (
            self.client.bucket(self.bck_name)
            .object(obj_name)
            .get(chunk_size=chunk_size)
        )
        self.assertEqual(stream.content_length, len(exp_content))
        self.assertTrue(stream.e_tag != "")
        if read_type == OBJ_READ_TYPE_ALL:
            obj = stream.read_all()
        else:
            obj = b""
            for chunk in stream:
                obj += chunk
        self.assertEqual(obj, exp_content)

    def test_put_head_get(self):
        self.client.bucket(self.bck_name).create()
        num_objs = 10

        for i in range(num_objs):
            obj_name = f"obj{ i }"
            content = create_and_put_object(
                client=self.client, bck_name=self.bck_name, obj_name=obj_name
            )
            properties = self.client.bucket(self.bck_name).object(obj_name).head()
            self.assertEqual(properties["ais-version"], "1")
            self.assertEqual(properties["content-length"], str(len(content)))
            for option in [OBJ_READ_TYPE_ALL, OBJ_READ_TYPE_CHUNK]:
                self._test_get_obj(option, obj_name, content)

    def test_list_object_page(self):
        bucket_size = 110
        tests = [
            {"page_size": None, "resp_size": bucket_size},
            {"page_size": 7, "resp_size": 7},
            {"page_size": bucket_size * 2, "resp_size": bucket_size},
        ]
        self.client.bucket(self.bck_name).create()
        for obj_id in range(bucket_size):
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"obj-{ obj_id }"
            )

        for test in list(tests):
            resp = self.client.bucket(self.bck_name).list_objects(
                page_size=test["page_size"]
            )
            self.assertEqual(len(resp.get_entries()), test["resp_size"])

    def test_list_all_objects(self):
        bucket_size = 110
        short_page_len = 17
        self.client.bucket(self.bck_name).create()
        for obj_id in range(bucket_size):
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"obj-{ obj_id }"
            )
        objects = self.client.bucket(self.bck_name).list_all_objects()
        self.assertEqual(len(objects), bucket_size)
        objects = self.client.bucket(self.bck_name).list_all_objects(
            page_size=short_page_len
        )
        self.assertEqual(len(objects), bucket_size)

    def test_list_object_iter(self):
        bucket_size = 110
        self.client.bucket(self.bck_name).create()
        objects = {}
        for obj_id in range(bucket_size):
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"obj-{ obj_id }"
            )
            objects[f"obj-{ obj_id }"] = 1

        # Read all `bucket_size` objects by prefix.
        obj_iter = self.client.bucket(self.bck_name).list_objects_iter(
            page_size=15, prefix="obj-"
        )
        for obj in obj_iter:
            del objects[obj.name]
        self.assertEqual(len(objects), 0)

        # Empty iterator if there are no objects matching the prefix.
        obj_iter = self.client.bucket(self.bck_name).list_objects_iter(
            prefix="invalid-obj-"
        )
        for obj in obj_iter:
            objects[obj.name] = 1
        self.assertEqual(len(objects), 0)

    def test_obj_delete(self):
        bucket_size = 10
        delete_cnt = 7
        self.client.bucket(self.bck_name).create()

        for obj_id in range(bucket_size):
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"obj-{ obj_id }"
            )

        objects = self.client.bucket(self.bck_name).list_objects()
        self.assertEqual(len(objects.get_entries()), bucket_size)

        for obj_id in range(delete_cnt):
            self.client.bucket(self.bck_name).object(f"obj-{ obj_id + 1 }").delete()
        objects = self.client.bucket(self.bck_name).list_objects()
        self.assertEqual(len(objects.get_entries()), bucket_size - delete_cnt)

    def test_empty_bucket(self):
        self.client.bucket(self.bck_name).create()
        objects = self.client.bucket(self.bck_name).list_objects()
        self.assertEqual(len(objects.get_entries()), 0)

    def test_bucket_with_no_matching_prefix(self):
        bucket_size = 10
        self.client.bucket(self.bck_name).create()
        objects = self.client.bucket(self.bck_name).list_objects()
        self.assertEqual(len(objects.get_entries()), 0)
        for obj_id in range(bucket_size):
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"obj-{ obj_id }"
            )

        objects = self.client.bucket(self.bck_name).list_objects(prefix="TEMP")
        self.assertEqual(len(objects.get_entries()), 0)

    def test_invalid_bck_name(self):
        with self.assertRaises(ErrBckNotFound):
            self.client.bucket("INVALID_BCK_NAME").list_objects()

    def test_invalid_bck_name_for_aws(self):
        with self.assertRaises(AISError):
            self.client.bucket("INVALID_BCK_NAME", "aws").list_objects()


if __name__ == "__main__":
    unittest.main()
