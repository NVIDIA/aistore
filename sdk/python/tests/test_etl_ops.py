#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

import unittest
import hashlib

from aistore.client import Client
from aistore.client.errors import ErrBckNotFound
from aistore.client.etl_templates import MD5
from tests import CLUSTER_ENDPOINT
from tests.utils import create_and_put_object, random_name


class TestETLOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.bck_name = random_name()
        self.etl_id_code = "etl-" + random_name(5)
        self.etl_id_spec = "etl-" + random_name(5)
        print("URL END PT ", CLUSTER_ENDPOINT)
        self.client = Client(CLUSTER_ENDPOINT)

        self.client.bucket(bck_name=self.bck_name).create()
        self.obj_name = "temp-obj1"
        self.content = create_and_put_object(
            client=self.client, bck_name=self.bck_name, obj_name=self.obj_name
        )

        self.current_etl_count = len(self.client.etl().list_etls())

    def tearDown(self) -> None:
        # Try to destroy all temporary buckets if there are left.
        try:
            self.client.bucket(self.bck_name).delete()
        except ErrBckNotFound:
            pass

        # delete all the etls
        for etl in self.client.etl().list_etls():
            self.client.etl().stop(etl_id=etl.id)

    def test_etl_apis(self):

        # code
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        self.client.etl().init_code(code=transform, etl_id=self.etl_id_code)

        obj = (
            self.client.bucket(self.bck_name)
            .object(self.obj_name)
            .get(etl_id=self.etl_id_code)
            .read_all()
        )
        self.assertEqual(obj, transform(bytes(self.content)))
        self.assertEqual(self.current_etl_count + 1, len(self.client.etl().list_etls()))

        # spec
        template = MD5.format(communication_type="hpush")
        self.client.etl().init_spec(template=template, etl_id=self.etl_id_spec)

        obj = (
            self.client.bucket(self.bck_name)
            .object(self.obj_name)
            .get(etl_id=self.etl_id_spec)
            .read_all()
        )
        self.assertEqual(obj, transform(bytes(self.content)))

        self.assertEqual(self.current_etl_count + 2, len(self.client.etl().list_etls()))

        self.assertIsNotNone(self.client.etl().view(etl_id=self.etl_id_code))
        self.assertIsNotNone(self.client.etl().view(etl_id=self.etl_id_spec))

        self.client.etl().stop(etl_id=self.etl_id_code)
        self.client.etl().stop(etl_id=self.etl_id_spec)
        self.assertEqual(len(self.client.etl().list_etls()), self.current_etl_count)


if __name__ == "__main__":
    unittest.main()
