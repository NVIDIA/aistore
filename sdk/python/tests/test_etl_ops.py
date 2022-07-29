#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

import unittest
import hashlib

from aistore.client import Client
from aistore.client.errors import AISError, ErrBckNotFound
from aistore.client.etl_templates import MD5, ECHO
from tests import CLUSTER_ENDPOINT
from tests.utils import create_and_put_object, random_name


class TestETLOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.bck_name = random_name()
        self.etl_id_code = "etl-" + random_name(5)
        self.etl_id_spec = "etl-" + random_name(5)
        self.etl_id_spec_comp = "etl-" + random_name(5)
        print("URL END PT ", CLUSTER_ENDPOINT)
        self.client = Client(CLUSTER_ENDPOINT)

        self.client.bucket(bck_name=self.bck_name).create()
        self.obj_name = "temp-obj1"
        self.content = create_and_put_object(
            client=self.client, bck_name=self.bck_name, obj_name=self.obj_name
        )

        self.current_etl_count = len(self.client.etl().list())

    def tearDown(self) -> None:
        # Try to destroy all temporary buckets if there are left.
        try:
            self.client.bucket(self.bck_name).delete()
        except ErrBckNotFound:
            pass

        # delete all the etls
        for etl in self.client.etl().list():
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
        self.assertEqual(self.current_etl_count + 1, len(self.client.etl().list()))

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

        self.assertEqual(self.current_etl_count + 2, len(self.client.etl().list()))

        self.assertIsNotNone(self.client.etl().view(etl_id=self.etl_id_code))
        self.assertIsNotNone(self.client.etl().view(etl_id=self.etl_id_spec))

        temp_bck1 = random_name()

        # Transform Bucket with MD5 Template
        self.client.bucket(self.bck_name).transform(
            etl_id=self.etl_id_spec, to_bck=temp_bck1
        )

        # Verify object counts of the original and transformed bucket are the same
        self.assertEqual(
            len(self.client.bucket(self.bck_name).list_objects().get_entries()),
            len(self.client.bucket(temp_bck1).list_objects().get_entries()),
        )

        md5_obj = self.client.bucket(temp_bck1).object(self.obj_name).get().read_all()

        # Verify bucket-level transformation and object-level transformation are the same
        self.assertEqual(obj, md5_obj)

        # Start ETL with ECHO template
        template = ECHO.format(communication_type="hpush")
        self.client.etl().init_spec(template=template, etl_id=self.etl_id_spec_comp)

        temp_bck2 = random_name()

        # Transform bucket with ECHO template
        self.client.bucket(self.bck_name).transform(
            etl_id=self.etl_id_spec_comp, to_bck=temp_bck2, force=True
        )

        echo_obj = self.client.bucket(temp_bck2).object(self.obj_name).get().read_all()

        # Verify different bucket-level transformations are not the same (compare ECHO transformation and MD5 transformation)
        self.assertNotEqual(md5_obj, echo_obj)

        self.client.etl().stop(etl_id=self.etl_id_spec_comp)
        self.client.etl().delete(etl_id=self.etl_id_spec_comp)

        # Transform w/ non-existent ETL ID raises exception
        with self.assertRaises(AISError):
            self.client.bucket(self.bck_name).transform(
                etl_id="faulty-name", to_bck=random_name()
            )

        # Stop ETLs
        self.client.etl().stop(etl_id=self.etl_id_code)
        self.client.etl().stop(etl_id=self.etl_id_spec)
        self.assertEqual(len(self.client.etl().list()), self.current_etl_count)

        # Start stopped ETLs
        self.client.etl().start(etl_id=self.etl_id_code)
        self.client.etl().start(etl_id=self.etl_id_spec)
        self.assertEqual(len(self.client.etl().list()), self.current_etl_count + 2)

        self.client.etl().stop(etl_id=self.etl_id_code)
        self.client.etl().stop(etl_id=self.etl_id_spec)

        # Delete stopped ETLs
        self.client.etl().delete(etl_id=self.etl_id_code)
        self.client.etl().delete(etl_id=self.etl_id_spec)

        # Starting deleted ETLs raises error
        with self.assertRaises(AISError):
            self.client.etl().start(etl_id=self.etl_id_code)
        with self.assertRaises(AISError):
            self.client.etl().start(etl_id=self.etl_id_spec)


if __name__ == "__main__":
    unittest.main()
