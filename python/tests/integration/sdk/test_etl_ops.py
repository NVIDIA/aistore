#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from itertools import cycle
import unittest
import hashlib
import sys
import time

from aistore.sdk import Client
from aistore.sdk.errors import AISError, ErrBckNotFound
from aistore.sdk.etl_templates import MD5, ECHO
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import create_and_put_object, random_string


# pylint: disable=unused-variable
class TestETLOps(unittest.TestCase):
    def setUp(self) -> None:
        self.bck_name = random_string()
        self.etl_id_code = "etl-" + random_string(5)
        self.etl_id_code_io = "etl-" + random_string(5)
        self.etl_id_code_stream = "etl-" + random_string(5)
        self.etl_id_spec = "etl-" + random_string(5)
        self.etl_id_spec_comp = "etl-" + random_string(5)
        print("URL END PT ", CLUSTER_ENDPOINT)
        self.client = Client(CLUSTER_ENDPOINT)

        self.client.bucket(bck_name=self.bck_name).create()
        self.obj_name = "temp-obj1.jpg"
        self.obj_size = 128
        self.content = create_and_put_object(
            client=self.client,
            bck_name=self.bck_name,
            obj_name=self.obj_name,
            obj_size=self.obj_size,
        )

        self.current_etl_count = len(self.client.etl().list())

    def tearDown(self) -> None:
        # Try to destroy all temporary buckets if there are left.
        try:
            for bucket in self.client.cluster().list_buckets():
                self.client.bucket(bucket.name).delete()
        except ErrBckNotFound:
            pass

        # delete all the etls
        for etl in self.client.etl().list():
            self.client.etl().stop(etl_name=etl.id)
            self.client.etl().delete(etl_name=etl.id)

    def test_etl_apis(self):

        # code
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        self.client.etl().init_code(transform=transform, etl_name=self.etl_id_code)

        obj = (
            self.client.bucket(self.bck_name)
            .object(self.obj_name)
            .get(etl_name=self.etl_id_code)
            .read_all()
        )
        self.assertEqual(obj, transform(bytes(self.content)))
        self.assertEqual(self.current_etl_count + 1, len(self.client.etl().list()))

        # code (io comm)
        def main():
            md5 = hashlib.md5()
            chunk = sys.stdin.buffer.read()
            md5.update(chunk)
            sys.stdout.buffer.write(md5.hexdigest().encode())

        self.client.etl().init_code(
            transform=main, etl_name=self.etl_id_code_io, communication_type="io"
        )

        obj_io = (
            self.client.bucket(self.bck_name)
            .object(self.obj_name)
            .get(etl_name=self.etl_id_code_io)
            .read_all()
        )
        self.assertEqual(obj_io, transform(bytes(self.content)))

        self.client.etl().stop(etl_name=self.etl_id_code_io)
        self.client.etl().delete(etl_name=self.etl_id_code_io)

        # spec
        template = MD5.format(communication_type="hpush")
        self.client.etl().init_spec(template=template, etl_name=self.etl_id_spec)

        obj = (
            self.client.bucket(self.bck_name)
            .object(self.obj_name)
            .get(etl_name=self.etl_id_spec)
            .read_all()
        )
        self.assertEqual(obj, transform(bytes(self.content)))

        self.assertEqual(self.current_etl_count + 2, len(self.client.etl().list()))

        self.assertIsNotNone(self.client.etl().view(etl_name=self.etl_id_code))
        self.assertIsNotNone(self.client.etl().view(etl_name=self.etl_id_spec))

        temp_bck1 = random_string()

        # Transform Bucket with MD5 Template
        job_id = self.client.bucket(self.bck_name).transform(
            etl_name=self.etl_id_spec, to_bck=temp_bck1
        )
        self.client.job().wait_for_job(job_id)

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
        self.client.etl().init_spec(template=template, etl_name=self.etl_id_spec_comp)

        temp_bck2 = random_string()

        # Transform bucket with ECHO template
        job_id = self.client.bucket(self.bck_name).transform(
            etl_name=self.etl_id_spec_comp,
            to_bck=temp_bck2,
            ext={"jpg": "txt"},
        )
        self.client.job().wait_for_job(job_id)

        # Verify extension rename
        for obj_iter in self.client.bucket(temp_bck2).list_objects().get_entries():
            self.assertEqual(obj_iter.name.split(".")[1], "txt")

        echo_obj = (
            self.client.bucket(temp_bck2).object("temp-obj1.txt").get().read_all()
        )

        # Verify different bucket-level transformations are not the same (compare ECHO transformation and MD5 transformation)
        self.assertNotEqual(md5_obj, echo_obj)

        self.client.etl().stop(etl_name=self.etl_id_spec_comp)
        self.client.etl().delete(etl_name=self.etl_id_spec_comp)

        # Transform w/ non-existent ETL ID raises exception
        with self.assertRaises(AISError):
            self.client.bucket(self.bck_name).transform(
                etl_name="faulty-name", to_bck=random_string()
            )

        # Stop ETLs
        self.client.etl().stop(etl_name=self.etl_id_code)
        self.client.etl().stop(etl_name=self.etl_id_spec)
        self.assertEqual(len(self.client.etl().list()), self.current_etl_count)

        # Start stopped ETLs
        self.client.etl().start(etl_name=self.etl_id_code)
        self.client.etl().start(etl_name=self.etl_id_spec)
        self.assertEqual(len(self.client.etl().list()), self.current_etl_count + 2)

        # Delete stopped ETLs
        self.client.etl().stop(etl_name=self.etl_id_code)
        self.client.etl().stop(etl_name=self.etl_id_spec)
        self.client.etl().delete(etl_name=self.etl_id_code)
        self.client.etl().delete(etl_name=self.etl_id_spec)

        # Starting deleted ETLs raises error
        with self.assertRaises(AISError):
            self.client.etl().start(etl_name=self.etl_id_code)
        with self.assertRaises(AISError):
            self.client.etl().start(etl_name=self.etl_id_spec)

    def test_etl_apis_stress(self):
        num_objs = 200
        content = {}
        for i in range(num_objs):
            obj_name = f"obj{ i }"
            content[obj_name] = create_and_put_object(
                client=self.client, bck_name=self.bck_name, obj_name=obj_name
            )

        # code (hpush)
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        self.client.etl().init_code(transform=transform, etl_name=self.etl_id_code)

        # code (io comm)
        def main():
            md5 = hashlib.md5()
            chunk = sys.stdin.buffer.read()
            md5.update(chunk)
            sys.stdout.buffer.write(md5.hexdigest().encode())

        self.client.etl().init_code(
            transform=main, etl_name=self.etl_id_code_io, communication_type="io"
        )

        start_time = time.time()
        job_id = self.client.bucket(self.bck_name).transform(
            etl_name=self.etl_id_code, to_bck="transformed-etl-hpush"
        )
        self.client.job().wait_for_job(job_id)
        print("Transform bucket using HPUSH took ", time.time() - start_time)

        start_time = time.time()
        job_id = self.client.bucket(self.bck_name).transform(
            etl_name=self.etl_id_code_io, to_bck="transformed-etl-io"
        )
        self.client.job().wait_for_job(job_id)
        print("Transform bucket using IO took ", time.time() - start_time)

        for key, value in content.items():
            transformed_obj_hpush = (
                self.client.bucket(self.bck_name)
                .object(key)
                .get(etl_name=self.etl_id_code)
                .read_all()
            )
            transformed_obj_io = (
                self.client.bucket(self.bck_name)
                .object(key)
                .get(etl_name=self.etl_id_code_io)
                .read_all()
            )

            self.assertEqual(transform(bytes(value)), transformed_obj_hpush)
            self.assertEqual(transform(bytes(value)), transformed_obj_io)

    def test_etl_apis_stream(self):
        def transform(reader, writer):
            checksum = hashlib.md5()
            for b in reader:
                checksum.update(b)
            writer.write(checksum.hexdigest().encode())

        self.client.etl().init_code(
            transform=transform,
            etl_name=self.etl_id_code_stream,
            chunk_size=32768,
        )

        obj = (
            self.client.bucket(self.bck_name)
            .object(self.obj_name)
            .get(etl_name=self.etl_id_code_stream)
            .read_all()
        )
        md5 = hashlib.md5()
        md5.update(self.content)
        self.assertEqual(obj, md5.hexdigest().encode())

    def test_etl_api_xor(self):
        def transform(reader, writer):
            checksum = hashlib.md5()
            key = b"AISTORE"
            for b in reader:
                out = bytes([_a ^ _b for _a, _b in zip(b, cycle(key))])
                writer.write(out)
                checksum.update(out)
            writer.write(checksum.hexdigest().encode())

        self.client.etl().init_code(
            transform=transform, etl_name="etl-xor1", chunk_size=32
        )
        transformed_obj = (
            self.client.bucket(self.bck_name)
            .object(self.obj_name)
            .get(etl_name="etl-xor1")
            .read_all()
        )
        data, checksum = transformed_obj[:-32], transformed_obj[-32:]
        computed_checksum = hashlib.md5(data).hexdigest().encode()
        self.assertEqual(checksum, computed_checksum)


if __name__ == "__main__":
    unittest.main()
