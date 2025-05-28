#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#

import hashlib
import time
import unittest
from itertools import cycle

import pytest
import xxhash

from aistore.sdk import Bucket
from aistore.sdk.etl import ETLConfig
from aistore.sdk.errors import AISError
from aistore.sdk.etl.etl_templates import MD5, ECHO, HASH
from aistore.sdk.etl.etl_const import (
    ETL_COMM_HPUSH,
    ETL_COMM_HPULL,
)
from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.utils import cases, create_and_put_object, random_string, has_targets


class TestETLOps(unittest.TestCase):
    def setUp(self) -> None:
        self.client = DEFAULT_TEST_CLIENT

        self.bucket = self.client.bucket(bck_name=random_string()).create()
        self.obj_name = "temp-obj1.jpg"
        self.obj_size = 128
        _, self.content = create_and_put_object(
            client=self.client,
            bck=self.bucket.as_model(),
            obj_name=self.obj_name,
            obj_size=self.obj_size,
        )
        create_and_put_object(
            client=self.client, bck=self.bucket.as_model(), obj_name="obj2.jpg"
        )

        self.etl_name = "etl-" + random_string(5)

    def tearDown(self) -> None:
        # Try to delete the bucket
        self.bucket.delete(missing_ok=True)

        # Try to delete the intialized ETLs
        try:
            self.client.etl(self.etl_name).stop()
            self.client.etl(self.etl_name).delete()
        except AISError:
            # If the ETL was not initialized, it will raise an error
            pass

    @pytest.mark.etl
    def test_init_code(self):
        # code
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        code_etl = self.client.etl(self.etl_name)
        code_etl.init_code(transform=transform)

        obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=code_etl.name))
            .read_all()
        )
        self.assertEqual(obj, transform(bytes(self.content)))

    @pytest.mark.etl
    def test_init_spec_md5(self):
        # spec
        template = MD5.format(communication_type=ETL_COMM_HPUSH)
        spec_etl = self.client.etl(self.etl_name)
        spec_etl.init_spec(template=template)

        obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=spec_etl.name))
            .read_all()
        )
        md5 = hashlib.md5()
        md5.update(bytes(self.content))
        self.assertEqual(obj, md5.hexdigest().encode())

        self.assertIsNotNone(spec_etl.view())

        temp_bck1 = self.client.bucket(random_string()).create()

        # Transform Bucket with MD5 Template
        job_id = self.bucket.transform(etl_name=spec_etl.name, to_bck=temp_bck1)
        self.client.job(job_id).wait()

        starting_obj = self.bucket.list_objects().entries
        transformed_obj = temp_bck1.list_objects().entries

        self.assertEqual(len(starting_obj), len(transformed_obj))

        md5_obj = temp_bck1.object(self.obj_name).get_reader().read_all()
        temp_bck1.delete(missing_ok=True)
        # Verify bucket-level transformation and object-level transformation are the same
        self.assertEqual(obj, md5_obj)

    @pytest.mark.etl
    def test_init_spec_echo(self):
        # Start ETL with ECHO template
        template = ECHO.format(communication_type=ETL_COMM_HPUSH)
        echo_spec_etl = self.client.etl(self.etl_name)
        echo_spec_etl.init_spec(template=template)

        temp_bck2 = self.client.bucket(random_string()).create()

        # Transform bucket with ECHO template
        job_id = self.bucket.transform(
            etl_name=self.etl_name,
            to_bck=temp_bck2,
            ext={"jpg": "txt"},
        )
        self.client.job(job_id).wait()

        # Verify extension rename
        for obj_iter in temp_bck2.list_objects().entries:
            self.assertEqual(obj_iter.name.split(".")[1], "txt")

        echo_obj = temp_bck2.object("temp-obj1.txt").get_reader().read_all()
        original_obj = self.bucket.object(self.obj_name).get_reader().read_all()

        self.assertEqual(echo_obj, original_obj, "Echo transformation failed")

        # Transform w/ non-existent ETL name raises exception
        with self.assertRaises(AISError):
            self.bucket.transform(
                etl_name="faulty-name", to_bck=Bucket(random_string())
            )

        temp_bck2.delete(missing_ok=True)

    @pytest.mark.etl
    def test_etl_apis_stress(self):
        num_objs = 200
        content = {}
        for i in range(num_objs):
            obj_name = f"obj{ i }"
            _, content[obj_name] = create_and_put_object(
                client=self.client, bck=self.bucket.as_model(), obj_name=obj_name
            )

        # code (hpush)
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        md5_hpush_etl = self.client.etl(self.etl_name)
        md5_hpush_etl.init_code(transform=transform)

        start_time = time.time()
        job_id = self.bucket.transform(
            etl_name=md5_hpush_etl.name, to_bck=Bucket("transformed-etl-hpush")
        )
        self.client.job(job_id).wait()
        print("Transform bucket using HPUSH took ", time.time() - start_time)

        for key, value in content.items():
            transformed_obj_hpush = (
                self.bucket.object(key)
                .get_reader(etl=ETLConfig(name=md5_hpush_etl.name))
                .read_all()
            )
            self.assertEqual(transform(bytes(value)), transformed_obj_hpush)

    @pytest.mark.etl
    def test_etl_api_xor(self):
        def transform(reader, writer):
            checksum = hashlib.md5()
            key = b"AISTORE"
            for byte in reader:
                out = bytes([_a ^ _b for _a, _b in zip(byte, cycle(key))])
                writer.write(out)
                checksum.update(out)
            writer.write(checksum.hexdigest().encode())

        xor_etl = self.client.etl(self.etl_name)
        xor_etl.init_code(transform=transform, chunk_size=32)
        transformed_obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(xor_etl.name))
            .read_all()
        )
        data, checksum = transformed_obj[:-32], transformed_obj[-32:]
        computed_checksum = hashlib.md5(data).hexdigest().encode()
        self.assertEqual(checksum, computed_checksum)

    @pytest.mark.etl
    def test_etl_with_various_sizes(self):
        obj_sizes = [128, 1024, 1048576]

        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        for obj_size in obj_sizes:
            obj_name = f"obj-{obj_size}.jpg"
            _, content = create_and_put_object(
                client=self.client,
                bck=self.bucket.as_model(),
                obj_name=obj_name,
                obj_size=obj_size,
            )

            etl = self.client.etl(self.etl_name)
            etl.init_code(transform=transform)

            obj = (
                self.bucket.object(obj_name)
                .get_reader(etl=ETLConfig(etl.name))
                .read_all()
            )
            self.assertEqual(obj, transform(bytes(content)))
            try:
                etl.stop()
                etl.delete()
            except AISError:
                # If the ETL was not initialized, it will raise an error
                pass

    @pytest.mark.etl
    @cases(ETL_COMM_HPUSH, ETL_COMM_HPULL)
    def test_etl_args(self, communication_type):
        """
        Test ETL with different communication types: HPUSH, HPULL.
        """
        template = HASH.format(communication_type=communication_type)
        spec_etl = self.client.etl(self.etl_name)
        spec_etl.init_spec(template=template)

        # Function to calculate xxhash
        def calculate_xxhash(data, seed):
            hasher = xxhash.xxh64(seed=seed)
            hasher.update(data)
            return hasher.hexdigest()

        # Default hash (seed = 0)
        default_hash = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=spec_etl.name))
            .read_all()
        )
        self.assertEqual(
            default_hash.decode(), calculate_xxhash(bytes(self.content), 0)
        )

        # Hash with seed = 10000
        seed = 10000
        new_hash = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=spec_etl.name, args=seed))
            .read_all()
        )
        self.assertEqual(new_hash.decode(), calculate_xxhash(bytes(self.content), seed))

        # Ensure hashes are different
        self.assertNotEqual(default_hash, new_hash)

        try:
            spec_etl.stop()
            spec_etl.delete()
        except AISError:
            # If the ETL was not initialized, it will raise an error
            pass

    @pytest.mark.etl
    @unittest.skipIf(not has_targets(2), "Test requires more than one target")
    def test_etl_concurrent_workers(self):
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        dst_bck = self.client.bucket(random_string()).create()

        etl = self.client.etl(self.etl_name)
        etl.init_code(transform=transform)

        num_workers = 10
        job_id = self.bucket.transform(
            etl_name=etl.name,
            to_bck=dst_bck,
            num_workers=num_workers,
        )

        job = self.client.job(job_id)
        job.wait()
        self.assertEqual(num_workers, job.get_details().get_num_workers())

        self.assertEqual(2, len(dst_bck.list_all_objects()))


if __name__ == "__main__":
    unittest.main()
