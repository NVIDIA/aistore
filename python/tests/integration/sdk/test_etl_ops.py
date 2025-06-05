#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#

import os
import hashlib
import unittest
import pytest
import xxhash

from aistore.sdk import Bucket
from aistore.sdk.etl import ETLConfig
from aistore.sdk.errors import AISError
from aistore.sdk.etl.etl_templates import MD5, ECHO, HASH
from aistore.sdk.etl.etl_const import (
    ETL_COMM_HPUSH,
    ETL_COMM_HPULL,
    FASTAPI_CMD,
)
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer
from aistore.sdk.types import EnvVar

from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.utils import cases, create_and_put_object, random_string, has_targets


# pylint: disable=unused-variable
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

    def _calculate_xxhash(self, data, seed):
        hasher = xxhash.xxh64(seed=seed)
        hasher.update(data)
        return hasher.hexdigest()

    def _calculate_md5(self, data):
        return hashlib.md5(data).hexdigest().encode()

    @pytest.mark.etl
    def test_init_etl_class_echo(self):

        etl = self.client.etl(self.etl_name)

        @etl.init_class()
        class EchoServer(FastAPIServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return data

        obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=self.etl_name))
            .read_all()
        )
        self.assertEqual(obj, bytes(self.content))

    @pytest.mark.etl
    def test_init_etl_class_md5(self):

        etl = self.client.etl(self.etl_name)

        @etl.init_class()
        class MD5Server(FastAPIServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return hashlib.md5(data).hexdigest().encode()

        obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=self.etl_name))
            .read_all()
        )
        self.assertEqual(obj, self._calculate_md5(bytes(self.content)))

    @pytest.mark.etl
    def test_init_etl_class_xxhash_arg_type(self):

        etl = self.client.etl(self.etl_name)

        @etl.init_class(
            comm_type=ETL_COMM_HPULL, dependencies=["xxhash"], SEED_DEFAULT="500"
        )
        class XXHash(FastAPIServer):
            def __init__(self):
                super().__init__()
                try:
                    self.default_seed = int(os.getenv("SEED_DEFAULT", "0"))
                except ValueError:
                    self.logger.warning(
                        "Invalid SEED_DEFAULT='%s', falling back to 0",
                        os.getenv("SEED_DEFAULT"),
                    )
                    self.default_seed = 0

            def transform(
                self,
                data: bytes,
                _path: str,
                etl_args: str,
            ) -> bytes:
                seed = self.default_seed
                if etl_args:
                    try:
                        seed = int(etl_args)
                    except ValueError:
                        self.logger.warning(
                            "Invalid etl_args seed=%r, using default_seed=%d",
                            etl_args,
                            self.default_seed,
                        )
                hasher = xxhash.xxh64(seed=seed)
                hasher.update(data)
                return hasher.hexdigest().encode("ascii")

        default_hashed_obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=self.etl_name))
            .read_all()
        )
        # 500 because of SEED_DEFAULT in the class decorator
        self.assertEqual(
            default_hashed_obj,
            self._calculate_xxhash(bytes(self.content), 500).encode("ascii"),
        )
        etl_args_hashed_obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=self.etl_name, args="10000"))
            .read_all()
        )
        self.assertEqual(
            etl_args_hashed_obj,
            self._calculate_xxhash(bytes(self.content), 10000).encode("ascii"),
        )

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

        self.assertEqual(obj, self._calculate_md5(bytes(self.content)))

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

        md5_hpush_etl = self.client.etl(self.etl_name)

        @md5_hpush_etl.init_class()
        class MD5Server(FastAPIServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return hashlib.md5(data).hexdigest().encode()

        job_id = self.bucket.transform(
            etl_name=md5_hpush_etl.name, to_bck=Bucket("transformed-etl-hpush")
        )
        self.client.job(job_id).wait()

        for key, value in content.items():
            transformed_obj_hpush = (
                self.bucket.object(key)
                .get_reader(etl=ETLConfig(name=self.etl_name))
                .read_all()
            )
            self.assertEqual(self._calculate_md5(bytes(value)), transformed_obj_hpush)

    @pytest.mark.etl
    def test_etl_with_various_sizes(self):
        obj_sizes = [128, 1024, 1048576]

        for obj_size in obj_sizes:
            obj_name = f"obj-{obj_size}.jpg"
            _, content = create_and_put_object(
                client=self.client,
                bck=self.bucket.as_model(),
                obj_name=obj_name,
                obj_size=obj_size,
            )

            etl = self.client.etl(self.etl_name)

            @etl.init_class()
            class MD5Server(FastAPIServer):
                def transform(self, data: bytes, *_args) -> bytes:
                    return hashlib.md5(data).hexdigest().encode()

            obj = (
                self.bucket.object(obj_name)
                .get_reader(etl=ETLConfig(etl.name))
                .read_all()
            )
            self.assertEqual(obj, self._calculate_md5(bytes(content)))
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

        spec_etl_details = spec_etl.view()
        self.assertIsNotNone(spec_etl_details)
        self.assertEqual(spec_etl_details.init_msg.name, self.etl_name)
        self.assertIsNotNone(spec_etl_details.init_msg.spec)

        # Need to add this because of @cases decorator
        try:
            spec_etl.stop()
            spec_etl.delete()
        except AISError:
            # If the ETL was not initialized, it will raise an error
            pass

    @pytest.mark.etl
    @unittest.skipIf(not has_targets(2), "Test requires more than one target")
    def test_etl_concurrent_workers(self):
        dst_bck = self.client.bucket(random_string()).create()

        etl = self.client.etl(self.etl_name)

        @etl.init_class()
        class MD5Server(FastAPIServer):
            def __init__(self):
                super().__init__()
                self.md5_hash = hashlib.md5()

            def transform(self, data: bytes, *_args) -> bytes:
                self.md5_hash.update(data)
                return self.md5_hash.digest()

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

    @pytest.mark.etl
    def test_etl_init_hello_world(self):
        etl = self.client.etl(self.etl_name)

        etl.init(
            image="aistorage/transformer_hello_world:latest",
            command=FASTAPI_CMD,
        )
        obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=etl.name))
            .read_all()
        )
        self.assertEqual(
            obj, b"Hello World!", "ETL initialization with image and command failed"
        )
        etl_details = etl.view()
        self.assertIsNotNone(etl_details)
        self.assertEqual(etl_details.init_msg.name, self.etl_name)
        self.assertEqual(
            etl_details.init_msg.runtime.image,
            "aistorage/transformer_hello_world:latest",
        )
        self.assertEqual(etl_details.init_msg.runtime.command, FASTAPI_CMD)

    @pytest.mark.etl
    def test_etl_init_hash_with_args(self):
        etl = self.client.etl(self.etl_name)

        etl.init(
            image="aistorage/transformer_hash_with_args:latest",
            command=FASTAPI_CMD,
            SEED_DEFAULT=500,
        )
        obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=etl.name))
            .read_all()
        )

        # Function to calculate xxhash
        def calculate_xxhash(data, seed):
            hasher = xxhash.xxh64(seed=seed)
            hasher.update(data)
            return hasher.hexdigest()

        self.assertEqual(obj.decode(), calculate_xxhash(bytes(self.content), 500))

        # different seed
        seed = 10000
        new_obj = (
            self.bucket.object(self.obj_name)
            .get_reader(etl=ETLConfig(name=etl.name, args=seed))
            .read_all()
        )
        self.assertEqual(new_obj.decode(), calculate_xxhash(bytes(self.content), seed))

        etl_details = etl.view()
        self.assertIsNotNone(etl_details)
        self.assertEqual(etl_details.init_msg.name, self.etl_name)
        self.assertEqual(
            etl_details.init_msg.runtime.image,
            "aistorage/transformer_hash_with_args:latest",
        )
        self.assertEqual(etl_details.init_msg.runtime.command, FASTAPI_CMD)
        self.assertEqual(
            etl_details.init_msg.runtime.env[0],
            EnvVar(name="SEED_DEFAULT", value="500"),
        )

    @pytest.mark.etl
    def test_etl_context_manager_cleanup(self):

        with self.client.etl(self.etl_name) as etl:

            @etl.init_class()
            class EchoServer(FastAPIServer):
                def transform(self, data: bytes, *_args) -> bytes:
                    return data

            # Read the object through the ETL to ensure it's running
            obj = (
                self.bucket.object(self.obj_name)
                .get_reader(etl=ETLConfig(name=etl.name))
                .read_all()
            )
            self.assertEqual(obj, bytes(self.content))

        # After context exit, view() or read should fail
        with self.assertRaises(AISError):
            self.client.etl(self.etl_name).view()

    @pytest.mark.etl
    def test_etl_with_transform_errors(self):
        src_bck = self.client.bucket(random_string()).create()
        for i in range(9):
            src_bck.object(str(i)).get_writer().put_content(b"hello, world!")

        etl = self.client.etl(self.etl_name)

        @etl.init_class()
        class ETLWithTransformErrors(FastAPIServer):
            def transform(self, data: bytes, path: str, etl_args: str) -> bytes:
                if int(path[-1]) > 5:
                    raise ValueError("Skip processing for objects with path > 5")
                return data.upper()

        dst_bck = self.client.bucket(random_string()).create()

        job_id = src_bck.transform(
            etl_name=etl.name,
            to_bck=dst_bck,
            cont_on_err=True,  # Allow continuation despite errors
        )
        job = self.client.job(job_id)
        job.wait()

        etl_details = etl.view()
        self.assertIsNotNone(etl_details.obj_errors)
        error_names = sorted(e.obj_name for e in etl_details.obj_errors)
        expected_errors = sorted([f"{i}" for i in range(6, 9)])
        self.assertEqual(error_names, expected_errors)


if __name__ == "__main__":
    unittest.main()
