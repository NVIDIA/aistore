#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from itertools import cycle
import unittest
import hashlib
import sys
import time

import pytest

from aistore.sdk import Client, Bucket
from aistore.sdk.etl.etl_const import ETL_COMM_HPUSH, ETL_COMM_IO
from aistore.sdk.errors import AISError
from aistore.sdk.etl.etl_templates import MD5, ECHO
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import create_and_put_object, random_string

ETL_NAME_CODE = "etl-" + random_string(5)
ETL_NAME_CODE_IO = "etl-" + random_string(5)
ETL_NAME_CODE_STREAM = "etl-" + random_string(5)
ETL_NAME_SPEC = "etl-" + random_string(5)
ETL_NAME_SPEC_COMP = "etl-" + random_string(5)


# pylint: disable=unused-variable
class TestETLOps(unittest.TestCase):
    def setUp(self) -> None:
        self.bck_name = random_string()
        print("URL END PT ", CLUSTER_ENDPOINT)
        self.client = Client(CLUSTER_ENDPOINT)

        self.bucket = self.client.bucket(bck_name=self.bck_name).create()
        self.obj_name = "temp-obj1.jpg"
        self.obj_size = 128
        self.content = create_and_put_object(
            client=self.client,
            bck_name=self.bck_name,
            obj_name=self.obj_name,
            obj_size=self.obj_size,
        )
        create_and_put_object(
            client=self.client, bck_name=self.bck_name, obj_name="obj2.jpg"
        )

        self.current_etl_count = len(self.client.cluster().list_running_etls())

    def tearDown(self) -> None:
        # Try to destroy all temporary buckets if there are left.
        for bucket in self.client.cluster().list_buckets():
            self.client.bucket(bucket.name).delete(missing_ok=True)

        # delete all the etls
        for etl in self.client.cluster().list_running_etls():
            self.client.etl(etl.id).stop()
            self.client.etl(etl.id).delete()

    # pylint: disable=too-many-statements,too-many-locals
    @pytest.mark.etl
    def test_etl_apis(self):
        # code
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        code_etl = self.client.etl(ETL_NAME_CODE)
        code_etl.init_code(transform=transform)

        obj = self.bucket.object(self.obj_name).get(etl_name=code_etl.name).read_all()
        self.assertEqual(obj, transform(bytes(self.content)))
        self.assertEqual(
            self.current_etl_count + 1, len(self.client.cluster().list_running_etls())
        )

        # code (io comm)
        def main():
            md5 = hashlib.md5()
            chunk = sys.stdin.buffer.read()
            md5.update(chunk)
            sys.stdout.buffer.write(md5.hexdigest().encode())

        code_io_etl = self.client.etl(ETL_NAME_CODE_IO)
        code_io_etl.init_code(transform=main, communication_type=ETL_COMM_IO)

        obj_io = (
            self.bucket.object(self.obj_name).get(etl_name=code_io_etl.name).read_all()
        )
        self.assertEqual(obj_io, transform(bytes(self.content)))

        code_io_etl.stop()
        code_io_etl.delete()

        # spec
        template = MD5.format(communication_type=ETL_COMM_HPUSH)
        spec_etl = self.client.etl(ETL_NAME_SPEC)
        spec_etl.init_spec(template=template)

        obj = self.bucket.object(self.obj_name).get(etl_name=spec_etl.name).read_all()
        self.assertEqual(obj, transform(bytes(self.content)))

        self.assertEqual(
            self.current_etl_count + 2, len(self.client.cluster().list_running_etls())
        )

        self.assertIsNotNone(code_etl.view())
        self.assertIsNotNone(spec_etl.view())

        temp_bck1 = self.client.bucket(random_string()).create()

        # Transform Bucket with MD5 Template
        job_id = self.bucket.transform(
            etl_name=spec_etl.name, to_bck=temp_bck1, prefix_filter="temp-"
        )
        self.client.job(job_id).wait()

        starting_obj = self.bucket.list_objects().entries
        transformed_obj = temp_bck1.list_objects().entries
        # Should transform only the object defined by the prefix filter
        self.assertEqual(len(starting_obj) - 1, len(transformed_obj))

        md5_obj = temp_bck1.object(self.obj_name).get().read_all()

        # Verify bucket-level transformation and object-level transformation are the same
        self.assertEqual(obj, md5_obj)

        # Start ETL with ECHO template
        template = ECHO.format(communication_type=ETL_COMM_HPUSH)
        echo_spec_etl = self.client.etl(ETL_NAME_SPEC_COMP)
        echo_spec_etl.init_spec(template=template)

        temp_bck2 = self.client.bucket(random_string()).create()

        # Transform bucket with ECHO template
        job_id = self.bucket.transform(
            etl_name=echo_spec_etl.name,
            to_bck=temp_bck2,
            ext={"jpg": "txt"},
        )
        self.client.job(job_id).wait()

        # Verify extension rename
        for obj_iter in temp_bck2.list_objects().entries:
            self.assertEqual(obj_iter.name.split(".")[1], "txt")

        echo_obj = temp_bck2.object("temp-obj1.txt").get().read_all()

        # Verify different bucket-level transformations are not the same (compare ECHO transformation and MD5
        # transformation)
        self.assertNotEqual(md5_obj, echo_obj)

        echo_spec_etl.stop()
        echo_spec_etl.delete()

        # Transform w/ non-existent ETL name raises exception
        with self.assertRaises(AISError):
            self.bucket.transform(
                etl_name="faulty-name", to_bck=Bucket(random_string())
            )

        # Stop ETLs
        code_etl.stop()
        spec_etl.stop()
        self.assertEqual(
            len(self.client.cluster().list_running_etls()), self.current_etl_count
        )

        # Start stopped ETLs
        code_etl.start()
        spec_etl.start()
        self.assertEqual(
            len(self.client.cluster().list_running_etls()), self.current_etl_count + 2
        )

        # Delete stopped ETLs
        code_etl.stop()
        spec_etl.stop()
        code_etl.delete()
        spec_etl.delete()

        # Starting deleted ETLs raises error
        with self.assertRaises(AISError):
            code_etl.start()
        with self.assertRaises(AISError):
            spec_etl.start()

    @pytest.mark.etl
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

        md5_hpush_etl = self.client.etl(ETL_NAME_CODE)
        md5_hpush_etl.init_code(transform=transform)

        # code (io comm)
        def main():
            md5 = hashlib.md5()
            chunk = sys.stdin.buffer.read()
            md5.update(chunk)
            sys.stdout.buffer.write(md5.hexdigest().encode())

        md5_io_etl = self.client.etl(ETL_NAME_CODE_IO)
        md5_io_etl.init_code(transform=main, communication_type=ETL_COMM_IO)

        start_time = time.time()
        job_id = self.bucket.transform(
            etl_name=md5_hpush_etl.name, to_bck=Bucket("transformed-etl-hpush")
        )
        self.client.job(job_id).wait()
        print("Transform bucket using HPUSH took ", time.time() - start_time)

        start_time = time.time()
        job_id = self.bucket.transform(
            etl_name=md5_io_etl.name, to_bck=Bucket("transformed-etl-io")
        )
        self.client.job(job_id).wait()
        print("Transform bucket using IO took ", time.time() - start_time)

        for key, value in content.items():
            transformed_obj_hpush = (
                self.bucket.object(key).get(etl_name=md5_hpush_etl.name).read_all()
            )
            transformed_obj_io = (
                self.bucket.object(key).get(etl_name=md5_io_etl.name).read_all()
            )

            self.assertEqual(transform(bytes(value)), transformed_obj_hpush)
            self.assertEqual(transform(bytes(value)), transformed_obj_io)

    @pytest.mark.etl
    def test_etl_apis_stream(self):
        def transform(reader, writer):
            checksum = hashlib.md5()
            for byte in reader:
                checksum.update(byte)
            writer.write(checksum.hexdigest().encode())

        code_stream_etl = self.client.etl(ETL_NAME_CODE_STREAM)
        code_stream_etl.init_code(transform=transform, chunk_size=32768)

        obj = (
            self.bucket.object(self.obj_name)
            .get(etl_name=code_stream_etl.name)
            .read_all()
        )
        md5 = hashlib.md5()
        md5.update(self.content)
        self.assertEqual(obj, md5.hexdigest().encode())

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

        xor_etl = self.client.etl("etl-xor1")
        xor_etl.init_code(transform=transform, chunk_size=32)
        transformed_obj = (
            self.bucket.object(self.obj_name).get(etl_name=xor_etl.name).read_all()
        )
        data, checksum = transformed_obj[:-32], transformed_obj[-32:]
        computed_checksum = hashlib.md5(data).hexdigest().encode()
        self.assertEqual(checksum, computed_checksum)

    @pytest.mark.etl
    def test_etl_transform_url(self):
        def url_transform(url):
            return url.encode("utf-8")

        url_etl = self.client.etl("etl-hpull-url")
        url_etl.init_code(
            transform=url_transform, arg_type="url", communication_type="hpull"
        )
        res = self.bucket.object(self.obj_name).get(etl_name=url_etl.name).read_all()
        result_url = res.decode("utf-8")

        self.assertTrue(self.bucket.name in result_url)
        self.assertTrue(self.obj_name in result_url)

    @pytest.mark.etl
    def test_etl_with_various_sizes(self):
        obj_sizes = [128, 1024, 1048576]

        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        for obj_size in obj_sizes:
            obj_name = f"obj-{obj_size}.jpg"
            content = create_and_put_object(
                client=self.client,
                bck_name=self.bck_name,
                obj_name=obj_name,
                obj_size=obj_size,
            )

            etl = self.client.etl(f"etl-{random_string(5)}")
            etl.init_code(transform=transform)

            obj = self.bucket.object(obj_name).get(etl_name=etl.name).read_all()
            self.assertEqual(obj, transform(bytes(content)))

    @pytest.mark.etl
    def test_etl_concurrent_transformations(self):
        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        num_transformations = 5
        num_objects = 5
        etl_jobs = []
        contents_in_bucket = []

        start_time = time.time()
        for i in range(num_transformations):
            content = {}
            src_bck_name = f"src-bck{i}"
            dst_bck_name = f"dst-bck{i}"
            bck = self.client.bucket(src_bck_name).create()
            for j in range(num_objects):
                obj_name = f"obj{j}.jpg"
                content[obj_name] = create_and_put_object(
                    client=self.client,
                    bck_name=src_bck_name,
                    obj_name=obj_name,
                    obj_size=self.obj_size,
                )
            contents_in_bucket.append((bck, content))

            etl = self.client.etl(f"etl-{src_bck_name}")
            etl.init_code(transform=transform)

            job_id = bck.transform(etl_name=etl.name, to_bck=Bucket(dst_bck_name))
            etl_jobs.append(job_id)

        for job_id in etl_jobs:
            self.client.job(job_id).wait()

        print(
            f"Transform {num_transformations} buckets with {num_objects} objects took ",
            time.time() - start_time,
        )

        for src_bck, content in contents_in_bucket:
            for key, value in content.items():
                transformed_obj = (
                    src_bck.object(key).get(etl_name=f"etl-{src_bck.name}").read_all()
                )
                self.assertEqual(transform(bytes(value)), transformed_obj)


if __name__ == "__main__":
    unittest.main()
