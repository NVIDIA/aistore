#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import unittest
from pathlib import Path

import requests

from aistore.sdk import Client
from aistore.sdk.const import ProviderAIS, UTF_ENCODING
from aistore.sdk.errors import ErrBckNotFound, InvalidBckProvider, AISError

from tests.utils import create_and_put_object, random_string, cleanup_local
from tests.integration import CLUSTER_ENDPOINT, REMOTE_BUCKET

# If remote bucket is not set, skip all cloud-related tests
REMOTE_SET = REMOTE_BUCKET != "" and not REMOTE_BUCKET.startswith(ProviderAIS + ":")
LOCAL_TEST_FILES = Path().absolute().joinpath("object-ops-test")

INNER_DIR = "directory"
TOP_LEVEL_FILES = {
    "top_level_file.txt": b"test data to verify",
    "other_top_level_file.txt": b"other file test data to verify",
}
LOWER_LEVEL_FILES = {"lower_level_file.txt": b"data in inner file"}
CLEANUP_TIMEOUT = 30


def _create_files(folder, file_dict):
    for filename, data in file_dict.items():
        lower_file = folder.joinpath(filename)
        with open(lower_file, "wb") as file:
            file.write(data)


def _create_put_files_structure(top_level_files, lower_level_files):
    LOCAL_TEST_FILES.mkdir(exist_ok=True)
    _create_files(LOCAL_TEST_FILES, top_level_files)
    inner_dir = LOCAL_TEST_FILES.joinpath(INNER_DIR)
    inner_dir.mkdir()
    _create_files(inner_dir, lower_level_files)


# pylint: disable=unused-variable
class TestBucketOps(unittest.TestCase):
    def setUp(self) -> None:
        self.bck_name = random_string()

        self.client = Client(CLUSTER_ENDPOINT)
        self.buckets = []
        self.provider = None
        self.cloud_bck = None
        self.obj_prefix = "test_bucket_ops-"
        if REMOTE_SET:
            self.cloud_objects = []
            self.provider, self.cloud_bck = REMOTE_BUCKET.split("://")
            self._cleanup_objects()

        cleanup_local(str(LOCAL_TEST_FILES))

    def tearDown(self) -> None:
        cleanup_local(str(LOCAL_TEST_FILES))
        # Try to destroy all temporary buckets if there are left.
        for bck_name in self.buckets:
            try:
                self.client.bucket(bck_name).delete()
            except ErrBckNotFound:
                pass
        # If we are using a remote bucket for this specific test, delete the objects instead of the full bucket
        if self.cloud_bck:
            bucket = self.client.bucket(self.cloud_bck, provider=self.provider)
            for obj_name in self.cloud_objects:
                bucket.objects(obj_range=obj_name).delete()

    def _cleanup_objects(self):
        cloud_bck = self.client.bucket(self.cloud_bck, self.provider)
        # Clean up any other objects created with the test prefix, potentially from aborted tests
        object_names = [
            x.name for x in cloud_bck.list_objects(self.obj_prefix).get_entries()
        ]
        if len(object_names) > 0:
            job_id = cloud_bck.objects(obj_names=object_names).delete()
            self.client.job(job_id).wait(timeout=CLEANUP_TIMEOUT)

    def test_bucket(self):
        res = self.client.cluster().list_buckets()
        count = len(res)
        self.create_bucket(self.bck_name)
        res = self.client.cluster().list_buckets()
        count_new = len(res)
        self.assertEqual(count + 1, count_new)

    def create_bucket(self, bck_name):
        self.buckets.append(bck_name)
        bucket = self.client.bucket(bck_name)
        bucket.create()
        return bucket

    def test_head_bucket(self):
        self.create_bucket(self.bck_name)
        self.client.bucket(self.bck_name).head()
        self.client.bucket(self.bck_name).delete()
        try:
            self.client.bucket(self.bck_name).head()
        except requests.exceptions.HTTPError as err:
            self.assertEqual(err.response.status_code, 404)

    def test_rename_bucket(self):
        from_bck_n = self.bck_name + "from"
        to_bck_n = self.bck_name + "to"
        self.create_bucket(from_bck_n)
        res = self.client.cluster().list_buckets()
        count = len(res)

        bck_obj = self.client.bucket(from_bck_n)
        self.assertEqual(bck_obj.name, from_bck_n)
        job_id = bck_obj.rename(to_bck=to_bck_n)
        self.assertNotEqual(job_id, "")

        # wait for rename to finish
        self.client.job(job_id).wait()

        # check if objects name has changed
        self.client.bucket(to_bck_n).head()

        # new bucket should be created and accessible
        self.assertEqual(bck_obj.name, to_bck_n)

        # old bucket should be inaccessible
        try:
            self.client.bucket(from_bck_n).head()
        except requests.exceptions.HTTPError as err:
            self.assertEqual(err.response.status_code, 404)

        # length of buckets before and after rename should be same
        res = self.client.cluster().list_buckets()
        count_new = len(res)
        self.assertEqual(count, count_new)

    def test_copy_bucket(self):
        from_bck = self.bck_name + "from"
        to_bck = self.bck_name + "to"
        self.create_bucket(from_bck)
        self.create_bucket(to_bck)

        job_id = self.client.bucket(from_bck).copy(to_bck)
        self.assertNotEqual(job_id, "")
        self.client.job(job_id).wait()

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict_bucket(self):
        obj_name = "test_evict_bucket-"
        create_and_put_object(
            self.client,
            bck_name=self.cloud_bck,
            provider=self.provider,
            obj_name=obj_name,
        )
        bucket = self.client.bucket(self.cloud_bck, provider=self.provider)
        objects = bucket.list_objects(
            props="name,cached", prefix=obj_name
        ).get_entries()
        self.verify_objects_cache_status(objects, True)

        bucket.evict()

        objects = bucket.list_objects(
            props="name,cached", prefix=obj_name
        ).get_entries()
        self.verify_objects_cache_status(objects, False)

    def test_evict_bucket_local(self):
        bucket = self.create_bucket(self.bck_name)
        with self.assertRaises(InvalidBckProvider):
            bucket.evict()

    def verify_objects_cache_status(self, objects, expected_status):
        self.assertTrue(len(objects) > 0)
        for obj in objects:
            self.assertTrue(obj.is_ok())
            if expected_status:
                self.assertTrue(obj.is_cached())
            else:
                self.assertFalse(obj.is_cached())

    def test_put_files_invalid(self):
        bucket = self.create_bucket(self.bck_name)
        with self.assertRaises(ValueError):
            bucket.put_files("non-existent-dir")
        LOCAL_TEST_FILES.mkdir()
        filename = LOCAL_TEST_FILES.joinpath("file_not_dir")
        with open(filename, "w", encoding=UTF_ENCODING):
            pass
        with self.assertRaises(ValueError):
            bucket.put_files(filename)

    def _verify_obj_res(self, bucket, expected_res_dict, expect_err=False):
        if expect_err:
            for obj_name in expected_res_dict:
                with self.assertRaises(AISError):
                    bucket.object(obj_name).get()
        else:
            for obj_name, expected_data in expected_res_dict.items():
                res = bucket.object(obj_name).get()
                self.assertEqual(expected_data, res.read_all())

    def test_put_files_default_args(self):
        bucket = self.create_bucket(self.bck_name)
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        bucket.put_files(LOCAL_TEST_FILES)
        self._verify_obj_res(bucket, TOP_LEVEL_FILES)
        self._verify_obj_res(bucket, LOWER_LEVEL_FILES, expect_err=True)

    def test_put_files_recursive(self):
        bucket = self.create_bucket(self.bck_name)
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        bucket.put_files(LOCAL_TEST_FILES, recursive=True)

        self._verify_obj_res(bucket, TOP_LEVEL_FILES)
        # Lower level file object names will include their relative path by default
        expected_lower_res = {}
        for obj_name, expected_data in LOWER_LEVEL_FILES.items():
            obj_name = str(Path(INNER_DIR).joinpath(obj_name))
            expected_lower_res[obj_name] = expected_data
        self._verify_obj_res(bucket, expected_lower_res)

    def test_put_files_recursive_name_options(self):
        bucket = self.create_bucket(self.bck_name)
        custom_name = "my-obj/"
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        bucket.put_files(
            LOCAL_TEST_FILES, recursive=True, basename=True, obj_prefix=custom_name
        )

        # Expect all objects to be prefixed by custom_name and with no relative path in the name due to basename opt
        joined_file_data = {**TOP_LEVEL_FILES, **LOWER_LEVEL_FILES}
        expected_res = {}
        for obj_name, expected_data in joined_file_data.items():
            expected_res[custom_name + obj_name] = expected_data
        self._verify_obj_res(bucket, expected_res)

    def test_put_files_filtered(self):
        bucket = self.create_bucket(self.bck_name)
        LOCAL_TEST_FILES.mkdir()
        included_filename = "prefix-file.txt"
        excluded_by_pattern = "extra_top_file.py"
        excluded_by_prefix = "non-prefix-file.txt"
        for file in [included_filename, excluded_by_pattern, excluded_by_prefix]:
            with open(Path(LOCAL_TEST_FILES).joinpath(file), "wb"):
                pass
        bucket.put_files(LOCAL_TEST_FILES, prefix_filter="prefix-", pattern="*.txt")
        bucket.object(included_filename).get()
        with self.assertRaises(AISError):
            bucket.object(excluded_by_pattern).get()
        with self.assertRaises(AISError):
            bucket.object(excluded_by_prefix).get()

    def test_put_files_dry_run(self):
        bucket = self.create_bucket(self.bck_name)
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        bucket.put_files(LOCAL_TEST_FILES, dry_run=True)
        # Verify the put files call does not actually create objects
        self._verify_obj_res(bucket, TOP_LEVEL_FILES, expect_err=True)


if __name__ == "__main__":
    unittest.main()
