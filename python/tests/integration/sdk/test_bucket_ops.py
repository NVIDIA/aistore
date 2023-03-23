#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import unittest
from pathlib import Path

import requests

from aistore.sdk import ListObjectFlag
from aistore.sdk.const import PROVIDER_AIS, UTF_ENCODING
from aistore.sdk.errors import InvalidBckProvider, AISError, ErrBckNotFound

from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest

from tests.utils import random_string, cleanup_local
from tests.integration import REMOTE_BUCKET

# If remote bucket is not set, skip all cloud-related tests
REMOTE_SET = REMOTE_BUCKET != "" and not REMOTE_BUCKET.startswith(PROVIDER_AIS + ":")
LOCAL_TEST_FILES = Path().absolute().joinpath("object-ops-test")

INNER_DIR = "directory"
TOP_LEVEL_FILES = {
    "top_level_file.txt": b"test data to verify",
    "other_top_level_file.txt": b"other file test data to verify",
}
LOWER_LEVEL_FILES = {"lower_level_file.txt": b"data in inner file"}


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
class TestBucketOps(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        cleanup_local(str(LOCAL_TEST_FILES))

    def tearDown(self) -> None:
        super().tearDown()
        cleanup_local(str(LOCAL_TEST_FILES))

    def test_bucket(self):
        new_bck_name = random_string(10)
        res = self.client.cluster().list_buckets()
        count = len(res)
        self._create_bucket(new_bck_name)
        res = self.client.cluster().list_buckets()
        count_new = len(res)
        self.assertEqual(count + 1, count_new)

    def test_bucket_invalid_name(self):
        with self.assertRaises(ErrBckNotFound):
            self.client.bucket("INVALID_BCK_NAME").list_objects()

    def test_bucket_invalid_aws_name(self):
        with self.assertRaises(AISError):
            self.client.bucket("INVALID_BCK_NAME", "aws").list_objects()

    def test_head(self):
        try:
            self.bucket.head()
        except requests.exceptions.HTTPError as err:
            self.assertEqual(err.response.status_code, 404)

    def test_rename(self):
        from_bck_name = self.bck_name + "from"
        to_bck_name = self.bck_name + "to"
        from_bck = self._create_bucket(from_bck_name)
        res = self.client.cluster().list_buckets()
        bucket_count = len(res)

        self.assertEqual(from_bck_name, from_bck.name)
        job_id = from_bck.rename(to_bck_name=to_bck_name)
        self.assertNotEqual(job_id, "")

        # wait for rename to finish
        self.client.job(job_id).wait()

        # new bucket should be created and accessible
        to_bck = self.client.bucket(to_bck_name)
        to_bck.head()
        self.assertEqual(to_bck_name, to_bck.name)

        # old bucket should be inaccessible
        try:
            from_bck.head()
        except requests.exceptions.HTTPError as err:
            self.assertEqual(err.response.status_code, 404)

        # length of buckets before and after rename should be same
        res = self.client.cluster().list_buckets()
        count_new = len(res)
        self.assertEqual(bucket_count, count_new)

    def test_copy(self):
        from_bck_name = self.bck_name + "from"
        to_bck_name = self.bck_name + "to"
        from_bck = self._create_bucket(from_bck_name)
        to_bck = self._create_bucket(to_bck_name)
        prefix = "prefix-"
        new_prefix = "new-"
        content = b"test"
        expected_name = prefix + "-obj"
        from_bck.object(expected_name).put_content(content)
        from_bck.object("notprefix-obj").put_content(content)

        job_id = from_bck.copy(to_bck, prefix_filter=prefix, prepend=new_prefix)

        self.assertNotEqual(job_id, "")
        self.client.job(job_id).wait()
        entries = to_bck.list_all_objects()
        self.assertEqual(1, len(entries))
        self.assertEqual(new_prefix + expected_name, entries[0].name)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict(self):
        self._create_objects(num_obj=1)
        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).get_entries()
        self._verify_objects_cache_status(objects, True)

        self.bucket.evict()

        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).get_entries()
        self.assertEqual(1, len(objects))
        self._verify_objects_cache_status(objects, False)

    def test_evict_local(self):
        # If the bucket is local, eviction should fail
        if not REMOTE_SET:
            with self.assertRaises(InvalidBckProvider):
                self.bucket.evict()
            return
        # Create a local bucket to test with if self.bucket is a cloud bucket
        local_bucket = self._create_bucket(self.bck_name + "-local")
        with self.assertRaises(InvalidBckProvider):
            local_bucket.evict()

    def _verify_objects_cache_status(self, objects, expected_status):
        self.assertTrue(len(objects) > 0)
        for obj in objects:
            self.assertTrue(obj.is_ok())
            self.assertEqual(expected_status, obj.is_cached())

    def test_put_files_invalid(self):
        with self.assertRaises(ValueError):
            self.bucket.put_files("non-existent-dir")
        LOCAL_TEST_FILES.mkdir()
        filename = LOCAL_TEST_FILES.joinpath("file_not_dir")
        with open(filename, "w", encoding=UTF_ENCODING):
            pass
        with self.assertRaises(ValueError):
            self.bucket.put_files(filename)

    def _verify_obj_res(self, expected_res_dict, expect_err=False):
        if expect_err:
            for obj_name in expected_res_dict:
                with self.assertRaises(AISError):
                    self.bucket.object(self.obj_prefix + obj_name).get()
        else:
            for obj_name, expected_data in expected_res_dict.items():
                res = self.bucket.object(self.obj_prefix + obj_name).get()
                self.assertEqual(expected_data, res.read_all())

    def test_put_files_default_args(self):
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(LOCAL_TEST_FILES, prepend=self.obj_prefix)
        self._verify_obj_res(TOP_LEVEL_FILES)
        self._verify_obj_res(LOWER_LEVEL_FILES, expect_err=True)

    def test_put_files_recursive(self):
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(LOCAL_TEST_FILES, recursive=True, prepend=self.obj_prefix)

        self._verify_obj_res(TOP_LEVEL_FILES)
        # Lower level file object names will include their relative path by default
        expected_lower_res = {}
        for obj_name, expected_data in LOWER_LEVEL_FILES.items():
            obj_name = str(Path(INNER_DIR).joinpath(obj_name))
            expected_lower_res[obj_name] = expected_data
        self._verify_obj_res(expected_lower_res)

    def test_put_files_recursive_basename(self):
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(
            LOCAL_TEST_FILES, recursive=True, basename=True, prepend=self.obj_prefix
        )

        # Expect all objects to be prefixed by custom_name and with no relative path in the name due to basename opt
        joined_file_data = {**TOP_LEVEL_FILES, **LOWER_LEVEL_FILES}
        expected_res = {}
        for obj_name, expected_data in joined_file_data.items():
            expected_res[obj_name] = expected_data
        self._verify_obj_res(expected_res)

    def test_put_files_filtered(self):
        LOCAL_TEST_FILES.mkdir()
        included_filename = "prefix-file.txt"
        excluded_by_pattern = "extra_top_file.py"
        excluded_by_prefix = "non-prefix-file.txt"
        for file in [included_filename, excluded_by_pattern, excluded_by_prefix]:
            with open(Path(LOCAL_TEST_FILES).joinpath(file), "wb"):
                pass
        self.bucket.put_files(
            LOCAL_TEST_FILES,
            prepend=self.obj_prefix,
            prefix_filter="prefix-",
            pattern="*.txt",
        )
        self.bucket.object(self.obj_prefix + included_filename).get()
        with self.assertRaises(AISError):
            self.bucket.object(excluded_by_pattern).get()
        with self.assertRaises(AISError):
            self.bucket.object(excluded_by_prefix).get()

    def test_put_files_dry_run(self):
        _create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(LOCAL_TEST_FILES, dry_run=True, prepend=self.obj_prefix)
        # Verify the put files call does not actually create objects
        self._verify_obj_res(TOP_LEVEL_FILES, expect_err=True)

    def test_list_objects(self):
        bucket_size = 110
        tests = [
            {"resp_size": bucket_size},
            {"page_size": 7, "resp_size": 7},
            {"page_size": bucket_size * 2, "resp_size": bucket_size},
        ]
        self._create_objects(bucket_size)

        for test in list(tests):
            resp = (
                self.bucket.list_objects(
                    page_size=test["page_size"], prefix=self.obj_prefix
                )
                if "page_size" in test
                else self.bucket.list_objects(prefix=self.obj_prefix)
            )
            self.assertEqual(len(resp.get_entries()), test["resp_size"])

    def test_list_all_objects(self):
        bucket_size = 110
        short_page_len = 17
        self._create_objects(bucket_size)
        objects = self.bucket.list_all_objects(prefix=self.obj_prefix)
        self.assertEqual(len(objects), bucket_size)
        objects = self.bucket.list_all_objects(
            page_size=short_page_len, prefix=self.obj_prefix
        )
        self.assertEqual(len(objects), bucket_size)

    def test_list_object_iter(self):
        bucket_size = 110
        obj_names = set(self._create_objects(bucket_size))

        # Empty iterator if there are no objects matching the prefix.
        obj_iter = self.bucket.list_objects_iter(prefix="invalid-obj-")
        self.assertEqual(0, len(list(obj_iter)))

        # Read all `bucket_size` objects by prefix.
        obj_iter = self.bucket.list_objects_iter(page_size=15, prefix=self.obj_prefix)
        for obj in obj_iter:
            obj_names.remove(obj.name)
        self.assertEqual(0, len(obj_names))

    def test_list_object_flags(self):
        num_obj = 10
        self._create_objects(num_obj)
        objects = self.bucket.list_all_objects(
            flags=[ListObjectFlag.NAME_ONLY, ListObjectFlag.CACHED],
            prefix=self.obj_prefix,
        )
        self.assertEqual(num_obj, len(objects))
        for obj in objects:
            self.assertEqual(0, obj.size)

        objects = self.bucket.list_all_objects(
            flags=[ListObjectFlag.NAME_SIZE], prefix=self.obj_prefix
        )
        self.assertEqual(num_obj, len(objects))
        for obj in objects:
            self.assertTrue(obj.size > 0)


if __name__ == "__main__":
    unittest.main()
