#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import unittest
from pathlib import Path

import boto3

import requests

from aistore.sdk import ListObjectFlag
from aistore.sdk.const import PROVIDER_AIS, UTF_ENCODING
from aistore.sdk.errors import InvalidBckProvider, AISError, ErrBckNotFound

from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.unit.sdk.test_utils import test_cases
from tests import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from tests.integration.boto3 import AWS_REGION

from tests.utils import random_string, cleanup_local
from tests.integration import REMOTE_BUCKET, OBJECT_COUNT

# If remote bucket is not set, skip all cloud-related tests
REMOTE_SET = REMOTE_BUCKET != "" and not REMOTE_BUCKET.startswith(PROVIDER_AIS + ":")

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


# pylint: disable=unused-variable, too-many-public-methods
class TestBucketOps(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        self.local_test_files = (
            Path().absolute().joinpath("bucket-ops-test-" + random_string(8))
        )

    def tearDown(self) -> None:
        super().tearDown()
        cleanup_local(str(self.local_test_files))

    def _create_put_files_structure(self, top_level_files, lower_level_files):
        self.local_test_files.mkdir(exist_ok=True)
        _create_files(self.local_test_files, top_level_files)
        inner_dir = self.local_test_files.joinpath(INNER_DIR)
        inner_dir.mkdir()
        _create_files(inner_dir, lower_level_files)

    def test_bucket(self):
        new_bck_name = random_string(10)
        self._create_bucket(new_bck_name)
        res = self.client.cluster().list_buckets()
        bucket_names = {bck.name for bck in res}
        self.assertIn(new_bck_name, bucket_names)

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
        self.client.cluster().list_buckets()

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
    def test_get_latest_flag(self):
        obj_name = random_string()
        self.cloud_objects.append(obj_name)

        lorem = (
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod"
            " tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim"
            " veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea"
            " commodo consequat."
        )
        duis = (
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum"
            " dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non"
            " proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
            " Et harum quidem.."
        )

        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            # aws_session_token=AWS_SESSION_TOKEN,
        )

        # out-of-band PUT: first version
        s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=lorem)

        # cold GET, and check
        content = self.bucket.object(obj_name).get().read_all()
        self.assertEqual(lorem, content.decode("utf-8"))

        # out-of-band PUT: 2nd version (overwrite)
        s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=duis)

        # warm GET and check (expecting the first version's content)
        content = self.bucket.object(obj_name).get().read_all()
        self.assertEqual(lorem, content.decode("utf-8"))

        # warm GET with `--latest` flag, content should be updated
        content = self.bucket.object(obj_name).get(latest=True).read_all()
        self.assertEqual(duis, content.decode("utf-8"))

        # out-of-band DELETE
        s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # warm GET must be fine
        content = self.bucket.object(obj_name).get().read_all()
        self.assertEqual(duis, content.decode("utf-8"))

        # cold GET must result in Error
        with self.assertRaises(AISError):
            self.bucket.object(obj_name).get(latest=True)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict(self):
        self._create_objects()
        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).entries
        self._verify_objects_cache_status(objects, True)

        self.bucket.evict(keep_md=True)

        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).entries
        self.assertEqual(OBJECT_COUNT, len(objects))
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
        self.local_test_files.mkdir()
        filename = self.local_test_files.joinpath("file_not_dir")
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
        self._create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(self.local_test_files, prepend=self.obj_prefix)
        self._verify_obj_res(TOP_LEVEL_FILES)
        self._verify_obj_res(LOWER_LEVEL_FILES, expect_err=True)

    def test_put_files_recursive(self):
        self._create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(
            self.local_test_files, recursive=True, prepend=self.obj_prefix
        )

        self._verify_obj_res(TOP_LEVEL_FILES)
        # Lower level file object names will include their relative path by default
        expected_lower_res = {}
        for obj_name, expected_data in LOWER_LEVEL_FILES.items():
            obj_name = str(Path(INNER_DIR).joinpath(obj_name))
            expected_lower_res[obj_name] = expected_data
        self._verify_obj_res(expected_lower_res)

    def test_put_files_recursive_basename(self):
        self._create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(
            self.local_test_files,
            recursive=True,
            basename=True,
            prepend=self.obj_prefix,
        )

        # Expect all objects to be prefixed by custom_name and with no relative path in the name due to basename opt
        joined_file_data = {**TOP_LEVEL_FILES, **LOWER_LEVEL_FILES}
        expected_res = {}
        for obj_name, expected_data in joined_file_data.items():
            expected_res[obj_name] = expected_data
        self._verify_obj_res(expected_res)

    def test_put_files_filtered(self):
        self.local_test_files.mkdir()
        included_filename = "prefix-file.txt"
        excluded_by_pattern = "extra_top_file.py"
        excluded_by_prefix = "non-prefix-file.txt"
        for file in [included_filename, excluded_by_pattern, excluded_by_prefix]:
            with open(self.local_test_files.joinpath(file), "wb"):
                pass
        self.bucket.put_files(
            self.local_test_files,
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
        self._create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(
            self.local_test_files, dry_run=True, prepend=self.obj_prefix
        )
        # Verify the put files call does not actually create objects
        self._verify_obj_res(TOP_LEVEL_FILES, expect_err=True)

    @test_cases((None, OBJECT_COUNT), (7, 7), (OBJECT_COUNT * 2, OBJECT_COUNT))
    def test_list_objects(self, test_case):
        page_size, response_size = test_case
        # Only create the bucket entries on the first subtest run
        if len(self.bucket.list_all_objects(prefix=self.obj_prefix)) == 0:
            self._create_objects()
        if page_size:
            resp = self.bucket.list_objects(page_size=page_size, prefix=self.obj_prefix)
        else:
            resp = self.bucket.list_objects(prefix=self.obj_prefix)
        self.assertEqual(response_size, len(resp.entries))

    def test_list_all_objects(self):
        short_page_len = 17
        self._create_objects()
        objects = self.bucket.list_all_objects(prefix=self.obj_prefix)
        self.assertEqual(OBJECT_COUNT, len(objects))
        objects = self.bucket.list_all_objects(
            page_size=short_page_len, prefix=self.obj_prefix
        )
        self.assertEqual(OBJECT_COUNT, len(objects))

    def test_list_object_iter(self):
        obj_names = set(self._create_objects())

        # Empty iterator if there are no objects matching the prefix.
        obj_iter = self.bucket.list_objects_iter(prefix="invalid-obj-")
        self.assertEqual(0, len(list(obj_iter)))

        # Read all `bucket_size` objects by prefix.
        obj_iter = self.bucket.list_objects_iter(page_size=10, prefix=self.obj_prefix)
        for obj in obj_iter:
            obj_names.remove(obj.name)
        self.assertEqual(0, len(obj_names))

    def test_list_object_flags(self):
        self._create_objects()
        objects = self.bucket.list_all_objects(
            flags=[ListObjectFlag.NAME_ONLY, ListObjectFlag.CACHED],
            prefix=self.obj_prefix,
        )
        self.assertEqual(OBJECT_COUNT, len(objects))
        for obj in objects:
            self.assertEqual(0, obj.size)

        objects = self.bucket.list_all_objects(
            flags=[ListObjectFlag.NAME_SIZE], prefix=self.obj_prefix
        )
        self.assertEqual(OBJECT_COUNT, len(objects))
        for obj in objects:
            self.assertTrue(obj.size > 0)

    def test_summary(self):
        summ_test_bck = self._create_bucket("summary-test")

        # Initially, the bucket should be empty
        bucket_summary = summ_test_bck.summary()

        self.assertEqual(bucket_summary["ObjCount"]["obj_count_present"], "0")
        self.assertEqual(bucket_summary["TotalSize"]["size_all_present_objs"], "0")
        self.assertEqual(bucket_summary["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bucket_summary["used_pct"], 0)

        summ_test_bck.object("test-object").put_content("test-content")

        bucket_summary = summ_test_bck.summary()

        # Now, the bucket should have 1 object
        self.assertEqual(bucket_summary["ObjCount"]["obj_count_present"], "1")
        self.assertNotEqual(bucket_summary["TotalSize"]["size_all_present_objs"], "0")

        summ_test_bck.delete()

        # Accessing the summary of a deleted bucket should raise an error
        with self.assertRaises(ErrBckNotFound):
            summ_test_bck.summary()

    def test_info(self):
        info_test_bck = self._create_bucket("info-test")

        # Initially, the bucket should be empty
        _, bck_summ = info_test_bck.info(flt_presence=0)

        # For an empty bucket, the object count and total size should be zero
        self.assertEqual(bck_summ["ObjCount"]["obj_count_present"], "0")
        self.assertEqual(bck_summ["TotalSize"]["size_all_present_objs"], "0")
        self.assertEqual(bck_summ["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bck_summ["provider"], "ais")
        self.assertEqual(bck_summ["name"], "info-test")

        # Upload an object to the bucket
        info_test_bck.object("test-object").put_content("test-content")

        _, bck_summ = info_test_bck.info()

        # Now the bucket should have one object and non-zero size
        self.assertEqual(bck_summ["ObjCount"]["obj_count_present"], "1")
        self.assertNotEqual(bck_summ["TotalSize"]["size_all_present_objs"], "0")
        self.assertEqual(bck_summ["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bck_summ["provider"], "ais")
        self.assertEqual(bck_summ["name"], "info-test")

        info_test_bck.delete()

        # Accessing the info of a deleted bucket should raise an error
        with self.assertRaises(ErrBckNotFound):
            info_test_bck.summary()


if __name__ == "__main__":
    unittest.main()
