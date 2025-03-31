#
# Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
#
import random
import unittest
from pathlib import Path

import pytest

import requests

from aistore.sdk import ListObjectFlag
from aistore.sdk.const import UTF_ENCODING, LOREM, DUIS
from aistore.sdk.dataset.dataset_config import DatasetConfig
from aistore.sdk.dataset.data_attribute import DataAttribute
from aistore.sdk.dataset.label_attribute import LabelAttribute
from aistore.sdk.errors import InvalidBckProvider, AISError, ErrBckNotFound
from aistore.sdk.enums import FLTPresence
from aistore.sdk.provider import Provider

from tests.integration.sdk.parallel_test_base import ParallelTestBase

from tests.utils import random_string, cleanup_local, cases
from tests.const import (
    OBJECT_COUNT,
    OBJ_CONTENT,
    PREFIX_NAME,
    TEST_TIMEOUT,
    SUFFIX_NAME,
)
from tests.integration import REMOTE_SET, AWS_BUCKET

INNER_DIR = "directory"
DATASET_DIR = "dataset"
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
class TestBucketOps(ParallelTestBase):
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
        new_bck = self._create_bucket()
        res = self.client.cluster().list_buckets()
        bucket_names = {bck.name for bck in res}
        self.assertIn(new_bck.name, bucket_names)

    @cases(
        "*", ".", "", " ", "bucket/name", "bucket and name", "#name", "$name", "~name"
    )
    def test_create_bucket_invalid_name(self, testcase):
        with self.assertRaises(AISError):
            bck = self.client.bucket(testcase)
            try:
                bck.create()
            finally:
                bck.delete(missing_ok=True)

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
        from_bck = self._create_bucket()
        to_bck_name = from_bck.name + "-renamed"

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
        self._register_for_post_test_cleanup(names=[to_bck_name], is_bucket=True)

    def test_copy(self):
        from_bck = self._create_bucket()
        to_bck = self._create_bucket()
        prefix = PREFIX_NAME
        new_prefix = "new-"
        content = b"test"
        expected_name = prefix + "-obj"
        from_bck.object(expected_name).get_writer().put_content(content)
        from_bck.object("notprefix-obj").get_writer().put_content(content)

        job_id = from_bck.copy(to_bck, prefix_filter=prefix, prepend=new_prefix)

        self.assertNotEqual(job_id, "")
        self.client.job(job_id).wait()
        entries = to_bck.list_all_objects()
        self.assertEqual(1, len(entries))
        self.assertEqual(new_prefix + expected_name, entries[0].name)

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_get_latest_flag(self):
        obj_name = random_string()
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)

        # out-of-band PUT: first version
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=LOREM)

        # cold GET, and check
        content = self.bucket.object(obj_name).get_reader().read_all()
        self.assertEqual(LOREM, content.decode("utf-8"))

        # out-of-band PUT: 2nd version (overwrite)
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=DUIS)

        # warm GET and check (expecting the first version's content)
        content = self.bucket.object(obj_name).get_reader().read_all()
        self.assertEqual(LOREM, content.decode("utf-8"))

        # warm GET with `--latest` flag, content should be updated
        content = self.bucket.object(obj_name).get_reader(latest=True).read_all()
        self.assertEqual(DUIS, content.decode("utf-8"))

        # out-of-band DELETE
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # warm GET must be fine
        content = self.bucket.object(obj_name).get_reader().read_all()
        self.assertEqual(DUIS, content.decode("utf-8"))

        # cold GET must result in Error
        with self.assertRaises(AISError):
            self.bucket.object(obj_name).get_reader(latest=True).read_all()

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    @pytest.mark.extended
    def test_copy_objects_sync_flag(self):
        num_obj = OBJECT_COUNT
        obj_names = self._create_objects(num_obj=num_obj, suffix=SUFFIX_NAME)
        to_bck = self._create_bucket()

        obj_group = self.bucket.objects(obj_names=obj_names)

        # cache and verify
        job_id = obj_group.prefetch()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT * 2)
        self._verify_cached_objects(num_obj, range(num_obj))
        # copy objs to dst bck
        copy_job = self.bucket.copy(prefix_filter=self.obj_prefix, to_bck=to_bck)
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT)
        self.assertEqual(num_obj, len(to_bck.list_all_objects()))

        # randomly delete 10% of the objects
        num_to_del = int(num_obj * 0.1)

        # out of band delete
        for obj_name in random.sample(obj_names, num_to_del):
            self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # test --sync flag
        copy_job = self.bucket.copy(
            prefix_filter=self.obj_prefix, to_bck=to_bck, sync=True
        )
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT * 3)
        self.assertEqual(num_obj - num_to_del, len(to_bck.list_all_objects()))

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict(self):
        self._create_objects()
        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).entries
        self._validate_objects_cached(objects, True)

        self.bucket.evict(keep_md=True)

        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).entries
        self.assertEqual(OBJECT_COUNT, len(objects))
        self._validate_objects_cached(objects, False)

    def test_evict_local(self):
        # If the bucket is local, eviction should fail
        if not REMOTE_SET:
            with self.assertRaises(InvalidBckProvider):
                self.bucket.evict()
            return
        # Create a local bucket to test with if self.bucket is a cloud bucket
        local_bucket = self._create_bucket()
        with self.assertRaises(InvalidBckProvider):
            local_bucket.evict()

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
                    self.bucket.object(
                        self.obj_prefix + obj_name
                    ).get_reader().read_all()
        else:
            for obj_name, expected_data in expected_res_dict.items():
                res = self.bucket.object(self.obj_prefix + obj_name).get_reader()
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
            prefix_filter=PREFIX_NAME,
            pattern="*.txt",
        )
        self.bucket.object(self.obj_prefix + included_filename).get_reader()
        with self.assertRaises(AISError):
            self.bucket.object(excluded_by_pattern).get_reader().read_all()
        with self.assertRaises(AISError):
            self.bucket.object(excluded_by_prefix).get_reader().read_all()

    def test_put_files_dry_run(self):
        self._create_put_files_structure(TOP_LEVEL_FILES, LOWER_LEVEL_FILES)
        self.bucket.put_files(
            self.local_test_files, dry_run=True, prepend=self.obj_prefix
        )
        # Verify the put files call does not actually create objects
        self._verify_obj_res(TOP_LEVEL_FILES, expect_err=True)

    @cases((None, OBJECT_COUNT), (7, 7), (OBJECT_COUNT * 2, OBJECT_COUNT))
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
        summ_test_bck = self._create_bucket()

        # Initially, the bucket should be empty
        bucket_summary = summ_test_bck.summary()

        self.assertEqual(bucket_summary["ObjCount"]["obj_count_present"], "0")
        self.assertEqual(bucket_summary["TotalSize"]["size_all_present_objs"], "0")
        self.assertEqual(bucket_summary["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bucket_summary["used_pct"], 0)

        # Upload objects to the bucket with different prefixes
        obj_names = ["prefix1_obj1", "prefix1_obj2", "prefix2_obj1", "prefix2_obj2"]
        for obj_name in obj_names:
            summ_test_bck.object(obj_name).get_writer().put_content(OBJ_CONTENT)

        # Verify the info with no prefix (should include all objects)
        bck_summ = summ_test_bck.summary()
        self.assertEqual(bck_summ["ObjCount"]["obj_count_present"], "4")
        self.assertNotEqual(bck_summ["TotalSize"]["size_all_present_objs"], "0")
        self.assertEqual(bck_summ["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bck_summ["used_pct"], 0)

        # Verify the info with prefix1
        bck_summ = summ_test_bck.summary(prefix="prefix1")
        self.assertEqual(bck_summ["ObjCount"]["obj_count_present"], "2")
        self.assertNotEqual(bck_summ["TotalSize"]["size_all_present_objs"], "0")
        self.assertEqual(bck_summ["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bck_summ["used_pct"], 0)

        # Verify the info with prefix2
        bck_summ = summ_test_bck.summary(prefix="prefix2")
        self.assertEqual(bck_summ["ObjCount"]["obj_count_present"], "2")
        self.assertNotEqual(bck_summ["TotalSize"]["size_all_present_objs"], "0")
        self.assertEqual(bck_summ["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bck_summ["used_pct"], 0)

        summ_test_bck.delete()

        # Accessing the summary of a deleted bucket should raise an error
        with self.assertRaises(ErrBckNotFound):
            summ_test_bck.summary()

    def test_info(self):
        info_test_bck = self._create_bucket()

        # Initially, the bucket should be empty
        _, bck_info = info_test_bck.info(flt_presence=FLTPresence.FLT_EXISTS)

        # For an empty bucket, the object count and total size should be zero
        self._validate_bck_info(info_test_bck, bck_info, "0", "0")

        # Upload objects to the bucket with different prefixes
        obj_names = ["prefix1_obj1", "prefix1_obj2", "prefix2_obj1"]
        for obj_name in obj_names:
            info_test_bck.object(obj_name).get_writer().put_content(OBJ_CONTENT)

        # Verify the info with no prefix (should include all objects)
        _, bck_info = info_test_bck.info()
        content_size = str(len(obj_names) * len(OBJ_CONTENT))
        self._validate_bck_info(
            info_test_bck, bck_info, str(len(obj_names)), content_size
        )

        # Verify the info with prefix1
        _, bck_info = info_test_bck.info(prefix="prefix1")
        content_size = str(2 * len(OBJ_CONTENT))
        self._validate_bck_info(info_test_bck, bck_info, "2", content_size)

        # Verify the info with prefix2
        _, bck_info = info_test_bck.info(prefix="prefix2")
        content_size = str(len(OBJ_CONTENT))
        self._validate_bck_info(info_test_bck, bck_info, "1", content_size)

        info_test_bck.delete()

        # Accessing the info of a deleted bucket should raise an error
        with self.assertRaises(ErrBckNotFound):
            info_test_bck.summary()

    def _validate_bck_info(
        self,
        bck,
        bck_info,
        present_obj_count,
        present_obj_size,
    ):
        self.assertEqual(bck_info["ObjCount"]["obj_count_present"], present_obj_count)
        self.assertEqual(
            bck_info["TotalSize"]["size_all_present_objs"], present_obj_size
        )
        self.assertEqual(bck_info["TotalSize"]["size_all_remote_objs"], "0")
        self.assertEqual(bck_info["provider"], Provider.AIS.value)
        self.assertEqual(bck_info["name"], bck.name)

    def test_write_dataset(self):
        self.local_test_files.mkdir(exist_ok=True)
        dataset_directory = self.local_test_files.joinpath(DATASET_DIR)
        dataset_directory.mkdir(exist_ok=True)
        img_files = {
            "file1.jpg": b"file1",
            "file2.jpg": b"file2",
            "file3.jpg": b"file3",
        }
        _create_files(dataset_directory, img_files)

        dataset_config = DatasetConfig(
            primary_attribute=DataAttribute(
                path=dataset_directory, name="image", file_type="jpg"
            ),
            secondary_attributes=[
                LabelAttribute(
                    name="label", label_identifier=lambda filename: f"{filename}_label"
                )
            ],
        )
        shards = []

        def post_process(shard_path):
            self._register_for_post_test_cleanup(names=[shard_path], is_bucket=False)
            shards.append(shard_path)

        self.bucket.write_dataset(
            dataset_config, pattern="dataset", maxcount=10, post=post_process
        )
        self.assertEqual(len(shards), 1)
        for shard in shards:
            self.assertIsNotNone(self.bucket.object(shard).head())

    def test_write_dataset_missing_attributes(self):
        self.local_test_files.mkdir(exist_ok=True)
        dataset_directory = self.local_test_files.joinpath(DATASET_DIR)
        dataset_directory.mkdir(exist_ok=True)
        img_files = {
            "file1.jpg": b"file1",
            "file2.jpg": b"file2",
            "file3.jpg": b"file3",
        }
        _create_files(dataset_directory, img_files)

        dataset_config = DatasetConfig(
            primary_attribute=DataAttribute(
                path=dataset_directory, name="image", file_type="jpg"
            ),
            secondary_attributes=[
                LabelAttribute(name="cls", label_identifier=lambda filename: None)
            ],
        )
        self.bucket.write_dataset(
            dataset_config, skip_missing=False, pattern="dataset", maxcount=10
        )
