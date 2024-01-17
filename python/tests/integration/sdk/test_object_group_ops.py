#
# Copyright (c) 2023-2024, NVIDIA CORPORATION. All rights reserved.
#
import hashlib
import unittest
import tarfile
import io

import pytest

from aistore.sdk.const import PROVIDER_AIS, LOREM, DUIS
from aistore.sdk.errors import InvalidBckProvider, AISError
from tests.integration import REMOTE_SET, TEST_TIMEOUT, OBJECT_COUNT
from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.utils import random_string


# pylint: disable=unused-variable,too-many-instance-attributes
class TestObjectGroupOps(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        self.obj_names = self._create_objects(suffix="-suffix")
        if REMOTE_SET:
            self.s3_client = self._get_boto3_client()

    def test_delete(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        job_id = object_group.delete()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        existing_objects = self.bucket.list_objects(prefix=self.obj_prefix).entries
        self.assertEqual(1, len(existing_objects))
        self.assertEqual(self.obj_names[0], existing_objects[0].name)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        job_id = object_group.evict()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        self._verify_cached_objects(OBJECT_COUNT, [0])

    def test_evict_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=PROVIDER_AIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).evict()

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_list(self):
        obj_group = self.bucket.objects(obj_names=self.obj_names[1:])
        self._evict_all_objects()
        # Fetch back a specific object group and verify cache status
        job_id = obj_group.prefetch()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT * 2)
        self._verify_cached_objects(OBJECT_COUNT, range(1, OBJECT_COUNT))

    def test_prefetch_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=PROVIDER_AIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).prefetch()

    def test_copy_objects(self):
        to_bck_name = "destination-bucket"
        to_bck = self._create_bucket(to_bck_name)
        self.assertEqual(0, len(to_bck.list_all_objects(prefix=self.obj_prefix)))
        self.assertEqual(
            OBJECT_COUNT, len(self.bucket.list_all_objects(prefix=self.obj_prefix))
        )

        new_prefix = "prefix-"
        copy_job = self.bucket.objects(obj_names=self.obj_names[1:5]).copy(
            to_bck, prepend=new_prefix
        )
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT)

        self.assertEqual(
            4, len(to_bck.list_all_objects(prefix=new_prefix + self.obj_prefix))
        )

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_copy_objects_latest_flag(self):
        obj_name = random_string()
        self.cloud_objects.append(obj_name)
        to_bck_name = "dst-bck-cp-latest"
        to_bck = self._create_bucket(to_bck_name)

        # out-of-band PUT: first version
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=LOREM)

        # copy, and check
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, LOREM, False)
        # create a cached copy in src bucket
        content = self.bucket.object(obj_name).get().read_all()
        self.assertEqual(LOREM, content.decode("utf-8"))

        # out-of-band PUT: 2nd version (overwrite)
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=DUIS)

        # copy and check (expecting the first version)
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, LOREM, False)

        # copy latest: update in-cluster copy
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, DUIS, True)
        # check if cached copy is src bck is still on prev version
        content = self.bucket.object(obj_name).get().read_all()
        self.assertEqual(LOREM, content.decode("utf-8"))

        # out-of-band DELETE
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # copy and check (expecting no changes)
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, DUIS, True)

        # run copy with '--sync' one last time, and make sure the object "disappears"
        copy_job = self.bucket.objects(obj_names=[obj_name]).copy(
            self.bucket, sync=True
        )
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT)
        with self.assertRaises(AISError):
            self.bucket.object(obj_name).get()

        # TODO: add check for different dst bucket (after delete)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_objects_latest_flag(self):
        obj_name = random_string()
        self.cloud_objects.append(obj_name)
        to_bck_name = "dst-bck-prefetch-latest"

        # out-of-band PUT: first version
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=LOREM)

        # prefetch, and check
        self._prefetch_and_check_with_latest(self.bucket, obj_name, LOREM, False)

        # out-of-band PUT: 2nd version (overwrite)
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=DUIS)

        # prefetch and check (expecting the first version)
        self._prefetch_and_check_with_latest(self.bucket, obj_name, LOREM, False)

        # prefetch latest: update in-cluster copy
        self._prefetch_and_check_with_latest(self.bucket, obj_name, DUIS, True)

        # out-of-band DELETE
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # prefetch without '--latest': expecting no changes
        self._prefetch_and_check_with_latest(self.bucket, obj_name, DUIS, False)

        # run prefetch with '--latest' one last time, and make sure the object "disappears"
        prefetch_job = self.bucket.objects(obj_names=[obj_name]).prefetch(latest=True)
        self.client.job(job_id=prefetch_job).wait_for_idle(timeout=TEST_TIMEOUT)
        with self.assertRaises(AISError):
            self.bucket.object(obj_name).get()

    def _prefetch_and_check_with_latest(self, bucket, obj_name, expected, latest_flag):
        prefetch_job = bucket.objects(obj_names=[obj_name]).prefetch(latest=latest_flag)
        self.client.job(job_id=prefetch_job).wait_for_idle(timeout=TEST_TIMEOUT)

        content = bucket.object(obj_name).get().read_all()
        self.assertEqual(expected, content.decode("utf-8"))

    # pylint: disable=too-many-arguments
    def _copy_and_check_with_latest(
        self, from_bck, to_bck, obj_name, expected, latest_flag
    ):
        copy_job = from_bck.objects(obj_names=[obj_name]).copy(
            to_bck, latest=latest_flag
        )
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT)
        self.assertEqual(1, len(to_bck.list_all_objects()))
        content = to_bck.object(obj_name).get().read_all()
        self.assertEqual(expected, content.decode("utf-8"))

    def test_archive_objects_without_copy(self):
        arch_name = self.obj_prefix + "-archive-without-copy.tar"
        self._archive_exec_assert(arch_name, self.bucket, self.bucket)

    def test_archive_objects_with_copy(self):
        arch_name = self.obj_prefix + "-archive-with-copy.tar"
        dest_bck = self._create_bucket(random_string())
        self._archive_exec_assert(arch_name, self.bucket, dest_bck, to_bck=dest_bck)

    def _archive_exec_assert(self, arch_name, src_bck, res_bck, **kwargs):
        # Add to object list to clean up on test finish
        if res_bck.provider != PROVIDER_AIS:
            self.cloud_objects.append(arch_name)
        archived_names = self.obj_names[1:5]
        expected_contents = {}
        for name in archived_names:
            expected_contents[name] = src_bck.object(obj_name=name).get().read_all()

        arch_job = src_bck.objects(obj_names=archived_names).archive(
            archive_name=arch_name, **kwargs
        )
        self.client.job(job_id=arch_job).wait_for_idle(timeout=TEST_TIMEOUT)

        # Read the tar archive and assert the object names and contents match
        res_bytes = res_bck.object(arch_name).get().read_all()
        with tarfile.open(fileobj=io.BytesIO(res_bytes), mode="r") as tar:
            member_names = []
            for member in tar.getmembers():
                inner_file = tar.extractfile(member)
                self.assertEqual(expected_contents[member.name], inner_file.read())
                inner_file.close()
                member_names.append(member.name)
            self.assertEqual(set(archived_names), set(member_names))

    @pytest.mark.etl
    def test_transform_objects(self):
        # Define an etl with code that hashes the contents of each object
        etl_name = "etl-" + random_string(5)

        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        md5_etl = self.client.etl(etl_name)
        md5_etl.init_code(transform=transform)

        to_bck_name = "destination-bucket"
        to_bck = self._create_bucket(to_bck_name)
        new_prefix = "prefix-"
        self.assertEqual(0, len(to_bck.list_all_objects(prefix=self.obj_prefix)))
        self.assertEqual(
            OBJECT_COUNT, len(self.bucket.list_all_objects(prefix=self.obj_prefix))
        )

        transform_job = self.bucket.objects(obj_names=self.obj_names).transform(
            to_bck, etl_name=md5_etl.name, prepend=new_prefix
        )
        self.client.job(job_id=transform_job).wait_for_idle(timeout=TEST_TIMEOUT)

        # Get the md5 transform of each source object and verify the destination bucket contains those results
        from_obj_hashes = [
            transform(self.bucket.object(name).get().read_all())
            for name in self.obj_names
        ]
        to_obj_values = [
            to_bck.object(new_prefix + name).get().read_all() for name in self.obj_names
        ]
        self.assertEqual(to_obj_values, from_obj_hashes)

    def _evict_all_objects(self):
        job_id = self.bucket.objects(obj_names=self.obj_names).evict()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        self._check_all_objects_cached(OBJECT_COUNT, expected_cached=False)
