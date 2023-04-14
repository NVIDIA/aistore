#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
import hashlib
import unittest
import tarfile
import io

import pytest

from aistore.sdk.const import PROVIDER_AIS
from aistore.sdk.errors import InvalidBckProvider
from tests.integration import REMOTE_SET, TEST_TIMEOUT
from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.utils import random_string


# pylint: disable=unused-variable,too-many-instance-attributes
class TestObjectGroupOps(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        self.obj_names = self._create_objects(10, suffix="-suffix")

    def test_delete(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        job_id = object_group.delete()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        existing_objects = self.bucket.list_objects(
            prefix=self.obj_prefix
        ).get_entries()
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
        self._verify_cached_objects(10, [0])

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
        self._verify_cached_objects(10, range(1, 10))

    def test_prefetch_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=PROVIDER_AIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).prefetch()

    def test_copy_objects(self):
        to_bck_name = "destination-bucket"
        to_bck = self._create_bucket(to_bck_name)
        self.assertEqual(0, len(to_bck.list_all_objects(prefix=self.obj_prefix)))
        self.assertEqual(10, len(self.bucket.list_all_objects(prefix=self.obj_prefix)))

        new_prefix = "prefix-"
        copy_job = self.bucket.objects(obj_names=self.obj_names[1:5]).copy(
            to_bck, prepend=new_prefix
        )
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT)

        self.assertEqual(
            4, len(to_bck.list_all_objects(prefix=new_prefix + self.obj_prefix))
        )

    def test_archive_objects_without_copy(self):
        self._archive_exec_assert(self.bucket, self.bucket)

    def test_archive_objects_with_copy(self):
        dest_bck = self._create_bucket(random_string())
        self._archive_exec_assert(self.bucket, dest_bck, to_bck=dest_bck)

    def _archive_exec_assert(self, src_bck, res_bck, **kwargs):
        arch_name = "my_arch.tar"
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
        self.assertEqual(10, len(self.bucket.list_all_objects(prefix=self.obj_prefix)))

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
        self._check_all_objects_cached(10, expected_cached=False)

    def _verify_cached_objects(self, expected_object_count, cached_range):
        """
        List each of the objects and verify the correct count and that all objects matching
        the cached range are cached and all others are not

        Args:
            expected_object_count: expected number of objects to list
            cached_range: object indices that should be cached, all others should not
        """
        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).get_entries()
        self.assertEqual(expected_object_count, len(objects))
        cached_names = {self.obj_prefix + str(x) + "-suffix" for x in cached_range}
        cached_objs = []
        evicted_objs = []
        for obj in objects:
            if obj.name in cached_names:
                cached_objs.append(obj)
            else:
                evicted_objs.append(obj)
        self._validate_objects_cached(cached_objs, True)
        self._validate_objects_cached(evicted_objs, False)
