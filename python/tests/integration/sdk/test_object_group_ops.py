#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
import unittest

from aistore.sdk.const import PROVIDER_AIS
from aistore.sdk.errors import InvalidBckProvider
from aistore.sdk.object_range import ObjectRange
from tests.integration import REMOTE_SET, TEST_TIMEOUT
from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.utils import random_string


# pylint: disable=unused-variable,too-many-instance-attributes
class TestObjectGroupOps(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        self.obj_suffix = "-suffix"
        self.obj_template = self.obj_prefix + "{1..8..2}" + self.obj_suffix

        # Range selecting objects 1,3,5,7
        self.obj_range = ObjectRange(
            self.obj_prefix, 1, 8, step=2, suffix=self.obj_suffix
        )
        self.obj_names = self._create_objects(10, suffix=self.obj_suffix)

    def test_delete_list(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        self._delete_test_helper(object_group, [self.obj_names[0]])

    def test_delete_range(self):
        object_group = self.bucket.objects(obj_range=self.obj_range)
        self._delete_group_helper(object_group)

    def test_delete_template(self):
        object_group = self.bucket.objects(obj_template=self.obj_template)
        self._delete_group_helper(object_group)

    def _delete_group_helper(self, object_group):
        expected_object_names = [
            self.obj_prefix + str(x) + self.obj_suffix for x in range(0, 9, 2)
        ]
        expected_object_names.append(self.obj_prefix + "9" + self.obj_suffix)
        self._delete_test_helper(object_group, expected_object_names)

    def _delete_test_helper(self, object_group, expected_object_names):
        job_id = object_group.delete()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        existing_objects = self.bucket.list_objects(
            prefix=self.obj_prefix
        ).get_entries()
        self.assertEqual(len(expected_object_names), len(existing_objects))

        existing_object_names = [x.name for x in existing_objects]
        self.assertEqual(expected_object_names, existing_object_names)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict_list(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        self._evict_test_helper(object_group, [0], 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict_range(self):
        object_group = self.bucket.objects(obj_range=self.obj_range)
        cached = list(range(0, 11, 2))
        cached.append(9)
        self._evict_test_helper(object_group, cached, 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict_template(self):
        object_group = self.bucket.objects(obj_template=self.obj_template)
        cached = list(range(0, 11, 2))
        cached.append(9)
        self._evict_test_helper(object_group, cached, 10)

    def _evict_test_helper(self, object_group, expected_cached, expected_total):
        job_id = object_group.evict()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        self._verify_cached_objects(expected_total, expected_cached)

    def test_evict_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=PROVIDER_AIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).evict()
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_range=self.obj_range).evict()

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_list(self):
        obj_group = self.bucket.objects(obj_names=self.obj_names[1:])
        self._prefetch_test_helper(obj_group, range(1, 10), 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_range(self):
        obj_group = self.bucket.objects(obj_range=self.obj_range)
        cached = list(range(1, 8, 2))
        self._prefetch_test_helper(obj_group, cached, 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_template(self):
        obj_group = self.bucket.objects(obj_template=self.obj_template)
        cached = list(range(1, 8, 2))
        self._prefetch_test_helper(obj_group, cached, 10)

    def _prefetch_test_helper(self, object_group, expected_cached, expected_total):
        self._evict_all_objects()
        # Fetch back a specific object group and verify cache status
        job_id = object_group.prefetch()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        self._verify_cached_objects(expected_total, expected_cached)

    def test_prefetch_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=PROVIDER_AIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).prefetch()
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_range=self.obj_range).prefetch()

    def test_copy_objects(self):
        to_bck_name = "destination-bucket"
        to_bck = self._create_bucket(to_bck_name)
        self.assertEqual(0, len(to_bck.list_all_objects(prefix=self.obj_prefix)))
        self.assertEqual(10, len(self.bucket.list_all_objects(prefix=self.obj_prefix)))

        copy_job = self.bucket.objects(obj_range=self.obj_range).copy(to_bck.name)
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT)

        self.assertEqual(4, len(to_bck.list_all_objects(prefix=self.obj_prefix)))

    def _evict_all_objects(self):
        job_id = self.bucket.objects(obj_names=self.obj_names).evict()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        self._verify_cached_objects(10, [])

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

        # All even numbers within the range should be cached
        cached_names = {
            self.obj_prefix + str(x) + self.obj_suffix for x in cached_range
        }
        cached_objs = []
        evicted_objs = []
        for obj in objects:
            if obj.name in cached_names:
                cached_objs.append(obj)
            else:
                evicted_objs.append(obj)
        self._validate_objects_cached(cached_objs, True)
        self._validate_objects_cached(evicted_objs, False)
