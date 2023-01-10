#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
import unittest

from aistore.sdk import Client
from aistore.sdk.const import ProviderAIS
from aistore.sdk.errors import InvalidBckProvider
from aistore.sdk.object_range import ObjectRange
from aistore.sdk.job import Job
from tests.integration import CLUSTER_ENDPOINT, REMOTE_BUCKET
from tests.utils import random_string, create_and_put_object

# If remote bucket is not set, skip all cloud-related tests
REMOTE_SET = REMOTE_BUCKET != "" and not REMOTE_BUCKET.startswith(ProviderAIS + ":")


class TestObjectGroupOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.client = Client(CLUSTER_ENDPOINT)
        self.obj_prefix = "test_object_group_prefix-"
        self.obj_suffix = "-suffix"
        self.obj_template = self.obj_prefix + "{1..8..2}" + self.obj_suffix

        if REMOTE_SET:
            self.cloud_objects = []
            self.provider, self.bck_name = REMOTE_BUCKET.split("://")
            self.bucket = self.client.bucket(self.bck_name, provider=self.provider)
        else:
            self.provider = ProviderAIS
            self.bck_name = random_string()
            self.bucket = self.client.bucket(self.bck_name)
            self.bucket.create()

        self._cleanup_objects()
        # Range selecting objects 1,3,5,7
        self.obj_range = ObjectRange(
            self.obj_prefix, 1, 8, step=2, suffix=self.obj_suffix
        )
        self.obj_names = self.create_object_list(
            self.obj_prefix, self.provider, self.bck_name, 10, self.obj_suffix
        )

    def _cleanup_objects(self):
        # Clean up any other objects created with the test prefix, potentially from aborted tests
        object_names = [
            x.name for x in self.bucket.list_objects(self.obj_prefix).get_entries()
        ]
        if len(object_names) > 0:
            job_id = self.bucket.objects(obj_names=object_names).delete()
            Job(self.client).wait_for_job(job_id=job_id, timeout=30)

    def tearDown(self) -> None:
        if REMOTE_SET:
            self.bucket.objects(obj_names=self.cloud_objects).delete()
        else:
            self.bucket.delete()

    def test_delete_list(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        self.delete_test_helper(object_group, [self.obj_names[0]])

    def test_delete_range(self):
        object_group = self.bucket.objects(obj_range=self.obj_range)
        expected_object_names = [
            self.obj_prefix + str(x) + self.obj_suffix for x in range(0, 9, 2)
        ]
        expected_object_names.append(self.obj_prefix + "9" + self.obj_suffix)
        self.delete_test_helper(object_group, expected_object_names)

    def test_delete_template(self):
        object_group = self.bucket.objects(obj_template=self.obj_template)
        expected_object_names = [
            self.obj_prefix + str(x) + self.obj_suffix for x in range(0, 9, 2)
        ]
        expected_object_names.append(self.obj_prefix + "9" + self.obj_suffix)
        self.delete_test_helper(object_group, expected_object_names)

    def delete_test_helper(self, object_group, expected_object_names):
        job_id = object_group.delete()
        Job(self.client).wait_for_job(job_id=job_id, timeout=30)
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
        self.evict_test_helper(object_group, [0], 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict_range(self):
        object_group = self.bucket.objects(obj_range=self.obj_range)
        cached = list(range(0, 11, 2))
        cached.append(9)
        self.evict_test_helper(object_group, cached, 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict_template(self):
        object_group = self.bucket.objects(obj_template=self.obj_template)
        cached = list(range(0, 11, 2))
        cached.append(9)
        self.evict_test_helper(object_group, cached, 10)

    def evict_test_helper(self, object_group, expected_cached, expected_total):
        job_id = object_group.evict()
        Job(self.client).wait_for_job(job_id=job_id, timeout=30)
        self.verify_cached_objects(expected_total, expected_cached)

    def test_evict_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=ProviderAIS)
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
        self.prefetch_test_helper(obj_group, range(1, 10), 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_range(self):
        obj_group = self.bucket.objects(obj_range=self.obj_range)
        cached = list(range(1, 8, 2))
        self.prefetch_test_helper(obj_group, cached, 10)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_template(self):
        obj_group = self.bucket.objects(obj_template=self.obj_template)
        cached = list(range(1, 8, 2))
        self.prefetch_test_helper(obj_group, cached, 10)

    def prefetch_test_helper(self, object_group, expected_cached, expected_total):
        self.evict_all_objects()
        # Fetch back a specific object group and verify cache status
        job_id = object_group.prefetch()
        Job(self.client).wait_for_job(job_id=job_id, timeout=30)
        self.verify_cached_objects(expected_total, expected_cached)

    def test_prefetch_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=ProviderAIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).prefetch()
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_range=self.obj_range).prefetch()

    def evict_all_objects(self):
        job_id = self.bucket.objects(obj_names=self.obj_names).evict()
        Job(self.client).wait_for_job(job_id=job_id, timeout=30)
        self.verify_cached_objects(10, [])

    def verify_cached_objects(self, expected_object_count, cached_range):
        """
        List each of the objects verify the correct count and that all objects matching
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
        for obj in objects:
            self.assertTrue(obj.is_ok())
            if obj.name in cached_names:
                self.assertTrue(obj.is_cached())
            else:
                self.assertFalse(obj.is_cached())

    def create_object_list(self, prefix, provider, bck_name, length, suffix=""):
        obj_names = [prefix + str(i) + suffix for i in range(length)]
        for obj_name in obj_names:
            if REMOTE_SET:
                self.cloud_objects.append(obj_name)
            create_and_put_object(
                self.client,
                bck_name=bck_name,
                provider=provider,
                obj_name=obj_name,
            )
        return obj_names
