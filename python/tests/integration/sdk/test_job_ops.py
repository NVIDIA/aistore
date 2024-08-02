#
# Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
#
from datetime import datetime
import unittest

from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.integration import REMOTE_SET
from tests.const import TEST_TIMEOUT, OBJECT_COUNT


class TestJobOps(RemoteEnabledTest):  # pylint: disable=unused-variable
    def test_job_start_wait(self):
        job_id = self.client.job(job_kind="lru").start()
        self.client.job(job_id=job_id).wait()
        self.assertNotEqual(0, self.client.job(job_id=job_id).status().end_time)

    def test_job_wait_for_idle(self):
        obj_names = self._create_objects()
        existing_names = {
            obj.name for obj in self.bucket.list_objects(prefix=self.obj_prefix).entries
        }
        self.assertEqual(set(obj_names), existing_names)

        # Start a deletion job that will reach an idle state when it finishes
        job_id = self.bucket.objects(obj_names=obj_names).delete()
        self.client.job(job_id).wait_for_idle(timeout=TEST_TIMEOUT)

        self.assertEqual(
            0, len(self.bucket.list_objects(prefix=self.obj_prefix).entries)
        )

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_async_job_wait_for_idle(self):
        obj_names = self._create_objects()
        obj_group = self.bucket.objects(obj_names=obj_names)
        job_id = obj_group.evict()
        self.client.job(job_id).wait_for_idle(timeout=TEST_TIMEOUT)
        self._check_all_objects_cached(OBJECT_COUNT, False)
        job_id = obj_group.prefetch()
        self.client.job(job_id).wait_for_idle(timeout=TEST_TIMEOUT)
        self._check_all_objects_cached(OBJECT_COUNT, True)

    def test_job_wait(self):
        object_names = self._create_objects()
        # Delete does not idle when finished
        job_id = self.bucket.objects(obj_names=object_names).delete()
        self.client.job(job_id=job_id).wait(timeout=TEST_TIMEOUT)
        # Check that objects do not exist
        existing_obj = [entry.name for entry in self.bucket.list_all_objects()]
        for name in object_names:
            self.assertNotIn(name, existing_obj)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_job_wait_single_node(self):
        obj_name, _ = self._create_object_with_content()

        evict_job_id = self.bucket.objects(obj_names=[obj_name]).evict()
        self.client.job(evict_job_id).wait(timeout=TEST_TIMEOUT)

        job_id = self.bucket.object(obj_name).blob_download()
        self.assertNotEqual(job_id, "")
        self.client.job(job_id=job_id).wait_single_node(timeout=TEST_TIMEOUT)

        objects = self.bucket.list_objects(props="name,cached", prefix=obj_name).entries
        self._validate_objects_cached(objects, True)

    def test_get_within_timeframe(self):
        start_time = datetime.now().time()
        job_id = self.client.job(job_kind="lru").start()
        self.client.job(job_id=job_id).wait()
        end_time = datetime.now().time()
        self.assertNotEqual(job_id, "")
        jobs_list = self.client.job(job_id=job_id).get_within_timeframe(
            start_time=start_time, end_time=end_time
        )

        self.assertTrue(len(jobs_list) > 0)
