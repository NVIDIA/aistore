#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import unittest

from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.integration import REMOTE_SET, TEST_TIMEOUT, OBJECT_COUNT


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


if __name__ == "__main__":
    unittest.main()
