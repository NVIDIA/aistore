#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import random
import unittest

from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.integration import REMOTE_SET, TEST_TIMEOUT


class TestJobOps(RemoteEnabledTest):  # pylint: disable=unused-variable
    def test_job_start_wait(self):
        job_id = self.client.job(job_kind="lru").start()
        self.client.job(job_id=job_id).wait()
        self.assertNotEqual(0, self.client.job(job_id=job_id).status().end_time)

    def test_job_wait_for_idle(self):
        # Put random number of objects to bucket, assert success
        obj_names = self._create_objects(random.randint(2, 10))
        existing_names = [
            obj.name for obj in self.bucket.list_objects(prefix=self.obj_prefix).entries
        ]
        self.assertEqual(obj_names, existing_names)

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
        num_obj = random.randint(5, 20)
        obj_names = self._create_objects(num_obj)
        obj_group = self.bucket.objects(obj_names=obj_names)
        job_id = obj_group.evict()
        self.client.job(job_id).wait_for_idle(timeout=TEST_TIMEOUT)
        self._check_all_objects_cached(num_obj, False)
        job_id = obj_group.prefetch()
        self.client.job(job_id).wait_for_idle(timeout=TEST_TIMEOUT)
        self._check_all_objects_cached(num_obj, True)

    def test_job_wait(self):
        new_bck_name = "renamed-bucket"
        first_bck = self._create_bucket("first_bck")
        # Add here so we clean up after renaming
        self.buckets.append(new_bck_name)
        job_id = first_bck.rename(new_bck_name)
        self.client.job(job_id=job_id).wait()
        # Will raise exception if the renamed bucket has not been created (job did not finish)
        self.client.bucket(new_bck_name).list_objects()


if __name__ == "__main__":
    unittest.main()
