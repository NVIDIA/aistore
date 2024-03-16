#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
import unittest
import random

from tests.integration import REMOTE_SET
from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.const import STRESS_TEST_OBJECT_COUNT, TEST_TIMEOUT


# pylint: disable=unused-variable,too-many-instance-attributes
class TestBucketOpsStress(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        if REMOTE_SET:
            self.s3_client = self._get_boto3_client()

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_stress_copy_objects_sync_flag(self):
        obj_names = self._create_objects(
            num_obj=STRESS_TEST_OBJECT_COUNT, suffix="-suffix"
        )
        to_bck_name = "dst-bck-cp-sync"
        to_bck = self._create_bucket(to_bck_name)

        obj_group = self.bucket.objects(obj_names=obj_names)
        # self._evict_all_objects()

        # cache and verify
        job_id = obj_group.prefetch()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT * 2)
        self._verify_cached_objects(
            STRESS_TEST_OBJECT_COUNT, range(STRESS_TEST_OBJECT_COUNT)
        )

        # copy objs to dst bck
        copy_job = self.bucket.copy(prefix_filter=self.obj_prefix, to_bck=to_bck)
        # copy_job = self.bucket.objects(obj_names=self.obj_names).copy(to_bck=to_bck)
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT)
        self.assertEqual(STRESS_TEST_OBJECT_COUNT, len(to_bck.list_all_objects()))

        # randomly delete 10% of the objects
        to_delete = random.sample(obj_names, int(STRESS_TEST_OBJECT_COUNT * 0.1))

        # out of band delete
        for obj_name in to_delete:
            self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # test --sync flag
        copy_job = self.bucket.copy(
            prefix_filter=self.obj_prefix, to_bck=to_bck, sync=True
        )
        self.client.job(job_id=copy_job).wait_for_idle(timeout=TEST_TIMEOUT * 3)
        self.assertEqual(
            int(STRESS_TEST_OBJECT_COUNT * 0.9), len(to_bck.list_all_objects())
        )
