#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
import unittest
import random

from tests.integration import AWS_BUCKET
from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.const import STRESS_TEST_OBJECT_COUNT, TEST_TIMEOUT


class TestBucketOpsStress(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        if AWS_BUCKET:
            self.s3_client = self._get_boto3_client()

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_stress_copy_objects_sync_flag(self):
        num_obj = STRESS_TEST_OBJECT_COUNT
        obj_names = self._create_objects(num_obj=num_obj, suffix="-suffix")
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
