#
# Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
#
from datetime import datetime, timedelta, timezone
import unittest

import pytest

from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.integration import REMOTE_SET
from tests.const import TEST_TIMEOUT


class TestJobOps(ParallelTestBase):  # pylint: disable=unused-variable
    @pytest.mark.nonparallel("lru incompatible with some job types")
    def test_job_start_wait(self):
        job_id = self.client.job(job_kind="lru").start()
        self.client.job(job_id=job_id).wait()
        self.assertNotEqual(0, self.client.job(job_id=job_id).status().end_time)

    def test_job_wait_for_idle(self):
        obj_names = list(self._create_objects().keys())
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

    def test_job_wait(self):
        object_names = list(self._create_objects().keys())
        # Delete does not idle when finished
        job_id = self.bucket.objects(obj_names=object_names).delete()
        self.client.job(job_id=job_id).wait(timeout=TEST_TIMEOUT)
        # Check that objects do not exist
        existing_obj = {entry.name for entry in self.bucket.list_all_objects()}
        self.assertTrue(set(object_names).isdisjoint(existing_obj))

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_job_wait_single_node(self):
        obj, _ = self._create_object_with_content()

        evict_job_id = self.bucket.objects(obj_names=[obj.name]).evict()
        self.client.job(evict_job_id).wait(timeout=TEST_TIMEOUT)
        self.assertFalse(obj.props.present)

        job_id = obj.blob_download()
        self.assertNotEqual(job_id, "")
        self.client.job(job_id=job_id).wait_single_node(timeout=TEST_TIMEOUT)

        self.assertTrue(obj.props.present)

    @pytest.mark.nonparallel("lru incompatible with some job types")
    def test_get_within_timeframe(self):
        start_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        job_id = self.client.job(job_kind="lru").start()
        self.client.job(job_id=job_id).wait()
        end_time = datetime.now(timezone.utc) + timedelta(seconds=1)
        self.assertNotEqual(job_id, "")
        jobs_list = self.client.job(job_id=job_id).get_within_timeframe(
            start_time=start_time, end_time=end_time
        )
        matching_job_ids = {snap.id for snap in jobs_list}

        self.assertGreater(len(matching_job_ids), 0)
        self.assertTrue(job_id in matching_job_ids)
