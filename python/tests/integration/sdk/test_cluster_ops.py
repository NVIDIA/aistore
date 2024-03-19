#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

# Default provider is AIS, so all Cloud-related tests are skipped.

import unittest

from aistore.sdk import Client
from aistore.sdk.const import ACT_COPY_OBJECTS
from aistore.sdk.types import ClusterPerformance
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import random_string


class TestClusterOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.client = Client(CLUSTER_ENDPOINT)
        self.cluster = self.client.cluster()

    def test_health_success(self):
        self.assertTrue(self.cluster.is_ready())

    def test_health_failure(self):
        # url not existing or URL down
        self.assertFalse(Client("http://localhost:1234").cluster().is_ready())

    def test_cluster_map(self):
        smap = self.cluster.get_info()

        self.assertIsNotNone(smap)
        self.assertIsNotNone(smap.proxy_si)
        self.assertNotEqual(len(smap.pmap), 0)
        self.assertNotEqual(len(smap.tmap), 0)
        self.assertNotEqual(smap.version, 0)
        self.assertIsNot(smap.uuid, "")

    def _check_jobs_in_result(self, expected_jobs, res, missing_jobs=None):
        job_ids = [job.uuid for job in res]
        for job in expected_jobs:
            self.assertTrue(job in job_ids)
        if not missing_jobs:
            return
        for job in missing_jobs:
            self.assertFalse(job in job_ids)

    def test_list_jobs_status(self):
        job_kind = "lru"

        job_1_id = self.client.job(job_kind=job_kind).start()
        job_2_id = self.client.job(job_kind=job_kind).start()
        job_3_id = self.client.job(job_kind="cleanup").start()

        self._check_jobs_in_result(
            [job_1_id, job_2_id], self.cluster.list_jobs_status()
        )
        self._check_jobs_in_result(
            [job_1_id, job_2_id],
            self.cluster.list_jobs_status(job_kind=job_kind),
            [job_3_id],
        )

    def test_list_running_jobs(self):
        # First generate a multi-obj copy job that will stay "running" (but idle) long enough to query
        bck_name = random_string()
        new_bck_name = random_string()
        obj_name = random_string()
        bck = self.client.bucket(bck_name).create()
        new_bck = self.client.bucket(new_bck_name).create()
        try:
            bck.object(obj_name).put_content("any content")
            idle_job = bck.objects(obj_names=[obj_name]).copy(to_bck=new_bck)

            expected_res = f"{ACT_COPY_OBJECTS}[{idle_job}]"
            self.assertIn(expected_res, self.client.cluster().list_running_jobs())
            self.assertIn(
                expected_res,
                self.cluster.list_running_jobs(job_kind=ACT_COPY_OBJECTS),
            )
            self.assertNotIn(
                expected_res, self.cluster.list_running_jobs(job_kind="lru")
            )
        finally:
            bck.delete()
            new_bck.delete()

    def test_get_performance(self):
        smap = self.cluster.get_info()
        performance = self.cluster.get_performance()

        self.assertIsInstance(performance, ClusterPerformance)

        num_targets_in_smap = len(smap.tmap)

        self.assertEqual(num_targets_in_smap, len(performance.throughput))
        self.assertEqual(num_targets_in_smap, len(performance.latency))
        self.assertEqual(num_targets_in_smap, len(performance.counters))

        for target_id in smap.tmap:
            self.assertIn(target_id, performance.throughput)
            self.assertIn(target_id, performance.latency)
            self.assertIn(target_id, performance.counters)
