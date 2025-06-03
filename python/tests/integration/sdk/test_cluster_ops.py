#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#
import pytest

from aistore.sdk import Client
from aistore.sdk.const import ACT_COPY_OBJECTS
from tests.integration.sdk import TEST_RETRY_CONFIG
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.utils import random_string


class TestClusterOps(ParallelTestBase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        super().setUp()
        self.cluster = self.client.cluster()

    def test_health_success(self):
        self.assertTrue(self.cluster.is_ready())

    def test_health_failure(self):
        self.assertFalse(
            Client("http://localhost:1234", retry_config=TEST_RETRY_CONFIG)
            .cluster()
            .is_ready()
        )

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

    @pytest.mark.nonparallel("does not work with existing copy-listrange jobs")
    def test_list_running_jobs(self):
        # First generate a multi-obj copy job that will stay "running" (but idle) long enough to query
        bck = self._create_bucket()
        dest_bck = self._create_bucket()
        obj_name = random_string()
        bck.object(obj_name).get_writer().put_content(b"any content")
        idle_job_ids = bck.objects(obj_names=[obj_name]).copy(to_bck=dest_bck)
        idle_job = idle_job_ids[0] if idle_job_ids else ""

        expected_res = f"{ACT_COPY_OBJECTS}[{idle_job}]"
        self.assertIn(expected_res, self.client.cluster().list_running_jobs())
        self.assertIn(
            expected_res,
            self.cluster.list_running_jobs(job_kind=ACT_COPY_OBJECTS),
        )
        self.assertNotIn(expected_res, self.cluster.list_running_jobs(job_kind="lru"))

    def test_get_performance(self):
        smap = self.cluster.get_info()
        performance = self.cluster.get_performance()

        self.assertIsInstance(performance, dict)
        self.assertEqual(len(smap.tmap), len(performance))
        for target_id in smap.tmap:
            self.assertIn(target_id, performance)
            self.assertIsInstance(performance[target_id], dict)
