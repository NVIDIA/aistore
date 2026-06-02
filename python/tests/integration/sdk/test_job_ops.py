#
# Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

from datetime import datetime, timedelta, timezone
import time
import unittest

import pytest

from aistore.sdk.xact_const import (
    XACT_KIND_COPY_OBJECTS,
    XACT_KIND_GET_BATCH,
    XACT_KIND_LRU,
)
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.integration import REMOTE_SET
from tests.const import TEST_TIMEOUT


class TestJobOps(ParallelTestBase):  # pylint: disable=unused-variable
    @pytest.mark.nonparallel("lru incompatible with some job types")
    def test_job_start_wait(self):
        job_id = self.client.job(job_kind="lru").start()
        result = self.client.job(job_id=job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)
        self.assertNotEqual(0, self.client.job(job_id=job_id).status().end_time)

    def test_job_wait_for_idle(self):
        obj_names = list(self._create_objects().keys())
        existing_names = {
            obj.name for obj in self.bucket.list_objects(prefix=self.obj_prefix).entries
        }
        self.assertEqual(set(obj_names), existing_names)

        # Start a deletion job that will reach an finished state
        job_id = self.bucket.objects(obj_names=obj_names).delete()
        result = self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)

        self.assertEqual(
            0, len(self.bucket.list_objects(prefix=self.obj_prefix).entries)
        )

    def test_job_wait(self):
        object_names = list(self._create_objects().keys())
        # Delete does not idle when finished
        job_id = self.bucket.objects(obj_names=object_names).delete()
        result = self.client.job(job_id=job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)
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
        result = self.client.job(evict_job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)
        self.assertFalse(obj.props.present)

        job_id = obj.blob_download()
        self.assertNotEqual(job_id, "")
        result = self.client.job(job_id=job_id).wait_single_node(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)

        self.assertTrue(obj.props.present)

    def test_job_wait_dispatch_idle_kind(self):
        """`wait()` with an idle kind (copy-listrange) must converge via
        the snaps/idle path, not block forever on terminal `end_time`."""
        obj_names = list(self._create_objects().keys())
        to_bck = self._create_bucket(prefix="wait-dispatch-dst")
        obj_group = self.bucket.objects(obj_names=obj_names)
        copy_job_ids = obj_group.copy(to_bck)
        self.assertTrue(copy_job_ids, "expected at least one copy-listrange job id")

        for job_id in copy_job_ids:
            result = self.client.job(
                job_id=job_id, job_kind=XACT_KIND_COPY_OBJECTS
            ).wait(timeout=TEST_TIMEOUT)
            self.assertTrue(
                result.success,
                f"copy-listrange wait failed: {result.error}",
            )

        self.assertEqual(len(obj_names), len(to_bck.list_all_objects()))

    @pytest.mark.nonparallel("lru incompatible with some job types")
    def test_job_wait_dispatch_terminal_kind(self):
        """`wait()` with a non-idle kind (lru) must follow the terminal
        (`end_time`) path -- same observable behavior as before convergence."""
        job_id = self.client.job(job_kind=XACT_KIND_LRU).start()
        result = self.client.job(job_id=job_id, job_kind=XACT_KIND_LRU).wait(
            timeout=TEST_TIMEOUT
        )
        self.assertTrue(result.success)
        self.assertNotEqual(0, self.client.job(job_id=job_id).status().end_time)

    #
    # get-batch (x-moss) is an idle kind; exercise the descriptor-aware
    # wait() against a real running job.
    #

    def _get_batch_job_ids(self) -> set:
        """Return the set of get-batch xaction ids currently known to the cluster."""
        details = self.client.job(job_kind=XACT_KIND_GET_BATCH).get_details()
        return {snap.id for snap in details.list_snapshots() if snap.id}

    def _await_new_get_batch_id(self, before: set, timeout: int = TEST_TIMEOUT) -> str:
        """Discover the id of the get-batch xaction that started after `before`
        was captured. Scoping the subsequent wait()/abort() to a specific id
        keeps them robust against lingering (idle or aborted) get-batch
        snapshots left behind by other jobs."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            new_ids = self._get_batch_job_ids() - before
            if new_ids:
                return next(iter(new_ids))
            time.sleep(0.1)
        raise AssertionError("did not observe a new get-batch job id")

    @pytest.mark.nonparallel("get-batch wait/abort dispatch")
    def test_job_wait_get_batch_finishes(self):
        """`wait()` on a get-batch (idle kind) converges via the snaps/idle
        path once the batch completes, returning success."""
        obj_names = list(self._create_objects().keys())
        before = self._get_batch_job_ids()

        batch = self.client.batch(
            objects=obj_names, bucket=self.bucket, streaming_get=True
        )
        results = list(batch.get())  # consume fully -> xaction goes idle
        self.assertEqual(len(obj_names), len(results))

        job_id = self._await_new_get_batch_id(before)
        result = self.client.job(job_id=job_id, job_kind=XACT_KIND_GET_BATCH).wait(
            timeout=TEST_TIMEOUT
        )
        self.assertTrue(result.success, f"get-batch wait failed: {result.error}")

    @pytest.mark.nonparallel("lru incompatible with some job types")
    def test_get_within_timeframe(self):
        start_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        job_id = self.client.job(job_kind="lru").start()
        result = self.client.job(job_id=job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)
        end_time = datetime.now(timezone.utc) + timedelta(seconds=1)
        self.assertNotEqual(job_id, "")
        jobs_list = self.client.job(job_id=job_id).get_within_timeframe(
            start_time=start_time, end_time=end_time
        )
        matching_job_ids = {snap.id for snap in jobs_list}

        self.assertGreater(len(matching_job_ids), 0)
        self.assertTrue(job_id in matching_job_ids)
