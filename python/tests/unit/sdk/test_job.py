import unittest
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from unittest.mock import Mock, patch, call

from aistore.sdk import Bucket
from aistore.sdk.const import (
    QPARAM_WHAT,
    QPARAM_FORCE,
    HTTP_METHOD_GET,
    HTTP_METHOD_PUT,
    WHAT_ONE_XACT_STATUS,
    URL_PATH_CLUSTER,
    ACT_START,
    WHAT_QUERY_XACT_STATS,
)
from aistore.sdk.errors import Timeout, JobInfoNotFound
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import JobStatus, JobArgs, ActionMsg, JobSnapshot
from aistore.sdk.utils import probing_frequency
from aistore.sdk.job import Job


# pylint: disable=unused-variable, too-many-public-methods
class TestJob(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.job_id = "1234"
        self.job_kind = "test job"
        self.default_job = Job(self.mock_client)
        self.job = Job(self.mock_client, self.job_id, self.job_kind)

    def test_properties(self):
        self.assertEqual(self.job_id, self.job.job_id)
        self.assertEqual(self.job_kind, self.job.job_kind)

    def test_job_status(self):
        expected_request_val = JobArgs(id=self.job_id, kind=self.job_kind).as_dict()
        returned_status = JobStatus()
        self.mock_client.request_deserialize.return_value = returned_status

        res = self.job.status()

        self.assertEqual(returned_status, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=JobStatus,
            json=expected_request_val,
            params={QPARAM_WHAT: WHAT_ONE_XACT_STATUS},
        )

    def test_job_status_no_id(self):
        job_no_id = Job(self.mock_client)
        with self.assertRaises(ValueError):
            job_no_id.status()

    @patch("aistore.sdk.job.time.sleep")
    @patch("aistore.sdk.job.Job.status")
    def test_wait_default_params(self, mock_status, mock_sleep):
        timeout = 300
        frequency = probing_frequency(timeout)
        expected_status_calls = [
            call(),
            call(),
            call(),
        ]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self.wait_exec_assert(
            self.default_job,
            mock_status,
            mock_sleep,
            expected_status_calls,
            expected_sleep_calls,
        )

    @patch("aistore.sdk.job.time.sleep")
    @patch("aistore.sdk.job.Job.status")
    def test_wait(self, mock_status, mock_sleep):
        timeout = 20
        frequency = probing_frequency(timeout)
        expected_status_calls = [call(), call(), call()]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self.wait_exec_assert(
            self.job,
            mock_status,
            mock_sleep,
            expected_status_calls,
            expected_sleep_calls,
            timeout=timeout,
        )

    @patch("aistore.sdk.job.time.sleep")
    @patch("aistore.sdk.job.Job.status")
    # pylint: disable=unused-argument
    def test_wait_timeout(self, mock_status, mock_sleep):
        mock_status.return_value = JobStatus(end_time=0)

        self.assertRaises(Timeout, self.job.wait)

    # pylint: disable=too-many-arguments
    def wait_exec_assert(
        self,
        job,
        mock_status,
        mock_sleep,
        expected_status_calls,
        expected_sleep_calls,
        **kwargs,
    ):
        mock_status.side_effect = [
            JobStatus(end_time=0),
            JobStatus(end_time=0),
            JobStatus(end_time=1),
        ]

        job.wait(**kwargs)

        mock_status.assert_has_calls(expected_status_calls)
        mock_sleep.assert_has_calls(expected_sleep_calls)
        self.assertEqual(3, mock_status.call_count)
        self.assertEqual(2, mock_sleep.call_count)

    @patch("aistore.sdk.job.time.sleep")
    def test_wait_for_idle(self, mock_sleep):
        snap_other_job_idle = JobSnapshot(id="other_id", is_idle=True)
        snap_job_running = JobSnapshot(id=self.job_id, is_idle=False)
        snap_job_idle = JobSnapshot(id=self.job_id, is_idle=True)
        self.mock_client.request_deserialize.side_effect = [
            {"d1": [snap_other_job_idle, snap_job_running], "d2": [snap_job_running]},
            {"d1": [snap_job_running], "d2": [snap_job_idle]},
            {"d1": [snap_job_idle], "d2": [snap_job_idle]},
        ]
        timeout = 20
        frequency = probing_frequency(timeout)
        expected_request_val = JobArgs(id=self.job_id, kind=self.job_kind).as_dict()
        expected_request_params = {QPARAM_WHAT: WHAT_QUERY_XACT_STATS}
        expected_call = call(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            json=expected_request_val,
            params=expected_request_params,
            res_model=Dict[str, List[JobSnapshot]],
        )

        expected_client_requests = [expected_call for _ in range(2)]
        expected_sleep_calls = [call(frequency), call(frequency)]

        self.job.wait_for_idle(timeout=timeout)

        self.mock_client.request_deserialize.assert_has_calls(expected_client_requests)
        mock_sleep.assert_has_calls(expected_sleep_calls)
        self.assertEqual(3, self.mock_client.request_deserialize.call_count)
        self.assertEqual(2, mock_sleep.call_count)

    @patch("aistore.sdk.job.time.sleep")
    # pylint: disable=unused-argument
    def test_wait_for_idle_timeout(self, mock_sleep):
        res = {
            "d1": [JobSnapshot(id=self.job_id, is_idle=True)],
            "d2": [JobSnapshot(id=self.job_id, is_idle=False)],
        }
        self.mock_client.request_deserialize.return_value = res
        self.assertRaises(Timeout, self.job.wait_for_idle)

    @patch("aistore.sdk.job.time.sleep")
    # pylint: disable=unused-argument
    def test_wait_for_idle_no_snapshots(self, mock_sleep):
        self.mock_client.request_deserialize.return_value = {}
        with self.assertRaises(Timeout) as exc:
            self.job.wait_for_idle()
        self.assertEqual(
            "Timed out while waiting for job '1234' to reach idle state. No job information found.",
            str(exc.exception.args[0]),
        )

    @patch("aistore.sdk.job.time.sleep")
    # pylint: disable=unused-argument
    def test_wait_for_idle_no_job_in_snapshots(self, mock_sleep):
        res = {
            "d1": [JobSnapshot(id="1"), JobSnapshot(id="2")],
            "d2": [JobSnapshot(id="2")],
        }
        self.mock_client.request_deserialize.return_value = res
        with self.assertRaises(Timeout) as exc:
            self.job.wait_for_idle()
        self.assertEqual(
            "Timed out while waiting for job '1234' to reach idle state. ",
            str(exc.exception.args[0]),
        )

    def test_job_start_single_bucket(self):
        daemon_id = "daemon id"
        bucket = Bucket(
            client=Mock(RequestClient),
            name="single bucket",
        )
        expected_json = JobArgs(
            kind=self.job_kind, daemon_id=daemon_id, bucket=bucket.as_model()
        ).as_dict()
        self.job_start_exec_assert(
            self.job,
            expected_json,
            {QPARAM_FORCE: "true"},
            daemon_id=daemon_id,
            force=True,
            buckets=[bucket],
        )

    def test_job_start_bucket_list(self):
        daemon_id = "daemon id"
        buckets = [
            Bucket(
                client=Mock(RequestClient),
                name="first bucket",
            ),
            Bucket(
                client=Mock(RequestClient),
                name="second bucket",
            ),
        ]
        expected_json = JobArgs(
            kind=self.job_kind,
            daemon_id=daemon_id,
            buckets=[bucket.as_model() for bucket in buckets],
        ).as_dict()
        self.job_start_exec_assert(
            self.job,
            expected_json,
            {QPARAM_FORCE: "true"},
            daemon_id=daemon_id,
            force=True,
            buckets=buckets,
        )

    def test_job_start_default_params(self):
        expected_act_value = JobArgs().as_dict()
        self.job_start_exec_assert(self.default_job, expected_act_value, {})

    def job_start_exec_assert(self, job, expected_json, expected_params, **kwargs):
        expected_action = ActionMsg(action=ACT_START, value=expected_json).dict()
        response_txt = "response"
        response = Mock()
        response.text = response_txt
        self.mock_client.request.return_value = response
        res = job.start(**kwargs)
        self.assertEqual(response_txt, res)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=URL_PATH_CLUSTER,
            json=expected_action,
            params=expected_params,
        )

    @patch("aistore.sdk.job.time.sleep", Mock())
    def test_wait_single_node_finishes_successfully(self):
        finished_snapshot = {
            "key": [
                JobSnapshot(
                    id=self.job_id,
                    is_idle=True,
                    end_time="2024-01-01T00:00:00Z",
                    aborted=False,
                )
            ]
        }
        self.mock_client.request_deserialize.return_value = finished_snapshot

        self.job.wait_single_node()

        self.mock_client.request_deserialize.assert_called()
        self.assertEqual(self.mock_client.request_deserialize.call_count, 1)

    @patch("aistore.sdk.job.time.sleep", Mock())
    def test_wait_single_node_is_aborted(self):
        aborted_snapshot = {
            "key": [
                JobSnapshot(
                    id=self.job_id,
                    is_idle=True,
                    end_time="2024-01-01T00:00:00Z",
                    aborted=True,
                )
            ]
        }
        self.mock_client.request_deserialize.return_value = aborted_snapshot

        self.job.wait_single_node()
        self.mock_client.request_deserialize.assert_called()

    @patch("aistore.sdk.job.time.sleep", Mock())
    def test_wait_single_node_timeout(self):
        ongoing_snapshots = {
            "key": [
                JobSnapshot(
                    id=self.job_id,
                    is_idle=False,
                    end_time="0001-01-01T00:00:00Z",
                    aborted=False,
                )
            ]
        }
        self.mock_client.request_deserialize.return_value = ongoing_snapshots

        with self.assertRaises(Timeout):
            self.job.wait_single_node()

        self.mock_client.request_deserialize.assert_called()

    def test_get_within_timeframe_found_jobs(self):
        start_time = datetime.now(timezone.utc) - timedelta(days=1)
        end_time = datetime.now(timezone.utc)

        mock_snapshots = [
            JobSnapshot(
                id="1234",
                kind="test job",
                start_time=(start_time.isoformat() + "Z"),
                end_time=(end_time.isoformat() + "Z"),
                aborted=False,
                is_idle=True,
            )
        ]

        self.mock_client.request_deserialize.return_value = {"key": mock_snapshots}

        found_jobs = self.job.get_within_timeframe(start_time, end_time)

        self.assertEqual(len(found_jobs), len(mock_snapshots))
        for found_job, expected_snapshot in zip(found_jobs, mock_snapshots):
            self.assertEqual(found_job.id, expected_snapshot.id)
            self.assertEqual(found_job.start_time, expected_snapshot.start_time)
            self.assertEqual(found_job.end_time, expected_snapshot.end_time)

    def test_get_within_timeframe_no_jobs_found(self):
        start_time = datetime.now(timezone.utc) - timedelta(days=1)
        end_time = datetime.now(timezone.utc)
        self.mock_client.request_deserialize.return_value = {}

        with self.assertRaises(JobInfoNotFound):
            self.job.get_within_timeframe(start_time, end_time)
