import unittest
from unittest.mock import Mock, patch, call

from aistore.sdk.const import (
    QParamWhat,
    QParamForce,
    HTTP_METHOD_GET,
    HTTP_METHOD_PUT,
    QParamStatus,
    URL_PATH_CLUSTER,
    ACT_START,
)
from aistore.sdk.errors import Timeout
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import JobStatus, JobArgs, BucketModel, ActionMsg
from aistore.sdk.utils import probing_frequency
from aistore.sdk.job import Job


# pylint: disable=unused-variable
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

    def test_job_status_default_params(self):
        expected_request_val = JobArgs().get_json()
        self.job_status_exec_assert(self.default_job, expected_request_val)

    def test_job_status(self):
        daemon_id = "daemon id"
        only_running = True

        expected_request_val = JobArgs(
            id=self.job_id,
            kind=self.job_kind,
            only_running=only_running,
            daemon_id=daemon_id,
        ).get_json()
        self.job_status_exec_assert(
            self.job,
            expected_request_val,
            daemon_id=daemon_id,
            only_running=only_running,
        )

    def job_status_exec_assert(self, job, expected_request_val, **kwargs):
        returned_status = JobStatus()
        self.mock_client.request_deserialize.return_value = returned_status

        res = job.status(**kwargs)

        self.assertEqual(returned_status, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=JobStatus,
            json=expected_request_val,
            params={QParamWhat: QParamStatus},
        )

    @patch("aistore.sdk.job.time.sleep")
    @patch("aistore.sdk.job.Job.status")
    def test_wait_default_params_default_job(self, mock_status, mock_sleep):
        daemon_id = ""
        timeout = 300
        frequency = probing_frequency(timeout)
        expected_status_calls = [
            call(daemon_id=daemon_id),
            call(daemon_id=daemon_id),
            call(daemon_id=daemon_id),
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
        daemon_id = "daemon id"
        timeout = 20
        frequency = probing_frequency(timeout)
        expected_status_calls = [
            call(daemon_id=daemon_id),
            call(daemon_id=daemon_id),
            call(daemon_id=daemon_id),
        ]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self.wait_exec_assert(
            self.job,
            mock_status,
            mock_sleep,
            expected_status_calls,
            expected_sleep_calls,
            daemon_id=daemon_id,
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
        **kwargs
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

    def test_job_start(self):
        daemon_id = "daemon id"
        buckets = [BucketModel(client=Mock(RequestClient), name="name")]
        expected_json = JobArgs(
            kind=self.job_kind, daemon_id=daemon_id, buckets=buckets
        ).get_json()
        self.job_start_exec_assert(
            self.job,
            expected_json,
            {QParamForce: "true"},
            daemon_id=daemon_id,
            force=True,
            buckets=buckets,
        )

    def test_job_start_default_params(self):
        expected_act_value = JobArgs().get_json()
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
