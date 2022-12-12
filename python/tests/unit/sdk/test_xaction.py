import unittest
from unittest.mock import Mock, patch, call

from aistore.sdk.bucket import Bucket
from aistore.sdk.const import QParamWhat, HTTP_METHOD_GET, HTTP_METHOD_PUT
from aistore.sdk.errors import Timeout
from aistore.sdk.types import XactStatus
from aistore.sdk.utils import probing_frequency
from aistore.sdk.xaction import Xaction


# pylint: disable=unused-variable
class TestXaction(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.xaction = Xaction(self.mock_client)

    def test_properties(self):
        self.assertEqual(self.mock_client, self.xaction.client)

    def test_xact_status_default_params(self):
        expected_request_val = {
            "id": "",
            "kind": "",
            "show_active": False,
            "node": "",
        }
        self.xact_status_exec_assert(expected_request_val)

    def test_xact_status(self):
        xact_id = "xact uuid"
        xact_kind = "xact kind"
        daemon_id = "daemon id"
        only_running = True

        expected_request_val = {
            "id": xact_id,
            "kind": xact_kind,
            "show_active": only_running,
            "node": daemon_id,
        }
        self.xact_status_exec_assert(
            expected_request_val,
            xact_id=xact_id,
            xact_kind=xact_kind,
            daemon_id=daemon_id,
            only_running=only_running,
        )

    def xact_status_exec_assert(self, expected_request_val, **kwargs):
        returned_status = XactStatus()
        self.mock_client.request_deserialize.return_value = returned_status

        res = self.xaction.xact_status(**kwargs)

        self.assertEqual(returned_status, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path="cluster",
            res_model=XactStatus,
            json=expected_request_val,
            params={QParamWhat: "status"},
        )

    @patch("aistore.sdk.xaction.time.sleep")
    @patch("aistore.sdk.xaction.Xaction.xact_status")
    def test_wait_for_xaction_finished_default_params(self, mock_status, mock_sleep):
        xact_id = ""
        xact_kind = ""
        daemon_id = ""
        timeout = 300
        frequency = probing_frequency(timeout)
        expected_status_calls = [
            call(xact_id=xact_id, xact_kind=xact_kind, daemon_id=daemon_id),
            call(xact_id=xact_id, xact_kind=xact_kind, daemon_id=daemon_id),
            call(xact_id=xact_id, xact_kind=xact_kind, daemon_id=daemon_id),
        ]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self.wait_for_xaction_finished_exec_assert(
            mock_status, mock_sleep, expected_status_calls, expected_sleep_calls
        )

    @patch("aistore.sdk.xaction.time.sleep")
    @patch("aistore.sdk.xaction.Xaction.xact_status")
    def test_wait_for_xaction_finished(self, mock_status, mock_sleep):
        xact_id = "xaction id"
        xact_kind = "xaction kind"
        daemon_id = "daemon id"
        timeout = 20
        frequency = probing_frequency(timeout)
        expected_status_calls = [
            call(xact_id=xact_id, xact_kind=xact_kind, daemon_id=daemon_id),
            call(xact_id=xact_id, xact_kind=xact_kind, daemon_id=daemon_id),
            call(xact_id=xact_id, xact_kind=xact_kind, daemon_id=daemon_id),
        ]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self.wait_for_xaction_finished_exec_assert(
            mock_status,
            mock_sleep,
            expected_status_calls,
            expected_sleep_calls,
            xact_id=xact_id,
            xact_kind=xact_kind,
            daemon_id=daemon_id,
            timeout=timeout,
        )

    @patch("aistore.sdk.xaction.time.sleep")
    @patch("aistore.sdk.xaction.Xaction.xact_status")
    # pylint: disable=unused-argument
    def test_wait_for_xaction_finished_timeout(self, mock_status, mock_sleep):
        mock_status.return_value = XactStatus(end_time=0)

        self.assertRaises(Timeout, self.xaction.wait_for_xaction_finished)

    def wait_for_xaction_finished_exec_assert(
        self,
        mock_status,
        mock_sleep,
        expected_status_calls,
        expected_sleep_calls,
        **kwargs
    ):
        mock_status.side_effect = [
            XactStatus(end_time=0),
            XactStatus(end_time=0),
            XactStatus(end_time=1),
        ]

        self.xaction.wait_for_xaction_finished(**kwargs)

        mock_status.assert_has_calls(expected_status_calls)
        mock_sleep.assert_has_calls(expected_sleep_calls)
        self.assertEqual(3, mock_status.call_count)
        self.assertEqual(2, mock_sleep.call_count)

    def test_xact_start(self):
        xact_kind = "xaction kind"
        daemon_id = "daemon id"
        buckets = [Bucket(bck_name="name", client=Mock())]
        expected_act_value = {
            "kind": xact_kind,
            "node": daemon_id,
            "buckets": buckets,
            "ext": {"force": True},
        }
        self.xact_start_exec_assert(
            expected_act_value,
            xact_kind=xact_kind,
            daemon_id=daemon_id,
            force=True,
            buckets=buckets,
        )

    def test_xact_start_default_params(self):
        expected_act_value = {"kind": "", "node": "", "buckets": None}
        self.xact_start_exec_assert(expected_act_value)

    def xact_start_exec_assert(self, expected_act_value, **kwargs):
        expected_action = {"action": "start", "value": expected_act_value}
        response_txt = "response"
        response = Mock()
        response.text = response_txt
        self.mock_client.request.return_value = response
        res = self.xaction.xact_start(**kwargs)
        self.assertEqual(response_txt, res)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT, path="cluster", json=expected_action
        )
