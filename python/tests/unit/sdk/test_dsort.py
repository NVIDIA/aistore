import unittest
from typing import Dict
from unittest.mock import Mock, patch, mock_open, call

from aistore.sdk.const import (
    URL_PATH_DSORT,
    HTTP_METHOD_POST,
    DSORT_ABORT,
    HTTP_METHOD_DELETE,
    DSORT_UUID,
    HTTP_METHOD_GET,
)
from aistore.sdk.dsort import Dsort
from aistore.sdk.dsort_types import DsortMetrics
from aistore.sdk.errors import Timeout
from aistore.sdk.utils import probing_frequency


class TestDsort(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock()
        self.dsort_id = "123"
        self.dsort = Dsort(client=self.mock_client, dsort_id=self.dsort_id)

    def test_properties(self):
        self.assertEqual(self.dsort_id, self.dsort.dsort_id)

    @patch("aistore.sdk.dsort.validate_file")
    @patch("aistore.sdk.dsort.json")
    # pylint: disable=unused-argument
    def test_start(self, mock_json, mock_validate_file):
        new_id = "456"
        spec = {"test_spec_entry": "test_spec_value"}
        mock_request_return_val = Mock(text=new_id)
        mock_json.load.return_value = spec
        self.mock_client.request.return_value = mock_request_return_val

        with patch("builtins.open", mock_open()):
            res = self.dsort.start("spec_file")

        self.assertEqual(new_id, res)
        self.assertEqual(new_id, self.dsort.dsort_id)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST, path=URL_PATH_DSORT, json=spec
        )

    def test_abort(self):
        self.dsort.abort()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_DSORT}/{DSORT_ABORT}",
            params={DSORT_UUID: [self.dsort_id]},
        )

    def test_metrics(self):
        metrics = {"id_1": Mock(DsortMetrics)}
        self.mock_client.request_deserialize.return_value = metrics
        res = self.dsort.metrics()
        self.assertEqual(metrics, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_DSORT,
            res_model=Dict[str, DsortMetrics],
            params={DSORT_UUID: [self.dsort_id]},
        )

    @patch("aistore.sdk.dsort.time.sleep")
    @patch("aistore.sdk.dsort.Dsort.metrics")
    def test_wait_default_timeout(self, mock_metrics, mock_sleep):
        timeout = 300
        frequency = probing_frequency(timeout)
        expected_metrics_calls = [
            call(),
            call(),
            call(),
        ]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self._wait_test_helper(
            self.dsort,
            mock_metrics,
            mock_sleep,
            expected_metrics_calls,
            expected_sleep_calls,
        )

    @patch("aistore.sdk.dsort.time.sleep")
    @patch("aistore.sdk.dsort.Dsort.metrics")
    def test_wait(self, mock_status, mock_sleep):
        timeout = 20
        frequency = probing_frequency(timeout)
        expected_metrics_calls = [call(), call(), call()]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self._wait_test_helper(
            self.dsort,
            mock_status,
            mock_sleep,
            expected_metrics_calls,
            expected_sleep_calls,
            timeout=timeout,
        )

    @patch("aistore.sdk.dsort.time.sleep")
    @patch("aistore.sdk.dsort.Dsort.metrics")
    # pylint: disable=unused-argument
    def test_wait_timeout(self, mock_metrics, mock_sleep):
        mock_metric = Mock(DsortMetrics)
        mock_metric.aborted = False
        mock_metric.shard_creation = Mock(finished=False)
        mock_metrics.return_value = {"key": mock_metric}
        self.assertRaises(Timeout, self.dsort.wait)

    @patch("aistore.sdk.dsort.time.sleep")
    @patch("aistore.sdk.dsort.Dsort.metrics")
    def test_wait_aborted(self, mock_metrics, mock_sleep):
        timeout = 300
        frequency = probing_frequency(timeout)
        expected_metrics_calls = [
            call(),
            call(),
        ]
        expected_sleep_calls = [call(frequency)]
        unfinished_metric = Mock(DsortMetrics)
        unfinished_metric.aborted = False
        unfinished_metric.shard_creation = Mock(finished=False)
        aborted_metric = Mock(DsortMetrics)
        aborted_metric.aborted = True
        aborted_metric.shard_creation = Mock(finished=False)
        mock_metrics.side_effect = [
            {"key": unfinished_metric},
            {"key": aborted_metric},
            {"key": unfinished_metric},
        ]

        self._wait_exec_assert(
            self.dsort,
            mock_metrics,
            mock_sleep,
            expected_metrics_calls,
            expected_sleep_calls,
        )

    # pylint: disable=too-many-arguments
    def _wait_test_helper(
        self,
        dsort,
        mock_metrics,
        mock_sleep,
        expected_metrics_calls,
        expected_sleep_calls,
        **kwargs,
    ):
        unfinished_metric = Mock(DsortMetrics)
        unfinished_metric.aborted = False
        unfinished_metric.shard_creation = Mock(finished=False)
        finished_metric = Mock(DsortMetrics)
        finished_metric.aborted = False
        finished_metric.shard_creation = Mock(finished=True)
        mock_metrics.side_effect = [
            {"key": unfinished_metric},
            {"key": unfinished_metric},
            {"key": finished_metric},
        ]
        self._wait_exec_assert(
            dsort,
            mock_metrics,
            mock_sleep,
            expected_metrics_calls,
            expected_sleep_calls,
            **kwargs,
        )

    def _wait_exec_assert(
        self,
        dsort,
        mock_metrics,
        mock_sleep,
        expected_metrics_calls,
        expected_sleep_calls,
        **kwargs,
    ):
        dsort.wait(**kwargs)

        mock_metrics.assert_has_calls(expected_metrics_calls)
        mock_sleep.assert_has_calls(expected_sleep_calls)
        self.assertEqual(len(expected_metrics_calls), mock_metrics.call_count)
        self.assertEqual(len(expected_sleep_calls), mock_sleep.call_count)
