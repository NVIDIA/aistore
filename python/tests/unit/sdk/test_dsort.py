import unittest
from typing import Dict
from unittest.mock import Mock, patch, mock_open, call

import json
import yaml

from aistore.sdk.const import (
    URL_PATH_DSORT,
    HTTP_METHOD_POST,
    DSORT_ABORT,
    HTTP_METHOD_DELETE,
    DSORT_UUID,
    HTTP_METHOD_GET,
)
from aistore.sdk.dsort import Dsort, DsortFramework, DsortShardsGroup
from aistore.sdk.dsort.types import DsortMetrics, JobInfo

from aistore.sdk.types import BucketModel
from aistore.sdk.errors import Timeout
from aistore.sdk.utils import probing_frequency

VALID_JSON_SPEC = """
{
    "input_bck": {"name": "input_bucket", "provider": "aws"},
    "output_bck": {"name": "output_bucket", "provider": "aws"},
    "input_format": {"template": "input_template"},
    "output_format": "output_template",
    "input_extension": ".txt",
    "output_extension": ".txt",
    "output_shard_size": "10MB",
    "description": ""
}
"""

VALID_YAML_SPEC = """
input_bck:
    name: input_bucket
    provider: aws
output_bck:
    name: output_bucket
    provider: aws
input_format:
    template: input_template
output_format:
    template: output_template
input_extension: .txt
output_extension: .txt
output_shard_size: 10MB
"""


class TestDsort(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_client = Mock()
        self.dsort_id = "123"
        self.dsort = Dsort(client=self.mock_client, dsort_id=self.dsort_id)

    @staticmethod
    def _get_mock_job_info(finished, aborted=False):
        mock_metrics = Mock(DsortMetrics)
        mock_metrics.aborted = aborted
        mock_metrics.shard_creation = Mock(finished=finished)
        mock_job_info = Mock(JobInfo)
        mock_job_info.metrics = mock_metrics
        return mock_job_info

    def test_properties(self):
        self.assertEqual(self.dsort_id, self.dsort.dsort_id)

    @patch("aistore.sdk.dsort.core.validate_file")
    def test_start_from_filepath(self, mock_validate_file):
        new_id = "456"
        mock_request_return_val = Mock(text=new_id)
        mock_validate_file.return_value = None
        self.mock_client.request.return_value = mock_request_return_val

        with patch("builtins.open", mock_open(read_data=VALID_JSON_SPEC)):
            res = self.dsort.start("spec_file.json")

        self.assertEqual(new_id, res)
        self.assertEqual(new_id, self.dsort.dsort_id)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST, path=URL_PATH_DSORT, json=json.loads(VALID_JSON_SPEC)
        )

    def test_abort(self):
        self.dsort.abort()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_DSORT}/{DSORT_ABORT}",
            params={DSORT_UUID: [self.dsort_id]},
        )

    def test_get_job_info(self):
        mock_job_info = {"id_1": Mock(JobInfo)}
        self.mock_client.request_deserialize.return_value = mock_job_info
        res = self.dsort.get_job_info()
        self.assertEqual(mock_job_info, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_DSORT,
            res_model=Dict[str, JobInfo],
            params={DSORT_UUID: [self.dsort_id]},
        )

    @patch("aistore.sdk.dsort.core.time.sleep")
    @patch("aistore.sdk.dsort.core.Dsort.get_job_info")
    def test_wait_default_timeout(self, mock_get_job_info, mock_sleep):
        timeout = 300
        frequency = probing_frequency(timeout)
        expected_job_info_calls = [
            call(),
            call(),
            call(),
        ]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self._wait_test_helper(
            self.dsort,
            mock_get_job_info,
            mock_sleep,
            expected_job_info_calls,
            expected_sleep_calls,
        )

    @patch("aistore.sdk.dsort.core.time.sleep")
    @patch("aistore.sdk.dsort.core.Dsort.get_job_info")
    def test_wait(self, mock_get_job_info, mock_sleep):
        timeout = 20
        frequency = probing_frequency(timeout)
        expected_job_info_calls = [call(), call(), call()]
        expected_sleep_calls = [call(frequency), call(frequency)]
        self._wait_test_helper(
            self.dsort,
            mock_get_job_info,
            mock_sleep,
            expected_job_info_calls,
            expected_sleep_calls,
            timeout=timeout,
        )

    @patch("aistore.sdk.dsort.core.time.sleep")
    @patch("aistore.sdk.dsort.core.Dsort.get_job_info")
    # pylint: disable=unused-argument
    def test_wait_timeout(self, mock_get_job_info, mock_sleep):
        mock_get_job_info.return_value = {
            "key": self._get_mock_job_info(finished=False, aborted=False)
        }
        self.assertRaises(Timeout, self.dsort.wait)

    @patch("aistore.sdk.dsort.core.time.sleep")
    @patch("aistore.sdk.dsort.core.Dsort.get_job_info")
    def test_wait_aborted(self, mock_get_job_info, mock_sleep):
        timeout = 300
        frequency = probing_frequency(timeout)
        expected_metrics_calls = [
            call(),
            call(),
        ]
        expected_sleep_calls = [call(frequency)]
        mock_get_job_info.side_effect = [
            {"key": self._get_mock_job_info(finished=False)},
            {"key": self._get_mock_job_info(finished=False, aborted=True)},
            {"key": self._get_mock_job_info(finished=False)},
        ]

        self._wait_exec_assert(
            self.dsort,
            mock_get_job_info,
            mock_sleep,
            expected_metrics_calls,
            expected_sleep_calls,
        )

    # pylint: disable=too-many-arguments
    def _wait_test_helper(
        self,
        dsort,
        mock_get_job_info,
        mock_sleep,
        expected_job_info_calls,
        expected_sleep_calls,
        **kwargs,
    ):
        mock_get_job_info.side_effect = [
            {"job_id": self._get_mock_job_info(finished=False)},
            {"job_id": self._get_mock_job_info(finished=False)},
            {"job_id": self._get_mock_job_info(finished=True)},
        ]
        self._wait_exec_assert(
            dsort,
            mock_get_job_info,
            mock_sleep,
            expected_job_info_calls,
            expected_sleep_calls,
            **kwargs,
        )

    def _wait_exec_assert(
        self,
        dsort,
        mock_get_job_info,
        mock_sleep,
        expected_job_info_calls,
        expected_sleep_calls,
        **kwargs,
    ):
        dsort.wait(**kwargs)

        mock_get_job_info.assert_has_calls(expected_job_info_calls)
        mock_sleep.assert_has_calls(expected_sleep_calls)
        self.assertEqual(len(expected_job_info_calls), mock_get_job_info.call_count)
        self.assertEqual(len(expected_sleep_calls), mock_sleep.call_count)

    # pylint: disable=unused-argument
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_from_file_invalid_extension(self, mock_file):
        with self.assertRaises(ValueError):
            DsortFramework.from_file("invalid.txt")

    @patch("aistore.sdk.dsort.framework.json")
    def test_from_file_json(self, mock_json):
        mock_json.load.return_value = json.loads(VALID_JSON_SPEC)
        with patch("builtins.open", mock_open(read_data=VALID_JSON_SPEC)):
            spec = DsortFramework.from_file("valid.json")
        mock_json.load.assert_called_once()
        self.assertEqual(spec.input_shards.bck.name, "input_bucket")
        self.assertEqual(spec.input_shards.extension, ".txt")
        self.assertEqual(spec.output_shards.bck.name, "output_bucket")
        self.assertEqual(spec.output_shards.extension, ".txt")
        self.assertEqual(spec.output_shard_size, "10MB")
        self.assertIsNone(spec.description)

    @patch("aistore.sdk.dsort.framework.yaml")
    def test_from_file_yaml(self, mock_yaml):
        mock_yaml.safe_load.return_value = yaml.safe_load(VALID_YAML_SPEC)
        with patch("builtins.open", mock_open(read_data=VALID_YAML_SPEC)):
            spec = DsortFramework.from_file("valid.yaml")
        mock_yaml.safe_load.assert_called_once()
        self.assertEqual(spec.input_shards.bck.name, "input_bucket")
        self.assertEqual(spec.input_shards.extension, ".txt")
        self.assertEqual(spec.output_shards.bck.name, "output_bucket")
        self.assertEqual(spec.output_shards.extension, ".txt")
        self.assertEqual(spec.output_shard_size, "10MB")
        self.assertIsNone(spec.description)

    def test_to_spec(self):
        input_shards = DsortShardsGroup(
            bck=BucketModel(name="input_bucket"),
            role="input",
            format={"template": "template_input"},
            extension="txt",
        )
        output_shards = DsortShardsGroup(
            bck=BucketModel(name="output_bucket"),
            role="output",
            format={"template": "template_output"},
            extension="txt",
        )
        dsort_framework = DsortFramework(
            input_shards=input_shards,
            output_shards=output_shards,
            output_shard_size="10GB",
            description="Test description",
        )

        spec = dsort_framework.to_spec()
        self.assertEqual(spec["input_bck"], {"name": "input_bucket", "provider": "ais"})
        self.assertEqual(spec["input_extension"], "txt")
        self.assertEqual(
            spec["output_bck"], {"name": "output_bucket", "provider": "ais"}
        )
        self.assertEqual(spec["output_extension"], "txt")
        self.assertEqual(spec["output_shard_size"], "10GB")
        self.assertEqual(spec["description"], "Test description")
