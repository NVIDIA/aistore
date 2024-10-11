import unittest
from typing import Dict
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, call

import json
import yaml

from aistore.sdk.const import (
    URL_PATH_DSORT,
    URL_PATH_OBJECTS,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    DSORT_ABORT,
    HTTP_METHOD_DELETE,
    DSORT_UUID,
    HTTP_METHOD_GET,
)
from aistore.sdk.dsort import Dsort, DsortFramework, DsortShardsGroup, DsortAlgorithm
from aistore.sdk.dsort.types import DsortMetrics, JobInfo
from aistore.sdk.dsort.ekm import ExternalKeyMap, EKM_FILE_NAME
from aistore.sdk.provider import Provider
from aistore.sdk.multiobj import ObjectNames, ObjectRange

from aistore.sdk.types import BucketModel
from aistore.sdk.errors import Timeout
from aistore.sdk.utils import probing_frequency

VALID_JSON_SPEC = """
{
    "input_bck": {"name": "input_bucket", "provider": "aws"},
    "output_bck": {"name": "output_bucket", "provider": "aws"},
    "input_format": {"template": "input-shard-{00..99..1}"},
    "output_format": "output-shard-{000..999..1}",
    "input_extension": ".txt",
    "output_extension": ".txt",
    "algorithm": {"kind": "alphanumeric", "decreasing": false, "seed": ""},
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
    template: input-shard-{00..99..1}
output_format: output-shard-{000..999..1}
input_extension: .txt
algorithm:
    kind: alphanumeric
output_extension: .txt
output_shard_size: 10MB
"""


class TestDsort(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_client = Mock()
        self.dsort_id = "123"
        self.dsort = Dsort(client=self.mock_client, dsort_id=self.dsort_id)

        # For testing ExternalKeyMap
        self.valid_key = "valid_key"
        self.valid_value = Mock(spec=ObjectNames)  # Mocking ObjectNames for the tests
        self.invalid_key = 123
        self.invalid_value = "invalid_value"

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

        # Test with regular file path
        with patch("builtins.open", mock_open(read_data=VALID_JSON_SPEC)):
            res = self.dsort.start("spec_file.json")

        self.assertEqual(new_id, res)
        self.assertEqual(new_id, self.dsort.dsort_id)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST, path=URL_PATH_DSORT, json=json.loads(VALID_JSON_SPEC)
        )

        # Test with Path from pathlib
        with patch("builtins.open", mock_open(read_data=VALID_JSON_SPEC)):
            spec_path = Path("spec_file.json")
            res = self.dsort.start(spec_path)

        self.assertEqual(new_id, res)
        self.assertEqual(new_id, self.dsort.dsort_id)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST, path=URL_PATH_DSORT, json=json.loads(VALID_JSON_SPEC)
        )

    def test_start_from_framework(self):
        new_id = "789"
        mock_request_return_val = Mock(text=new_id)
        self.mock_client.request.return_value = mock_request_return_val

        input_shards = DsortShardsGroup(
            bck=BucketModel(name="input_bucket", provider=Provider.AIS.value),
            role="input",
            format=ObjectRange("input-", 0, 99),
            extension="txt",
        )
        output_shards = DsortShardsGroup(
            bck=BucketModel(name="output_bucket", provider=Provider.AIS.value),
            role="output",
            format=ObjectRange("output-", 0, 99),
            extension="txt",
        )
        dsort_framework = DsortFramework(
            input_shards=input_shards,
            output_shards=output_shards,
            output_shard_size="10GB",
            algorithm=DsortAlgorithm(),
            description="Test description",
        )
        res = self.dsort.start(dsort_framework)

        self.assertEqual(new_id, res)
        self.assertEqual(new_id, self.dsort.dsort_id)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST, path=URL_PATH_DSORT, json=dsort_framework.to_spec()
        )

    @patch("aistore.sdk.obj.object.Object.get_url")
    def test_start_from_framework_with_ekm(self, mock_get_url):
        new_id = "789"
        mock_request_return_val = Mock(text=new_id)
        self.mock_client.request.return_value = mock_request_return_val

        input_shards = DsortShardsGroup(
            bck=BucketModel(name="input_bucket", provider=Provider.AIS.value),
            role="input",
            format=ObjectRange("input-", 0, 99),
            extension="txt",
        )
        ekm = ExternalKeyMap()
        ekm["shard-%d-suf"] = ObjectNames(
            [
                "2854426993101776575.txt",
                "2618707812048272555.txt",
                "1741850460848810679.txt",
                "5802693705337902329.txt",
            ]
        )
        ekm["input-%d-pref"] = ObjectNames(
            [
                "5572318253765218801.txt",
                "2616544611865131484.txt",
                "1066141796602636610.txt",
                "4083990031653643606.txt",
            ]
        )
        ekm["smth-%d"] = ObjectNames(
            [
                "4540068038224404716.txt",
                "4657933324577966298.txt",
                "2972823983340349521.txt",
                "7151198881742927530.txt",
            ]
        )

        output_shards = DsortShardsGroup(
            bck=BucketModel(name="output_bucket", provider=Provider.AIS.value),
            role="output",
            format=ekm,
            extension="txt",
        )
        dsort_framework = DsortFramework(
            input_shards=input_shards,
            output_shards=output_shards,
            output_shard_size="10GB",
            algorithm=DsortAlgorithm(),
            description="Test description",
        )
        ekm_url = f"{URL_PATH_OBJECTS}/input_bucket/{ EKM_FILE_NAME }"
        mock_get_url.return_value = ekm_url

        res = self.dsort.start(dsort_framework)
        self.assertEqual(new_id, res)
        self.assertEqual(new_id, self.dsort.dsort_id)
        spec = dsort_framework.to_spec()
        spec["ekm_file"] = ekm_url
        spec["ekm_file_sep"] = ""

        # Ensure object.put_content and dsort.start are called
        self.mock_client.request.assert_has_calls(
            [
                call(
                    HTTP_METHOD_PUT,
                    path=ekm_url,
                    params={"provider": Provider.AIS.value},
                    data=json.dumps(ekm.as_dict()).encode("utf-8"),
                ),
                call(HTTP_METHOD_POST, path=URL_PATH_DSORT, json=spec),
            ]
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
        self.assertEqual(spec.algorithm.kind, "alphanumeric")
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
        self.assertEqual(spec.algorithm.kind, "alphanumeric")
        self.assertIsNone(spec.description)

    def test_to_spec(self):
        input_shards = DsortShardsGroup(
            bck=BucketModel(name="input_bucket", provider=Provider.AIS.value),
            role="input",
            format=ObjectRange("input-", 0, 99),
            extension="txt",
        )
        output_shards = DsortShardsGroup(
            bck=BucketModel(name="output_bucket", provider=Provider.AIS.value),
            role="output",
            format=ObjectRange("output-", 0, 99),
            extension="txt",
        )
        dsort_framework = DsortFramework(
            input_shards=input_shards,
            output_shards=output_shards,
            output_shard_size="10GB",
            algorithm=DsortAlgorithm(),
            description="Test description",
        )

        spec = dsort_framework.to_spec()
        self.assertEqual(spec["input_bck"], {"name": "input_bucket", "provider": "ais"})
        self.assertEqual(spec["input_extension"], "txt")
        self.assertDictEqual(spec["input_format"], {"template": "input-{0..99..1}"})
        self.assertEqual(
            spec["output_bck"], {"name": "output_bucket", "provider": "ais"}
        )
        self.assertEqual(spec["output_extension"], "txt")
        self.assertEqual(spec["output_format"], "output-{0..99..1}")
        self.assertEqual(spec["output_shard_size"], "10GB")
        self.assertEqual(spec["description"], "Test description")

    def test_invalid_dsort_shards_group(self):
        invalid_input = {
            "bck": BucketModel(name="test_bucket", provider="aws"),
            "role": "input",
            "format": "palne_string",
            "extension": ".txt",
        }
        with self.assertRaises(ValueError):
            DsortShardsGroup(**invalid_input)

        invalid_output = {
            "bck": BucketModel(name="test_bucket", provider="aws"),
            "role": "output",
            "format": "palne_string",
            "extension": ".txt",
        }
        with self.assertRaises(ValueError):
            DsortShardsGroup(**invalid_output)

        invalid_role = {
            "bck": BucketModel(name="test_bucket", provider="aws"),
            "role": "invalid",
            "format": ObjectRange("output-", 0, 99),
            "extension": ".txt",
        }
        with self.assertRaises(ValueError):
            DsortShardsGroup(**invalid_role)

    def test_invalid_dsort_algorithm(self):
        invalid_algo_content_missing_ext = {
            "kind": "content",
            "decreasing": True,
            "content_key_type": "int",
        }
        with self.assertRaises(ValueError):
            DsortAlgorithm(**invalid_algo_content_missing_ext)

        invalid_algo_content_missing_key_type = {
            "kind": "content",
            "decreasing": True,
            "extension": ".txt",
        }
        with self.assertRaises(ValueError):
            DsortAlgorithm(**invalid_algo_content_missing_key_type)

        invalid_algo_content_extra_fields = {
            "kind": "alphanumeric",
            "decreasing": False,
            "extension": ".txt",
            "content_key_type": "int",
        }
        with self.assertRaises(ValueError):
            DsortAlgorithm(**invalid_algo_content_extra_fields)

    def test_ekm_setitem_valid(self):
        ekm = ExternalKeyMap()
        ekm[self.valid_key] = self.valid_value
        self.assertEqual(ekm[self.valid_key], self.valid_value)

    def test_ekm_setitem_invalid_key(self):
        ekm = ExternalKeyMap()
        with self.assertRaises(TypeError) as context:
            ekm[self.invalid_key] = self.valid_value
        self.assertEqual(str(context.exception), "Key must be a string, got int")

    def test_ekm_setitem_invalid_value(self):
        ekm = ExternalKeyMap()
        with self.assertRaises(TypeError) as context:
            ekm[self.valid_key] = self.invalid_value
        self.assertEqual(
            str(context.exception), "Value must be an instance of ObjectNames, got str"
        )
