import unittest
from typing import List, Optional
from unittest.mock import Mock, create_autospec

from aistore.sdk.bucket import Bucket
from aistore.sdk.cluster import Cluster
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    QPARAM_WHAT,
    QPARAM_PROVIDER,
    ACT_LIST,
    PROVIDER_AIS,
    WHAT_SMAP,
    URL_PATH_DAEMON,
    URL_PATH_BUCKETS,
    URL_PATH_HEALTH,
    QPARAM_PRIMARY_READY_REB,
    URL_PATH_CLUSTER,
    WHAT_ALL_XACT_STATUS,
    WHAT_ALL_RUNNING_STATUS,
    URL_PATH_ETL,
)
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import Smap, ActionMsg, BucketModel, JobStatus, JobQuery, ETLInfo


class TestCluster(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock(RequestClient)
        self.cluster = Cluster(self.mock_client)

    def test_get_info(self):
        expected_result = create_autospec(Smap)
        self.mock_client.request_deserialize.return_value = expected_result
        result = self.cluster.get_info()
        self.assertEqual(result, expected_result)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_DAEMON,
            res_model=Smap,
            params={QPARAM_WHAT: WHAT_SMAP},
        )

    def test_list_buckets(self):
        provider = "any-provider"
        expected_params = {QPARAM_PROVIDER: provider}
        self.list_buckets_exec_assert(expected_params, provider=provider)

    def test_list_buckets_default_param(self):
        expected_params = {QPARAM_PROVIDER: PROVIDER_AIS}
        self.list_buckets_exec_assert(expected_params)

    def list_buckets_exec_assert(self, expected_params, **kwargs):
        expected_result = [Mock(Bucket)]
        self.mock_client.request_deserialize.return_value = expected_result

        res = self.cluster.list_buckets(**kwargs)

        self.assertEqual(expected_result, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_BUCKETS,
            res_model=List[BucketModel],
            json=ActionMsg(action=ACT_LIST).dict(),
            params=expected_params,
        )

    def test_is_aistore_running_exception(self):
        self.mock_client.request.side_effect = Exception
        self.assertFalse(self.cluster.is_aistore_running())

    def test_is_aistore_running(self):
        expected_params = {QPARAM_PRIMARY_READY_REB: "true"}
        response = Mock()
        response.ok = True
        self.mock_client.request.return_value = response
        self.assertTrue(self.cluster.is_aistore_running())
        response.ok = False
        self.mock_client.request.return_value = response
        self.assertFalse(self.cluster.is_aistore_running())
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_HEALTH,
            params=expected_params,
        )

    def test_list_jobs_status_default_params(self):
        expected_request_val = JobQuery().as_dict()
        self._list_jobs_status_exec_assert(expected_request_val)

    def test_list_jobs_status(self):
        job_kind = "kind"
        target_id = "specific_node"

        expected_request_val = JobQuery(kind=job_kind, target=target_id).as_dict()
        self._list_jobs_status_exec_assert(
            expected_request_val,
            job_kind=job_kind,
            target_id=target_id,
        )

    def test_list_jobs_status_no_result(self):
        self.mock_client.request_deserialize.return_value = None
        self.assertEqual([], self.cluster.list_jobs_status())

    def _list_jobs_status_exec_assert(self, expected_request_val, **kwargs):
        returned_status = JobStatus()
        self.mock_client.request_deserialize.return_value = returned_status

        res = self.cluster.list_jobs_status(**kwargs)

        self.assertEqual(returned_status, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=Optional[List[JobStatus]],
            json=expected_request_val,
            params={QPARAM_WHAT: WHAT_ALL_XACT_STATUS},
        )

    def test_list_running_jobs_default_params(self):
        expected_request_val = JobQuery(active=True).as_dict()
        self._list_running_jobs_exec_assert(expected_request_val)

    def test_list_running_jobs(self):
        job_kind = "job-kind"
        target_id = "my-target"
        expected_request_val = JobQuery(
            active=True, kind=job_kind, target=target_id
        ).as_dict()
        self._list_running_jobs_exec_assert(
            expected_request_val, job_kind=job_kind, target_id=target_id
        )

    def _list_running_jobs_exec_assert(self, expected_request_val, **kwargs):
        mock_response = ["job_1_kind[job_1_id]", "job_2_kind[job_2_id]"]
        self.mock_client.request_deserialize.return_value = mock_response

        res = self.cluster.list_running_jobs(**kwargs)

        self.assertEqual(mock_response, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_CLUSTER,
            res_model=List[str],
            json=expected_request_val,
            params={QPARAM_WHAT: WHAT_ALL_RUNNING_STATUS},
        )

    def test_list_running_etls(self):
        mock_response = Mock()
        self.mock_client.request_deserialize.return_value = mock_response
        response = self.cluster.list_running_etls()
        self.assertEqual(mock_response, response)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET, path=URL_PATH_ETL, res_model=List[ETLInfo]
        )
