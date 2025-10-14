import unittest
from typing import List, Optional
from unittest.mock import Mock, patch

from aistore.sdk.bucket import Bucket
from aistore.sdk.provider import Provider
from aistore.sdk.cluster import Cluster
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    QPARAM_WHAT,
    QPARAM_PROVIDER,
    ACT_LIST,
    WHAT_SMAP,
    URL_PATH_DAEMON,
    URL_PATH_BUCKETS,
    URL_PATH_HEALTH,
    QPARAM_PRIMARY_READY_REB,
    URL_PATH_CLUSTER,
    WHAT_ALL_XACT_STATUS,
    WHAT_ALL_RUNNING_STATUS,
    URL_PATH_ETL,
    URL_PATH_REVERSE,
    WHAT_NODE_STATS_AND_STATUS,
)
from aistore.sdk.etl.etl_const import ETL_STAGE_RUNNING, ETL_STAGE_ABORTED
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import (
    Smap,
    ActionMsg,
    BucketModel,
    JobStatus,
    JobQuery,
    ETLInfo,
    Snode,
    NetInfo,
)

from tests.utils import cases


class TestCluster(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock(RequestClient)
        self.cluster = Cluster(self.mock_client)

    def test_get_info(self):
        expected_result = Mock()
        self.mock_client.request_deserialize.return_value = expected_result
        result = self.cluster.get_info()
        self.assertEqual(result, expected_result)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_DAEMON,
            res_model=Smap,
            params={QPARAM_WHAT: WHAT_SMAP},
        )

    @cases(*Provider)
    def test_list_buckets(self, provider):
        expected_params = {QPARAM_PROVIDER: provider.value}
        self.list_buckets_exec_assert(expected_params, provider=provider)

    def test_list_buckets_default_param(self):
        expected_params = {QPARAM_PROVIDER: Provider.AIS.value}
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
            json=ActionMsg(action=ACT_LIST).model_dump(),
            params=expected_params,
        )

    def test_is_ready_exception(self):
        self.mock_client.request.side_effect = Exception
        self.assertFalse(self.cluster.is_ready())

    @cases(True, False)
    def test_is_ready(self, test_case):
        expected_params = {QPARAM_PRIMARY_READY_REB: "true"}
        primary_proxy_endpoint = "primary_proxy_url"

        mock_response = Mock()
        mock_response.ok = test_case
        self.mock_client.request.return_value = mock_response
        mock_smap = Mock(spec=Smap)
        mock_snode = Mock(spec=Snode)
        mock_netinfo = Mock(spec=NetInfo)
        mock_netinfo.direct_url = primary_proxy_endpoint
        mock_snode.public_net = mock_netinfo
        mock_smap.proxy_si = mock_snode
        self.mock_client.request_deserialize.return_value = mock_smap

        self.assertEqual(test_case, self.cluster.is_ready())
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_GET,
            path=URL_PATH_HEALTH,
            endpoint=primary_proxy_endpoint,
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
        mock_running_etl = Mock(stage=ETL_STAGE_RUNNING)
        mock_aborted_etl = Mock(stage=ETL_STAGE_ABORTED)
        mock_response = [mock_running_etl, mock_aborted_etl]
        self.mock_client.request_deserialize.return_value = mock_response
        response = self.cluster.list_etls()
        self.assertEqual([mock_running_etl], response)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET, path=URL_PATH_ETL, res_model=List[ETLInfo]
        )

    @patch("aistore.sdk.cluster.Cluster._get_smap")
    def test_get_performance(self, mock_get_smap):
        mock_smap = Smap(
            tmap={"target1": Mock(spec=Snode), "target2": Mock(spec=Snode)},
            pmap={"proxy1": Mock(spec=Snode)},
            proxy_si=Mock(spec=Snode),
        )
        mock_get_smap.return_value = mock_smap

        mock_response_1 = Mock()
        mock_response_1.json.return_value = {
            "tracker": {"get.n": 100, "put.n": 50},
            "capacity": {"pct_used": 47.5},
        }

        mock_response_2 = Mock()
        mock_response_2.json.return_value = {
            "tracker": {"get.n": 200, "put.n": 80},
            "capacity": {"pct_used": 55.0},
        }

        self.mock_client.request.side_effect = [mock_response_1, mock_response_2]

        performance = self.cluster.get_performance()

        expected = {
            "target1": {
                "tracker": {"get.n": 100, "put.n": 50},
                "capacity": {"pct_used": 47.5},
            },
            "target2": {
                "tracker": {"get.n": 200, "put.n": 80},
                "capacity": {"pct_used": 55.0},
            },
        }
        self.assertEqual(expected, performance)
        self.assertEqual(2, self.mock_client.request.call_count)

        self.mock_client.request.assert_any_call(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_REVERSE}/{URL_PATH_DAEMON}",
            params={QPARAM_WHAT: WHAT_NODE_STATS_AND_STATUS},
            headers={"ais-node-id": "target1"},
        )
        self.mock_client.request.assert_any_call(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_REVERSE}/{URL_PATH_DAEMON}",
            params={QPARAM_WHAT: WHAT_NODE_STATS_AND_STATUS},
            headers={"ais-node-id": "target2"},
        )
