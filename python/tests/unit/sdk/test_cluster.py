import unittest
from typing import List
from unittest.mock import Mock, create_autospec

from aistore.sdk import Client
from aistore.sdk.cluster import Cluster
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    QParamWhat,
    QParamProvider,
    ACT_LIST,
    ProviderAIS,
)
from aistore.sdk.types import Smap, ActionMsg, Bck


class TestCluster(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock(Client)
        self.cluster = Cluster(self.mock_client)

    def test_get_info(self):
        expected_result = create_autospec(Smap)
        self.mock_client.request_deserialize.return_value = expected_result
        result = self.cluster.get_info()
        self.assertEqual(result, expected_result)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET, path="daemon", res_model=Smap, params={QParamWhat: "smap"}
        )

    def test_list_buckets(self):
        provider = "any-provider"
        expected_params = {QParamProvider: provider}
        self.list_buckets_exec_assert(expected_params, provider=provider)

    def test_list_buckets_default_param(self):
        expected_params = {QParamProvider: ProviderAIS}
        self.list_buckets_exec_assert(expected_params)

    def list_buckets_exec_assert(self, expected_params, **kwargs):
        expected_result = [Bck(name="bucket")]
        self.mock_client.request_deserialize.return_value = expected_result

        res = self.cluster.list_buckets(**kwargs)

        self.assertEqual(expected_result, res)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path="buckets",
            res_model=List[Bck],
            json=ActionMsg(action=ACT_LIST).dict(),
            params=expected_params,
        )

    def test_is_aistore_running_exception(self):
        self.mock_client.request.side_effect = Exception
        self.assertFalse(self.cluster.is_aistore_running())

    def test_is_aistore_running(self):
        response = Mock()
        response.ok = True
        self.mock_client.request.return_value = response
        self.assertTrue(self.cluster.is_aistore_running())
        response.ok = False
        self.mock_client.request.return_value = response
        self.assertFalse(self.cluster.is_aistore_running())
