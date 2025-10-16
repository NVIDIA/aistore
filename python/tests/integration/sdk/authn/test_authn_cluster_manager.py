#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

import pytest

from aistore.sdk.authn.types import ClusterInfo, ClusterList
from aistore.sdk.authn.errors import ErrClusterAlreadyRegistered
from tests.integration import CLUSTER_ENDPOINT
from tests.integration.sdk.authn.authn_test_base import AuthNTestBase


class TestAuthNClusterManager(AuthNTestBase):
    def _assert_cluster_info(self, fetched_cluster):
        self.assertEqual([CLUSTER_ENDPOINT], fetched_cluster.urls)
        self.assertEqual(self.cluster_alias, fetched_cluster.alias)
        self.assertEqual(self.uuid, fetched_cluster.id)

    @pytest.mark.authn
    def test_list_clusters(self):
        cluster_list = self.authn_client.cluster_manager().list()
        self.assertIsInstance(cluster_list, ClusterList)
        self.assertEqual(
            cluster_list, ClusterList(clusters={self.uuid: self.cluster_info})
        )

    @pytest.mark.authn
    def test_register_cluster(self):
        self.assertIsInstance(self.cluster_info, ClusterInfo)
        self._assert_cluster_info(self.cluster_info)

    @pytest.mark.authn
    def test_register_cluster_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.authn_client.cluster_manager().register(
                cluster_alias="test-cluster", urls=[]
            )

    @pytest.mark.authn
    def test_register_cluster_duplicate(self):
        with self.assertRaises(ErrClusterAlreadyRegistered):
            self.authn_client.cluster_manager().register(
                self.cluster_alias, [CLUSTER_ENDPOINT]
            )

    @pytest.mark.authn
    def test_get_cluster_by_id(self):
        fetched_cluster = self.authn_client.cluster_manager().get(cluster_id=self.uuid)
        self._assert_cluster_info(fetched_cluster)

    @pytest.mark.authn
    def test_get_cluster_by_alias(self):
        fetched_cluster = self.authn_client.cluster_manager().get(
            cluster_alias=self.cluster_alias
        )
        self._assert_cluster_info(fetched_cluster)

    @pytest.mark.authn
    def test_update_cluster(self):
        new_cluster_alias = "new-test-alias"
        updated_cluster = self.authn_client.cluster_manager().update(
            self.uuid, cluster_alias=new_cluster_alias
        )
        self.cluster_alias = new_cluster_alias
        self._assert_cluster_info(updated_cluster)
