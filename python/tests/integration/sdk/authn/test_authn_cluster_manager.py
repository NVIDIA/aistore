#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import unittest

import pytest

from aistore.sdk.authn.authn_client import AuthNClient
from aistore.sdk.client import Client
from aistore.sdk.authn.types import ClusterInfo, ClusterList
from aistore.sdk.authn.errors import ErrClusterAlreadyRegistered
from tests.integration import (
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
    AUTHN_ENDPOINT,
    CLUSTER_ENDPOINT,
)


class TestAuthNClusterManager(
    unittest.TestCase
):  # pylint: disable=too-many-instance-attributes
    def setUp(self) -> None:
        # AuthN Client
        self.authn_client = AuthNClient(AUTHN_ENDPOINT)
        self.cluster_manager = self.authn_client.cluster_manager()
        self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self.cluster_alias = "test-cluster"
        self.urls = [CLUSTER_ENDPOINT]

        # AIS Client
        self.ais_client = Client(CLUSTER_ENDPOINT, token=self.authn_client.client.token)
        self.uuid = self.ais_client.cluster().get_uuid()

        # Register the AIS Cluster
        self.cluster_info = self.cluster_manager.register(self.cluster_alias, self.urls)

    def tearDown(self) -> None:
        self.cluster_manager.delete(cluster_id=self.uuid)

    def _assert_cluster_info(self, fetched_cluster):
        self.assertEqual(self.urls, fetched_cluster.urls)
        self.assertEqual(self.cluster_alias, fetched_cluster.alias)
        self.assertEqual(self.uuid, fetched_cluster.id)

    @pytest.mark.authn
    def test_list_clusters(self):
        cluster_list = self.cluster_manager.list()
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
            self.cluster_manager.register(cluster_alias="test-cluster", urls=[])

    @pytest.mark.authn
    def test_register_cluster_duplicate(self):
        with self.assertRaises(ErrClusterAlreadyRegistered):
            self.cluster_manager.register(self.cluster_alias, self.urls)

    @pytest.mark.authn
    def test_get_cluster_by_id(self):
        fetched_cluster = self.cluster_manager.get(cluster_id=self.uuid)
        self._assert_cluster_info(fetched_cluster)

    @pytest.mark.authn
    def test_get_cluster_by_alias(self):
        fetched_cluster = self.cluster_manager.get(cluster_alias=self.cluster_alias)
        self._assert_cluster_info(fetched_cluster)

    @pytest.mark.authn
    def test_update_cluster(self):
        new_cluster_alias = "new-test-alias"
        updated_cluster = self.cluster_manager.update(
            self.uuid, cluster_alias=new_cluster_alias
        )
        self.cluster_alias = new_cluster_alias
        self._assert_cluster_info(updated_cluster)
