#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import patch, Mock

from aistore.sdk.request_client import RequestClient
from aistore.sdk.authn.types import ClusterInfo, ClusterList
from aistore.sdk.authn.cluster_manager import ClusterManager
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    URL_PATH_AUTHN_CLUSTERS,
)


class TestAuthNClusterManager(unittest.TestCase):
    def setUp(self) -> None:
        self.endpoint = "http://authn-endpoint"
        self.mock_client = Mock(RequestClient)
        self.cluster_manager = ClusterManager(self.mock_client)

    def test_list_clusters(self):
        mock_cluster_list = Mock(spec=ClusterList)
        self.mock_client.request_deserialize.return_value = mock_cluster_list
        clusters = self.cluster_manager.list()

        self.assertEqual(clusters, mock_cluster_list)
        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=URL_PATH_AUTHN_CLUSTERS,
            res_model=ClusterList,
        )

    def test_get_cluster_by_id(self):
        cluster_id = "1"
        mock_cluster_info = Mock(spec=ClusterInfo)
        mock_cluster_list = ClusterList(clusters={cluster_id: mock_cluster_info})
        self.mock_client.request_deserialize.return_value = mock_cluster_list
        cluster = self.cluster_manager.get(cluster_id=cluster_id)

        self.assertEqual(cluster, mock_cluster_list.clusters[cluster_id])
        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{cluster_id}",
            res_model=ClusterList,
        )

    def test_get_cluster_by_alias(self):
        cluster_alias = "test-cluster"
        cluster_id = "1"
        mock_cluster_info = ClusterInfo(id=cluster_id, alias=cluster_alias)
        mock_cluster_list = ClusterList(clusters={cluster_id: mock_cluster_info})
        self.mock_client.request_deserialize.return_value = mock_cluster_list

        cluster = self.cluster_manager.get(cluster_alias=cluster_alias)

        self.assertEqual(cluster, mock_cluster_list.clusters[cluster_id])
        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{cluster_alias}",
            res_model=ClusterList,
        )

    @patch("aistore.sdk.authn.cluster_manager.AISClient")
    def test_register_cluster(self, mock_ais_client):
        cluster_alias = "test-cluster"
        urls = ["http://ais-cluster-url"]
        uuid = "test-uuid"

        mock_cluster_info = ClusterInfo(id=uuid, alias=cluster_alias, urls=urls)
        mock_cluster_list = ClusterList(clusters={uuid: mock_cluster_info})
        self.mock_client.request_deserialize.return_value = mock_cluster_list

        mock_ais_cluster = Mock()
        mock_ais_cluster.get_uuid.return_value = uuid
        # The client created by AISClient() calls cluster() which finally returns a mock cluster instance
        mock_ais_client.return_value.cluster.return_value = mock_ais_cluster

        cluster = self.cluster_manager.register(cluster_alias=cluster_alias, urls=urls)

        self.assertEqual(cluster, mock_cluster_info)
        mock_ais_cluster.get_uuid.assert_called_once()
        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_POST,
            path=URL_PATH_AUTHN_CLUSTERS,
            json={"id": uuid, "alias": cluster_alias, "urls": urls},
        )
        self.mock_client.request_deserialize.assert_any_call(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{uuid}",
            res_model=ClusterList,
        )

    def test_update_cluster(self):
        cluster_id = "1"
        cluster_alias = "updated-cluster"
        urls = ["http://new-cluster-url"]
        mock_cluster_info = ClusterInfo(id=cluster_id, alias=cluster_alias, urls=urls)
        mock_cluster_list = ClusterList(clusters={cluster_id: mock_cluster_info})
        self.mock_client.request_deserialize.return_value = mock_cluster_list

        cluster = self.cluster_manager.update(
            cluster_id=cluster_id, cluster_alias=cluster_alias, urls=urls
        )

        self.assertEqual(cluster, mock_cluster_info)
        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_PUT,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{cluster_id}",
            json=mock_cluster_info.model_dump(),
        )

    def test_delete_cluster(self):
        cluster_id = "1"

        self.cluster_manager.delete(cluster_id=cluster_id)

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{cluster_id}",
        )

    def test_get_cluster_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.cluster_manager.get()

    def test_register_cluster_raises_value_error_no_urls(self):
        with self.assertRaises(ValueError):
            self.cluster_manager.register(cluster_alias="test-cluster", urls=[])

    def test_update_cluster_raises_value_error_no_alias_or_urls(self):
        with self.assertRaises(ValueError):
            self.cluster_manager.update(cluster_id="1")

    def test_delete_cluster_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.cluster_manager.delete()
