#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import unittest
from unittest.mock import patch, Mock

from aistore.sdk.request_client import RequestClient
from aistore.sdk.authn.types import (
    BucketModel,
    BucketPermission,
    ClusterInfo,
    ClusterPermission,
    ClusterList,
    RoleInfo,
    RolesList,
)
from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.authn.role_manager import RoleManager
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_PUT,
    HTTP_METHOD_POST,
    URL_PATH_AUTHN_ROLES,
)


class TestAuthNRoleManager(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock(RequestClient)
        self.role_manager = RoleManager(self.mock_client)

    def test_role_delete(self):
        self.role_manager.delete(name="test-role")
        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_AUTHN_ROLES}/test-role",
        )

    def test_role_list(self):
        mock_roles_list = RolesList(
            __root__=[
                RoleInfo(
                    name="role1",
                    desc="Description for role1",
                    clusters=[
                        ClusterPermission(id="cluster1", perm=str(AccessAttr.GET.value))
                    ],
                    buckets=[
                        BucketPermission(
                            bck=BucketModel(name="bucket1"),
                            perm=str(AccessAttr.GET.value),
                        )
                    ],
                    admin=False,
                ),
                RoleInfo(
                    name="role2",
                    desc="Description for role2",
                    clusters=[
                        ClusterPermission(id="cluster2", perm=str(AccessAttr.PUT.value))
                    ],
                    buckets=[
                        BucketPermission(
                            bck=BucketModel(name="bucket2"),
                            perm=str(AccessAttr.PUT.value),
                        )
                    ],
                    admin=True,
                ),
            ]
        )
        self.mock_client.request_deserialize.return_value = mock_roles_list

        roles_list = self.role_manager.list()
        self.assertEqual(roles_list, mock_roles_list)

        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=URL_PATH_AUTHN_ROLES,
            res_model=RolesList,
        )

    def test_role_get(self):
        mock_role_info = RoleInfo(
            name="test-role",
            desc="Test Description",
            clusters=[],
            buckets=[],
            admin=False,
        )
        self.mock_client.request_deserialize.return_value = mock_role_info

        role_info = self.role_manager.get(role_name="test-role")
        self.assertEqual(role_info, mock_role_info)

        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_ROLES}/test-role",
            res_model=RoleInfo,
        )

    def test_role_create(self):
        cluster_id = "cluster_id"
        cluster_alias = "test-cluster"
        urls = ["http://new-cluster-url"]
        mock_cluster_permission = ClusterPermission(
            id=cluster_id, perm=str(AccessAttr.GET.value)
        )
        mock_cluster_list = ClusterList(
            clusters={
                cluster_id: ClusterInfo(id=cluster_id, alias=cluster_alias, urls=urls)
            }
        )
        mock_role_info = RoleInfo(
            name="test-role",
            desc="Test Description",
            clusters=[mock_cluster_permission],
            admin=False,
        )
        self.mock_client.request_deserialize.side_effect = [
            mock_cluster_list,
            mock_role_info,
        ]

        role_info = self.role_manager.create(
            name="test-role",
            desc="Test Description",
            cluster_alias=cluster_alias,
            perms=[AccessAttr.GET],
        )

        self.assertEqual(role_info, mock_role_info)

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_POST,
            path=URL_PATH_AUTHN_ROLES,
            json=role_info.dict(),
        )

    @patch("aistore.sdk.authn.cluster_manager.ClusterManager.get")
    def test_update_role(self, mock_cluster_manager_get):
        cluster_id = "cluster-id"
        role_name = "test-role"
        new_desc = "Updated Description"
        cluster_alias = "test-cluster"
        perms = [AccessAttr.GET]

        mock_role_info = RoleInfo(
            name=role_name,
            desc="Original Description",
            clusters=[ClusterPermission(id=cluster_id, perm=str(AccessAttr.PUT.value))],
        )
        self.mock_client.request_deserialize.return_value = mock_role_info

        mock_cluster_info = ClusterInfo(
            id=cluster_id, alias=cluster_alias, urls=["http://example.com"]
        )
        mock_cluster_manager_get.return_value = mock_cluster_info

        self.role_manager.update(
            name=role_name,
            desc=new_desc,
            cluster_alias=cluster_alias,
            perms=perms,
        )

        expected_updated_role_info = RoleInfo(
            name=role_name,
            desc=new_desc,
            clusters=[ClusterPermission(id=cluster_id, perm=str(AccessAttr.GET.value))],
        )

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_PUT,
            path=f"{URL_PATH_AUTHN_ROLES}/{role_name}",
            json=expected_updated_role_info.dict(),
        )
