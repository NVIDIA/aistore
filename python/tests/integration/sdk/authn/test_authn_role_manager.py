#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest

import pytest

from aistore.sdk.authn.authn_client import AuthNClient
from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.client import Client
from aistore.sdk.authn.errors import ErrRoleNotFound
from tests.integration import (
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
    AUTHN_ENDPOINT,
    CLUSTER_ENDPOINT,
)

from tests.utils import random_string


class TestAuthNRoleManager(
    unittest.TestCase
):  # pylint: disable=too-many-instance-attributes
    def setUp(self) -> None:
        self.authn_client = AuthNClient(AUTHN_ENDPOINT)
        self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self.cluster_manager = self.authn_client.cluster_manager()
        self.cluster_alias = "Test-Cluster-" + random_string()
        self.cluster_info = self.cluster_manager.register(
            self.cluster_alias, [CLUSTER_ENDPOINT]
        )
        self.role_manager = self.authn_client.role_manager()
        self.role = self._create_role()
        self.ais_client = Client(CLUSTER_ENDPOINT, token=self.authn_client.client.token)
        self.uuid = self.ais_client.cluster().get_uuid()

    def tearDown(self) -> None:
        self.cluster_manager.delete(cluster_id=self.uuid)
        self.role_manager.delete(name=self.role.name, missing_ok=True)

    def _create_role(self):
        return self.role_manager.create(
            name="Test-Role-" + random_string(),
            desc="Test Description",
            cluster_alias=self.cluster_info.alias,
            perms=[AccessAttr.GET],
        )

    @pytest.mark.authn
    def test_role_get(self):
        self.assertTrue(self.role == self.role_manager.get(role_name=self.role.name))

    @pytest.mark.authn
    def test_role_list(self):
        self.assertIn(self.role, self.role_manager.list())
        self.role_manager.delete(name=self.role.name)
        self.assertNotIn(self.role, self.role_manager.list())

    @pytest.mark.authn
    def test_role_invalid_delete(self):
        with self.assertRaises(ErrRoleNotFound):
            self.role_manager.delete(name="invalid-name")

    @pytest.mark.authn
    def test_role_delete(self):
        self.role_manager.delete(name=self.role.name)
        with self.assertRaises(ErrRoleNotFound):
            self.role_manager.get(role_name=self.role.name)

    @pytest.mark.authn
    def test_role_invalid_update(self):
        with self.assertRaises(ValueError):
            self.role_manager.update(name=self.role.name)
        with self.assertRaises(ValueError):
            self.role_manager.update(name="invalid-name")
        with self.assertRaises(ValueError):
            self.role_manager.update(name=self.role.name, perms=[AccessAttr.GET])
        with self.assertRaises(ValueError):
            self.role_manager.update(
                name=self.role.name, cluster_alias=self.cluster_alias
            )
        with self.assertRaises(ValueError):
            self.role_manager.update(
                name=self.role.name,
                cluster_alias=self.cluster_alias,
                bucket_name="bucket",
            )
        with self.assertRaises(ValueError):
            self.role_manager.update(name=self.role.name, bucket_name="bucket")
        with self.assertRaises(ValueError):
            self.role_manager.update(
                name=self.role.name, bucket_name="bucket", perms=[AccessAttr.GET]
            )

    @pytest.mark.authn
    def test_role_desc_update(self):
        updated_description = "New Test Description"
        self.role_manager.update(name=self.role.name, desc=updated_description)
        self.assertTrue(
            updated_description == self.role_manager.get(role_name=self.role.name).desc
        )

    @pytest.mark.authn
    def test_role_perms_bucket_update(self):
        self.role_manager.update(
            name=self.role.name,
            bucket_name="test-bucket",
            cluster_alias=self.cluster_alias,
            perms=[AccessAttr.ACCESS_RO],
        )

        buckets = self.role_manager.get(role_name=self.role.name).buckets
        combined_perms = (
            AccessAttr.GET.value
            | AccessAttr.OBJ_HEAD.value
            | AccessAttr.LIST_BUCKETS.value
            | AccessAttr.BCK_HEAD.value
            | AccessAttr.OBJ_LIST.value
        )

        self.assertTrue(
            buckets[0].bck.name == "test-bucket"
            and int(buckets[0].perm) == combined_perms == AccessAttr.ACCESS_RO
        )

    @pytest.mark.authn
    def test_role_perms_cluster_update(self):
        self.role_manager.update(
            name=self.role.name,
            cluster_alias=self.cluster_alias,
            perms=[
                AccessAttr.LIST_BUCKETS,
                AccessAttr.CREATE_BUCKET,
                AccessAttr.DESTROY_BUCKET,
            ],
        )

        clusters = self.role_manager.get(role_name=self.role.name).clusters
        combined_perms = (
            AccessAttr.LIST_BUCKETS.value
            | AccessAttr.CREATE_BUCKET.value
            | AccessAttr.DESTROY_BUCKET.value
        )

        self.assertTrue(
            clusters[0].id == self.uuid and int(clusters[0].perm) == combined_perms
        )
