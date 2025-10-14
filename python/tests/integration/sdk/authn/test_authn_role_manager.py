#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#
import pytest

from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.authn.errors import ErrRoleNotFound
from tests.integration.sdk.authn.authn_test_base import AuthNTestBase


class TestAuthNRoleManager(AuthNTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.role = self._create_role(
            access_attrs=[AccessAttr.GET], bucket_name=self.bck.name
        )
        self.role_manager = self.authn_client.role_manager()

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

        bck_perms = self.role_manager.get(role_name=self.role.name).buckets
        combined_perms = str(
            (
                AccessAttr.GET.value
                | AccessAttr.OBJ_HEAD.value
                | AccessAttr.LIST_BUCKETS.value
                | AccessAttr.BCK_HEAD.value
                | AccessAttr.OBJ_LIST.value
            )
        )
        self.assertEqual(combined_perms, str(AccessAttr.ACCESS_RO))

        self.assertIn("test-bucket", [perm.bck.name for perm in bck_perms])
        for bck_perm in bck_perms:
            if bck_perm.bck.name == "test-bucket":
                self.assertEqual(combined_perms, bck_perm.perm)

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
        combined_perms = str(
            (
                AccessAttr.LIST_BUCKETS.value
                | AccessAttr.CREATE_BUCKET.value
                | AccessAttr.DESTROY_BUCKET.value
            )
        )

        self.assertEqual(self.uuid, clusters[0].id)
        self.assertEqual(combined_perms, clusters[0].perm)
