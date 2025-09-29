#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import unittest
from typing import List, Optional
from pathlib import Path

import pytest

from aistore.sdk.authn.authn_client import AuthNClient
from aistore.sdk.client import Client
from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.authn.types import AccessAttr
from tests.integration import (
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
    AUTHN_ENDPOINT,
    CLUSTER_ENDPOINT,
)
from tests.utils import random_string


class TestAuthNAccess(
    unittest.TestCase
):  # pylint: disable=too-many-instance-attributes, too-many-public-methods
    def setUp(self) -> None:
        # AuthN Client
        self.authn_client = AuthNClient(AUTHN_ENDPOINT)
        self.admin_token = self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)

        # AIS Client
        self._create_ais_client(self.admin_token)
        self.uuid = self.ais_client.cluster().get_uuid()

        # Register the AIS Cluster
        self.cluster_alias = "test-cluster"
        self.cluster_manager = self.authn_client.cluster_manager()
        self.cluster_info = self.cluster_manager.register(
            self.cluster_alias, [CLUSTER_ENDPOINT]
        )

        # Test Role and User
        self.role_manager = self.authn_client.role_manager()
        self.user_manager = self.authn_client.user_manager()

        # Initialize Role and User attributes
        self.role = None
        self.user = None

        # Test Bucket and Object
        bucket_name = f"Test-Bucket-{random_string()}"
        self.bucket = self.ais_client.bucket(bucket_name).create()
        object_name = "Test-Object"
        self.object = self.bucket.object(object_name)
        self.object.get_writer().put_content(b"Test-Data")

    def tearDown(self) -> None:
        self.admin_token = self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self._create_ais_client(self.admin_token)
        self.cluster_manager.delete(cluster_id=self.uuid)
        self.role_manager.delete(name=self.role.name, missing_ok=True)
        self.user_manager.delete(username=self.user.id, missing_ok=True)
        self.bucket.delete(missing_ok=True)

    def _create_role(
        self, access_attrs: List[AccessAttr], bucket_name: Optional[str] = None
    ):
        if bucket_name:
            return self.role_manager.create(
                name=f"Test-Role-{random_string()}",
                desc="Test Description",
                cluster_alias=self.cluster_info.alias,
                perms=access_attrs,
                bucket_name=bucket_name,
            )
        return self.role_manager.create(
            name=f"Test-Role-{random_string()}",
            desc="Test Description",
            cluster_alias=self.cluster_info.alias,
            perms=access_attrs,
        )

    def _create_user(self, roles: List[str], password: str = "12345"):
        return self.user_manager.create(
            username=f"Test-User-{random_string()}",
            password=password,
            roles=roles,
        )

    def _create_ais_client(self, token: str):
        self.ais_client = Client(CLUSTER_ENDPOINT, token=token)

    # pylint: disable=inconsistent-return-statements
    def _assert_does_not_raise(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            self.fail(f"An exception was raised: {err}")

    # Test Individual Access Permissions

    @pytest.mark.authn
    def test_access_obj_get(self):
        """Test object get permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.GET], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .get_reader()
            .read_all()
        )

    @pytest.mark.authn
    def test_access_obj_put(self):
        """Test object put permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.PUT], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .get_writer()
            .put_content(b"Test-Data")
        )

    @pytest.mark.authn
    def test_access_obj_head(self):
        """Test object head permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.OBJ_HEAD], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .head()
        )

    @pytest.mark.authn
    def test_access_obj_append(self):
        """Test object append permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.APPEND], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .get_writer()
            .append_content(b"Test-Data")
        )

    @pytest.mark.authn
    def test_access_obj_delete(self):
        """Test object delete permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.OBJ_DELETE], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .delete()
        )

    @pytest.mark.authn
    def test_access_bck_head(self):
        """Test bucket head permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.BCK_HEAD], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name).head()
        )

    @pytest.mark.authn
    def test_access_obj_list(self):
        self.role = self._create_role(
            access_attrs=[AccessAttr.OBJ_LIST], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name).list_objects()
        )

    @pytest.mark.authn
    def test_access_list_buckets(self):
        """Test list buckets permission."""
        self.role = self._create_role(access_attrs=[AccessAttr.LIST_BUCKETS])
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(lambda: self.ais_client.cluster().list_buckets())

    @pytest.mark.authn
    def test_access_create_bucket(self):
        """Test create bucket permission."""
        self.role = self._create_role(access_attrs=[AccessAttr.CREATE_BUCKET])
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        bucket_name = f"Test-Bucket-Create{random_string()}"
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(bucket_name).create()
        )

    @pytest.mark.authn
    def test_access_destroy_bucket(self):
        """Test destroy bucket permission."""
        self.role = self._create_role(access_attrs=[AccessAttr.DESTROY_BUCKET])
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name).delete()
        )

    @pytest.mark.authn
    def test_access_show_cluster(self):
        """Test show cluster permission."""
        self.role = self._create_role(access_attrs=[AccessAttr.SHOW_CLUSTER])
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(lambda: self.ais_client.cluster().get_info())

    @pytest.mark.authn
    def test_access_obj_promote(self):
        """Test object promote permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.PROMOTE], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        local_file_path = Path("test_promote_file.txt").absolute()
        local_file_content = "Test content for promotion"
        with open(local_file_path, "w", encoding=UTF_ENCODING) as file:
            file.write(local_file_content)

        # Verify Access
        obj_name = "promoted_test_file"
        self.ais_client.bucket(self.bucket.name).object(obj_name).promote(
            str(local_file_path)
        )

        local_file_path.unlink()

    @pytest.mark.authn
    def test_access_move_bucket(self):
        """Test move bucket permission."""
        self.role = self._create_role(access_attrs=[AccessAttr.MOVE_BUCKET])
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self.ais_client.bucket(self.bucket.name).rename(self.bucket.name + "-Renamed")

    @pytest.mark.authn
    def test_access_obj_update(self):
        """Test object update permission."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.OBJ_UPDATE], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self.ais_client.bucket(self.bucket.name).object(
            self.object.name
        ).get_writer().set_custom_props(custom_metadata={"Test-Key": "Test-Value"})

    # Test Derived Roles (RO, RW, SU)

    @pytest.mark.authn
    def test_access_ro_bucket_specific(self):
        """Test bucket-specific role with read-only permissions."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.ACCESS_RO], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .get_reader()
            .read_all()
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .head()
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name).head()
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name).list_objects()
        )

    @pytest.mark.authn
    def test_access_ro_cluster_wide(self):
        """Test cluster-wide role with read-only permissions."""
        self.role = self._create_role(access_attrs=[AccessAttr.ACCESS_RO])
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(lambda: self.ais_client.cluster().list_buckets())

    @pytest.mark.authn
    def test_access_rw_bucket_specific(self):
        """Test bucket-specific role with read-write permissions."""
        self.role = self._create_role(
            access_attrs=[AccessAttr.ACCESS_RW], bucket_name=self.bucket.name
        )
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        test_rw_object = self.ais_client.bucket(self.bucket.name).object(
            "Test-RW-Object"
        )

        # Verify Access
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(test_rw_object.name)
            .get_writer()
            .put_content(b"Test-Data")
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(test_rw_object.name)
            .get_writer()
            .append_content(b"Test-Data")
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(test_rw_object.name)
            .delete()
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .head()
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name)
            .object(self.object.name)
            .get_reader()
            .read_all()
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name).head()
        )
        self._assert_does_not_raise(
            lambda: self.ais_client.bucket(self.bucket.name).list_objects()
        )

    @pytest.mark.authn
    def test_access_rw_cluster_wide(self):
        """Test cluster-wide role with read-write permissions."""
        self.role = self._create_role(access_attrs=[AccessAttr.ACCESS_RW])
        self.user = self._create_user(roles=[self.role.name])

        user_token = self.authn_client.login(username=self.user.id, password="12345")
        self._create_ais_client(user_token)

        # Verify Access
        self._assert_does_not_raise(lambda: self.ais_client.cluster().list_buckets())

    # TODO: Add tests for SU derived role here

    # Skipped Tests Requiring Attention

    @pytest.mark.authn
    @pytest.mark.skip(reason="Object patch not implemented in SDK")
    def test_access_obj_patch(self):
        """Test object patch permission."""

    @pytest.mark.authn
    @pytest.mark.skip(reason="Bucket set ACL not implemented in SDK")
    def test_access_bck_set_acl(self):
        """Test bucket set ACL permission."""
