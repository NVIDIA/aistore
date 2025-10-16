#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#
import tempfile
from typing import List
from pathlib import Path

import pytest

from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.authn.types import AccessAttr
from tests.integration.sdk.authn.authn_test_base import AuthNTestBase
from tests.utils import random_string


class TestAuthNAccess(AuthNTestBase):  # pylint: disable=too-many-public-methods
    def setUp(self) -> None:
        super().setUp()
        # Test Role and User
        self.role_manager = self.authn_client.role_manager()
        self.user_manager = self.authn_client.user_manager()

        self.object = self.bck.object(random_string())
        self.object.get_writer().put_content(b"Test-Data")

    def _create_client_with_access(self, access_attrs: List[AccessAttr], scoped=True):
        if scoped:
            role = self._create_role(
                access_attrs=access_attrs, bucket_name=self.bck.name
            )
        else:
            role = self._create_role(access_attrs=access_attrs)
        user = self._create_user(roles=[role.name])

        user_token = self.authn_client.login(username=user.id, password="12345")
        return self._create_ais_client(user_token)

    # Test Individual Access Permissions

    @pytest.mark.authn
    def test_access_obj_get(self):
        """Test object get permission."""
        client = self._create_client_with_access([AccessAttr.GET])
        client.bucket(self.bck.name).object(self.object.name).get_reader().read_all()

    @pytest.mark.authn
    def test_access_obj_put(self):
        """Test object put permission."""
        client = self._create_client_with_access([AccessAttr.PUT])
        client.bucket(self.bck.name).object(self.object.name).get_writer().put_content(
            b"Test-Data"
        )

    @pytest.mark.authn
    def test_access_obj_head(self):
        """Test object head permission."""
        client = self._create_client_with_access([AccessAttr.OBJ_HEAD])
        client.bucket(self.bck.name).object(self.object.name).head()

    @pytest.mark.authn
    def test_access_obj_append(self):
        """Test object append permission."""
        client = self._create_client_with_access([AccessAttr.APPEND])
        client.bucket(self.bck.name).object(
            self.object.name
        ).get_writer().append_content(b"Test-Data")

    @pytest.mark.authn
    def test_access_obj_delete(self):
        """Test object delete permission."""
        client = self._create_client_with_access([AccessAttr.OBJ_DELETE])
        client.bucket(self.bck.name).object(self.object.name).delete()

    @pytest.mark.authn
    def test_access_bck_head(self):
        """Test bucket head permission."""
        client = self._create_client_with_access([AccessAttr.BCK_HEAD])
        client.bucket(self.bck.name).head()

    @pytest.mark.authn
    def test_access_obj_list(self):
        client = self._create_client_with_access([AccessAttr.OBJ_LIST])
        client.bucket(self.bck.name).list_objects()

    @pytest.mark.authn
    def test_access_list_buckets(self):
        """Test list buckets permission."""
        client = self._create_client_with_access(
            [AccessAttr.LIST_BUCKETS], scoped=False
        )
        client.cluster().list_buckets()

    @pytest.mark.authn
    def test_access_create_bucket(self):
        """Test create bucket permission."""
        client = self._create_client_with_access(
            access_attrs=[AccessAttr.CREATE_BUCKET], scoped=False
        )

        # store and append for cleanup
        bck = client.bucket(f"Test-Bucket-Create{random_string()}").create()
        self.buckets.append(bck)

    @pytest.mark.authn
    def test_access_destroy_bucket(self):
        """Test destroy bucket permission."""
        client = self._create_client_with_access(
            access_attrs=[AccessAttr.DESTROY_BUCKET], scoped=False
        )
        client.bucket(self.bck.name).delete()

    @pytest.mark.authn
    def test_access_show_cluster(self):
        """Test show cluster permission."""
        client = self._create_client_with_access(
            access_attrs=[AccessAttr.SHOW_CLUSTER], scoped=False
        )
        client.cluster().get_info()

    @pytest.mark.authn
    def test_access_obj_promote(self):
        """Test object promote permission."""
        client = self._create_client_with_access(access_attrs=[AccessAttr.PROMOTE])

        with tempfile.TemporaryDirectory() as dirname:
            tmpdir = Path(dirname)
            local_file_path = tmpdir.joinpath("test_promote_file.txt").absolute()
            local_file_content = "Test content for promotion"
            with open(local_file_path, "w", encoding=UTF_ENCODING) as file:
                file.write(local_file_content)

            client.bucket(self.bck.name).object("promoted_test_file").promote(
                str(local_file_path)
            )

    @pytest.mark.authn
    def test_access_move_bucket(self):
        """Test move bucket permission."""
        new_name = self.bck.name + "-Renamed"
        client = self._create_client_with_access(
            access_attrs=[AccessAttr.MOVE_BUCKET], scoped=False
        )
        client.bucket(self.bck.name).rename(new_name)
        # Mark for cleanup
        self.buckets.append(client.bucket(new_name))

    @pytest.mark.authn
    def test_access_obj_update(self):
        """Test object update permission."""
        client = self._create_client_with_access(access_attrs=[AccessAttr.OBJ_UPDATE])
        client.bucket(self.bck.name).object(
            self.object.name
        ).get_writer().set_custom_props(custom_metadata={"Test-Key": "Test-Value"})

    # Test Derived Roles (RO, RW, SU)

    @pytest.mark.authn
    def test_access_ro_bucket_specific(self):
        """Test bucket-specific role with read-only permissions."""
        client = self._create_client_with_access(access_attrs=[AccessAttr.ACCESS_RO])

        bck = client.bucket(self.bck.name)
        obj = bck.object(self.object.name)

        obj.get_reader().read_all()
        obj.head()
        bck.head()
        bck.list_objects()

    @pytest.mark.authn
    def test_access_ro_cluster_wide(self):
        """Test cluster-wide role with read-only permissions."""
        client = self._create_client_with_access(
            access_attrs=[AccessAttr.ACCESS_RO], scoped=False
        )
        client.cluster().list_buckets()

    @pytest.mark.authn
    def test_access_rw_bucket_specific(self):
        """Test bucket-specific role with read-write permissions."""
        client = self._create_client_with_access(access_attrs=[AccessAttr.ACCESS_RW])
        bck = client.bucket(self.bck.name)

        obj = bck.object(self.object.name)
        obj.head()
        obj.get_reader().read_all()
        # Write-required
        obj.get_writer().put_content(b"Test-Data")
        obj.get_writer().append_content(b"Test-Data")
        obj.delete()
        # Bucket RW
        bck.head()
        bck.list_objects()

    @pytest.mark.authn
    def test_access_rw_cluster_wide(self):
        """Test cluster-wide role with read-write permissions."""
        client = self._create_client_with_access(
            access_attrs=[AccessAttr.ACCESS_RW], scoped=False
        )
        client.cluster().list_buckets()

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
