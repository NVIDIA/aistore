#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import unittest
from typing import List

import pytest

from aistore.sdk.authn.authn_client import AuthNClient
from aistore.sdk.authn.errors import ErrUserInvalidCredentials, ErrUserNotFound
from aistore.sdk.client import Client
from aistore.sdk.authn.types import AccessAttr
from aistore.sdk.errors import AISError
from tests.integration import (
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
    AUTHN_ENDPOINT,
    CLUSTER_ENDPOINT,
)
from tests.utils import random_string


class TestAuthNUserManager(
    unittest.TestCase
):  # pylint: disable=too-many-instance-attributes
    def setUp(self) -> None:
        # AuthN Client
        self.authn_client = AuthNClient(AUTHN_ENDPOINT)
        self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)

        # AIS Client
        self._create_ais_client()
        self.uuid = self.ais_client.cluster().get_uuid()

        # Register the AIS Cluster
        self.cluster_alias = "test-cluster"
        self.cluster_manager = self.authn_client.cluster_manager()
        self.cluster_info = self.cluster_manager.register(
            self.cluster_alias, [CLUSTER_ENDPOINT]
        )

        # Test Role and User
        self.role_manager = self.authn_client.role_manager()
        self.role = self._create_role([AccessAttr.LIST_BUCKETS])
        self.user_manager = self.authn_client.user_manager()
        self.user = self._create_user(password="12345", roles=[self.role.name])
        self.bucket = None

    def tearDown(self) -> None:
        self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self._create_ais_client()
        self.cluster_manager.delete(cluster_id=self.uuid)
        self.role_manager.delete(name=self.role.name)
        self.user_manager.delete(username=self.user.id, missing_ok=True)
        if self.bucket:
            self.ais_client.bucket(self.bucket.name).delete()

    def _create_role(self, access_attrs: List[AccessAttr]):
        return self.role_manager.create(
            name="Test-Role-" + random_string(),
            desc="Test Description",
            cluster_alias=self.cluster_info.alias,
            perms=access_attrs,
        )

    def _create_user(self, roles: List[str], password: str = ""):
        return self.user_manager.create(
            username=f"Test-User-{random_string()}",
            password=password,
            roles=roles,
        )

    def _create_bucket(self):
        return self.ais_client.bucket("Test-Bucket-" + random_string()).create()

    def _create_ais_client(self):
        self.ais_client = Client(CLUSTER_ENDPOINT, token=self.authn_client.client.token)

    @pytest.mark.authn
    def test_create_user(self):
        # Test invalid login with incorrect password
        with self.assertRaises(ErrUserInvalidCredentials):
            self.authn_client.login(self.user.id, "1234")

        # Test valid login with correct password
        self.authn_client.login(self.user.id, "12345")
        self._create_ais_client()

        # Test user role and permissions
        self.ais_client.cluster().list_buckets()
        with self.assertRaises(AISError):
            self.bucket = self._create_bucket()

    @pytest.mark.authn
    def test_get_user(self):
        # Retrieve the user and check if it is the same
        retrieved_user = self.user_manager.get(username=self.user.id)
        self.assertEqual(self.user, retrieved_user)

    @pytest.mark.authn
    def test_delete_user(self):
        # Delete the user and check if it exists
        self.user_manager.delete(username=self.user.id)
        with self.assertRaises(ErrUserNotFound):
            self.user_manager.get(username=self.user.id)

    @pytest.mark.authn
    def test_list_users(self):
        # List all users and check if the user exists
        users = self.user_manager.list()
        self.assertIn(self.user, users)
        self.assertEqual(users[self.user.id].roles, self.user.roles)

        # Delete the user and check if it exists in the list
        self.user_manager.delete(username=self.user.id)
        users_after_deletion = self.user_manager.list()
        self.assertNotIn(self.user, users_after_deletion)

    @pytest.mark.authn
    def test_invalid_update_user(self):
        # Test invalid update with no update parameters
        with self.assertRaises(ValueError):
            self.user_manager.update(username=self.user.id)

    @pytest.mark.authn
    def test_update_user_no_password(self):
        # Create a new role and update the user with the new role without changing the password
        new_role = self._create_role([AccessAttr.CREATE_BUCKET])
        self.user_manager.update(
            username=self.user.id,
            roles=[new_role.name],
        )

        # Check if the user has the new role and not the old role
        updated_user = self.user_manager.get(username=self.user.id)
        self.assertNotIn(self.role, updated_user.roles)
        self.assertIn(new_role, updated_user.roles)

        # Test valid login with existing password
        self.authn_client.login(self.user.id, "12345")
        self._create_ais_client()

        # Test updated user role and permissions
        self.bucket = self._create_bucket()
        with self.assertRaises(AISError):
            self.ais_client.cluster().list_buckets()

    @pytest.mark.authn
    def test_update_user_no_roles(self):
        # Update the user with a new password without changing the roles
        self.user_manager.update(
            username=self.user.id,
            password="123456",
        )

        # Test invalid login with old password
        with self.assertRaises(ErrUserInvalidCredentials):
            self.authn_client.login(self.user.id, "12345")

        # Check if the user has the same roles
        updated_user = self.user_manager.get(username=self.user.id)
        self.assertEqual(self.user.roles, updated_user.roles)

        # Test valid login with updated password
        self.authn_client.login(self.user.id, "123456")
        self._create_ais_client()

        # Test updated user role and permissions
        self.ais_client.cluster().list_buckets()
        with self.assertRaises(AISError):
            self.bucket = self._create_bucket()

    @pytest.mark.authn
    def test_update_user(self):
        # Create a new role and update the user with the new role
        new_role = self._create_role([AccessAttr.CREATE_BUCKET])
        self.user_manager.update(
            username=self.user.id,
            password="1234567",
            roles=[new_role.name],
        )

        # Test invalid login with old password
        with self.assertRaises(ErrUserInvalidCredentials):
            self.authn_client.login(self.user.id, "12345")

        # Check if the user has the new role and not the old role
        updated_user = self.user_manager.get(username=self.user.id)
        self.assertNotIn(self.role, updated_user.roles)
        self.assertIn(new_role, updated_user.roles)

        # Test valid login with updated password
        self.authn_client.login(self.user.id, "1234567")
        self._create_ais_client()

        # Test updated user role and permissions
        self.bucket = self._create_bucket()
        with self.assertRaises(AISError):
            self.ais_client.cluster().list_buckets()
