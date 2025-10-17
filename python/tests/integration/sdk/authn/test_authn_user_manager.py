#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#
import pytest

from aistore.sdk.authn.errors import ErrUserInvalidCredentials, ErrUserNotFound
from aistore.sdk.authn.types import AccessAttr
from tests.integration.sdk.authn.authn_test_base import AuthNTestBase
from tests.utils import random_string


class TestAuthNUserManager(AuthNTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.role = self._create_role([AccessAttr.LIST_BUCKETS])
        self.test_pass = "test-create-user-pass"
        self.user = self._create_user(password=self.test_pass, roles=[self.role.name])
        self.user_manager = self.authn_client.user_manager()

    def _create_bucket(self):
        return self.ais_client.bucket("Test-Bucket-" + random_string()).create()

    @pytest.mark.authn
    def test_create_user(self):
        # Test invalid login with incorrect password
        with self.assertRaises(ErrUserInvalidCredentials):
            self.authn_client.login(self.user.id, "1234")

        # Test valid login with correct password
        token = self.authn_client.login(self.user.id, self.test_pass)
        client = self._create_ais_client(token)

        # Test user role and permissions
        client.cluster().list_buckets()
        bck = self._assert_forbidden(client.bucket("any").create)
        # Mark for cleanup if creation did succeed
        if bck:
            self.buckets.append(bck)

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
        self.assertIn(self.user.id, users)
        self.assertEqual(self.user, users[self.user.id])

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
        token = self.authn_client.login(self.user.id, self.test_pass)
        client = self._create_ais_client(token)

        # Test updated user role and permissions
        self.buckets.append(client.bucket(random_string()).create())
        self._assert_forbidden(client.cluster().list_buckets)

    @pytest.mark.authn
    def test_update_user_no_roles(self):
        new_pw = "123456"
        # Update the user with a new password without changing the roles
        self.user_manager.update(
            username=self.user.id,
            password=new_pw,
        )

        # Test invalid login with old password
        with self.assertRaises(ErrUserInvalidCredentials):
            self.authn_client.login(self.user.id, self.test_pass)

        # Check if the user has the same roles
        updated_user = self.user_manager.get(username=self.user.id)
        self.assertEqual(self.user.roles, updated_user.roles)

        # Test valid login with updated password
        token = self.authn_client.login(self.user.id, new_pw)
        client = self._create_ais_client(token)

        # Test updated user role and permissions
        client.cluster().list_buckets()
        bck = self._assert_forbidden(client.bucket(random_string()).create)
        # Mark for cleanup if creation did succeed
        if bck:
            self.buckets.append(bck)

    @pytest.mark.authn
    def test_update_user(self):
        new_pw = "1234567"
        # Create a new role and update the user with the new role
        new_role = self._create_role([AccessAttr.CREATE_BUCKET])
        self.user_manager.update(
            username=self.user.id,
            password=new_pw,
            roles=[new_role.name],
        )

        # Test invalid login with old password
        with self.assertRaises(ErrUserInvalidCredentials):
            self.authn_client.login(self.user.id, self.test_pass)

        # Check if the user has the new role and not the old role
        updated_user = self.user_manager.get(username=self.user.id)
        self.assertNotIn(self.role, updated_user.roles)
        self.assertIn(new_role, updated_user.roles)

        # Test valid login with updated password
        token = self.authn_client.login(self.user.id, new_pw)
        client = self._create_ais_client(token)

        # Test updated user role and permissions
        self.buckets.append(client.bucket(random_string()).create())
        self._assert_forbidden(client.cluster().list_buckets)
