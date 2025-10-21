#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#
import pytest

from aistore.sdk import Client
from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.errors import AISError
from aistore.sdk.authn.errors import ErrUserInvalidCredentials
from tests.integration import (
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
    CLUSTER_ENDPOINT,
)
from tests.integration.sdk.authn.authn_test_base import AuthNTestBase
from tests.utils import random_string


class TestAuthNClient(AuthNTestBase):
    @pytest.mark.authn
    def test_login_failure(self):
        with self.assertRaises(ErrUserInvalidCredentials) as context:
            self.authn_client.login(AIS_AUTHN_SU_NAME, "WRONG_PASSWORD")

        self.assertEqual(context.exception.status_code, 401)
        self.assertIn("invalid credentials", context.exception.message)

    @pytest.mark.authn
    def test_login_success(self):
        token = self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self.assertIsNotNone(token)

    @pytest.mark.authn
    def test_logout(self):
        # Create a new admin user with a token
        # We will revoke this token, so don't use the main admin user/password
        role = self._create_role([AccessAttr.ACCESS_SU])
        user_pw = random_string()
        user = self._create_user(roles=[role.name], password=user_pw)
        token = self.authn_client.login(user.id, user_pw)
        self.assertIsNotNone(token)

        client = self._create_ais_client(token)
        # Assert token is valid and working
        client.cluster().get_info()

        self.authn_client.logout()
        self.assertIsNone(self.authn_client.client.token)
        with self.assertRaises(AISError) as context:
            client.cluster().get_info()
        self.assertIn("revoked", context.exception.message)

    @pytest.mark.authn
    def test_create_bucket_without_token(self):
        unauthenticated_client = Client(CLUSTER_ENDPOINT)
        bucket = unauthenticated_client.bucket(random_string())

        with self.assertRaises(AISError) as context:
            bucket.create()

        self.assertEqual(context.exception.status_code, 401)
        self.assertIn("token required", context.exception.message)
