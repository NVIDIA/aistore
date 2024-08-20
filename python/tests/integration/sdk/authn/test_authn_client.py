#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import unittest

import pytest

from aistore.sdk import AuthNClient, Client
from aistore.sdk.errors import AISError
from aistore.sdk.const import PROVIDER_AIS
from aistore.sdk.authn.errors import ErrUserInvalidCredentials
from tests.integration import (
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
    AUTHN_ENDPOINT,
    CLUSTER_ENDPOINT,
)
from tests.utils import random_string


# pylint: disable=duplicate-code
class TestAuthNClient(unittest.TestCase):
    def setUp(self) -> None:
        # AIStore Client
        self.bck_name = random_string()
        self.provider = PROVIDER_AIS

        # AuthN Client
        self.authn_client = AuthNClient(AUTHN_ENDPOINT)

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
        token = self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self.assertIsNotNone(token)

        self.authn_client.logout()
        self.assertIsNone(self.authn_client.client.token)

    @pytest.mark.authn
    def test_create_bucket_without_token(self):
        ais_client = Client(CLUSTER_ENDPOINT)
        bucket = ais_client.bucket(self.bck_name, provider=self.provider)

        with self.assertRaises(AISError) as context:
            bucket.create()

        self.assertEqual(context.exception.status_code, 401)
        self.assertIn("token required", context.exception.message)

    @pytest.mark.authn
    def test_create_bucket_with_token(self):
        token = self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self.assertIsNotNone(token)

        ais_client = Client(CLUSTER_ENDPOINT, token=token)
        bucket = ais_client.bucket(self.bck_name, provider=self.provider)
        bucket.create()

        self.assertIsNotNone(bucket.info())
        bucket.delete()
