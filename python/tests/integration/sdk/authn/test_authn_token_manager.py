#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import unittest

import pytest

from aistore.sdk.authn.authn_client import AuthNClient
from aistore.sdk.client import Client
from tests.integration import (
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
    AUTHN_ENDPOINT,
    CLUSTER_ENDPOINT,
)
from tests.utils import random_string


class TestAuthNTokenManager(
    unittest.TestCase
):  # pylint: disable=too-many-instance-attributes
    def setUp(self) -> None:
        # AuthN Client
        self.authn_client = AuthNClient(AUTHN_ENDPOINT)
        self.token = self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)

        # AIS Client
        self._create_ais_client()
        self.uuid = self.ais_client.cluster().get_uuid()

        # Register the AIS Cluster
        self.cluster_alias = "Test-Cluster" + random_string()
        self.cluster_manager = self.authn_client.cluster_manager()
        self.cluster_info = self.cluster_manager.register(
            self.cluster_alias, [CLUSTER_ENDPOINT]
        )

        self.token_manager = self.authn_client.token_manager()

    def tearDown(self) -> None:
        self.authn_client.login(AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS)
        self._create_ais_client()
        self.cluster_manager.delete(cluster_id=self.uuid)

    def _create_ais_client(self):
        self.ais_client = Client(CLUSTER_ENDPOINT, token=self.authn_client.client.token)

    @pytest.mark.authn
    def test_revoke_token(self):
        # Assert token is valid and working
        self.ais_client.cluster().list_buckets()

        # Revoke the token
        self.token_manager.revoke(self.token)

        # Attempt to use the token after revoking it
        with self.assertRaises(Exception):
            self.ais_client.cluster().list_buckets()
