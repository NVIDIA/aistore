#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#
import pytest

from aistore.sdk.authn.access_attr import AccessAttr
from tests.integration.sdk.authn.authn_test_base import AuthNTestBase
from tests.utils import random_string


class TestAuthNTokenManager(AuthNTestBase):
    @pytest.mark.authn
    def test_revoke_token(self):
        # Create a user with a token
        list_bck_role = self._create_role([AccessAttr.LIST_BUCKETS])
        user_pw = random_string()
        user = self._create_user(roles=[list_bck_role.name], password=user_pw)
        token = self.authn_client.login(user.id, user_pw)

        client = self._create_ais_client(token)
        # Assert token is valid and working
        client.cluster().list_buckets()

        # Revoke the token
        self.authn_client.token_manager().revoke(token)

        # Attempt to use the token after revoking it
        self._assert_unauthorized(client.cluster().list_buckets)
