#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock

from aistore.sdk.request_client import RequestClient
from aistore.sdk.authn.types import TokenMsg
from aistore.sdk.authn.token_manager import TokenManager
from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    URL_PATH_AUTHN_TOKENS,
)


class TestAuthNTokenManager(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock(RequestClient)
        self.token_manager = TokenManager(self.mock_client)

    def test_token_revoke(self):
        token = "test-token"
        self.token_manager.revoke(token=token)
        self.mock_client.request.assert_called_once_with(
            method=HTTP_METHOD_DELETE,
            path=f"{URL_PATH_AUTHN_TOKENS}",
            json=TokenMsg(token=token).model_dump(),
        )
