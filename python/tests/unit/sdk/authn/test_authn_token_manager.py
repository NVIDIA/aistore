#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock, patch

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

    @patch("aistore.sdk.authn.token_manager.logger")
    def test_token_revoke(self, mock_logger):
        token = "test-token"
        self.token_manager.revoke(token=token)
        mock_logger.info.assert_called_once_with("Revoking token")
        self.assertNotIn(token, repr(mock_logger.method_calls))
        self.mock_client.request.assert_called_once_with(
            method=HTTP_METHOD_DELETE,
            path=f"{URL_PATH_AUTHN_TOKENS}",
            json=TokenMsg(token=token).model_dump(),
        )

    @patch("aistore.sdk.authn.token_manager.logger")
    def test_token_revoke_failure(self, mock_logger):
        token = "test-token"
        self.mock_client.request.side_effect = RuntimeError("request failed")

        with self.assertRaisesRegex(RuntimeError, "request failed"):
            self.token_manager.revoke(token=token)

        mock_logger.info.assert_called_once_with("Revoking token")
        self.assertNotIn(token, repr(mock_logger.method_calls))
