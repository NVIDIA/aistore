#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import patch, Mock

from urllib3 import Retry

from aistore.sdk.authn import AuthNClient
from aistore.sdk.authn.types import TokenMsg, LoginMsg

from tests.utils import test_cases


# pylint: disable=unused-variable, duplicate-code
class TestAuthNClient(unittest.TestCase):
    def setUp(self) -> None:
        self.endpoint = "http://authn-endpoint"
        self.client = AuthNClient(self.endpoint)

    @patch("aistore.sdk.authn.authn_client.RequestClient")
    def test_init_defaults(self, mock_request_client):
        AuthNClient(self.endpoint)
        mock_request_client.assert_called_with(
            endpoint=self.endpoint,
            skip_verify=False,
            ca_cert=None,
            timeout=None,
            retry=None,
            token=None,
        )

    @test_cases(
        (True, None, None, None, "dummy.token"),
        (False, "ca_cert_location", None, None, None),
        (False, None, 30.0, Retry(total=20), None),
        (False, None, (10, 30.0), Retry(total=20), "dummy.token"),
    )
    @patch("aistore.sdk.authn.authn_client.RequestClient")
    def test_init(self, test_case, mock_request_client):
        skip_verify, ca_cert, timeout, retry, token = test_case
        # print all vars
        print(
            f"skip_verify: {skip_verify}, ca_cert: {ca_cert}, timeout: {timeout}, retry: {retry}, token: {token}"
        )
        AuthNClient(
            self.endpoint,
            skip_verify=skip_verify,
            ca_cert=ca_cert,
            timeout=timeout,
            retry=retry,
            token=token,
        )
        mock_request_client.assert_called_with(
            endpoint=self.endpoint,
            skip_verify=skip_verify,
            ca_cert=ca_cert,
            timeout=timeout,
            retry=retry,
            token=token,
        )

    @patch("aistore.sdk.request_client.RequestClient.request_deserialize")
    def test_login_success(self, mock_request_deserialize):
        mock_token_msg = Mock(token="mock_token")
        mock_request_deserialize.return_value = mock_token_msg

        username = "testuser"
        password = "testpassword"
        token = self.client.login(username, password)

        self.assertEqual(token, "mock_token")
        mock_request_deserialize.assert_called_once_with(
            "post",
            path=f"users/{username}",
            json=LoginMsg(password=password).as_dict(),
            res_model=TokenMsg,
        )

    @patch("aistore.sdk.request_client.RequestClient.request_deserialize")
    def test_login_empty_password(self, mock_request_deserialize):
        username = "testuser"
        password = " "

        with self.assertRaises(ValueError) as context:
            self.client.login(username, password)

        self.assertEqual(
            str(context.exception), "Password cannot be empty or spaces only"
        )
        mock_request_deserialize.assert_not_called()

    @patch("aistore.sdk.request_client.RequestClient.request_deserialize")
    def test_login_failed_authentication(self, mock_request_deserialize):
        mock_request_deserialize.side_effect = Exception("Authentication failed")

        username = "testuser"
        password = "wrongpassword"

        with self.assertRaises(Exception) as context:
            self.client.login(username, password)

        self.assertEqual(str(context.exception), "Authentication failed")
        mock_request_deserialize.assert_called_once_with(
            "post",
            path=f"users/{username}",
            json=LoginMsg(password=password).as_dict(),
            res_model=TokenMsg,
        )
