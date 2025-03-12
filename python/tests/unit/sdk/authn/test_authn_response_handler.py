import unittest

from aistore.sdk.authn.errors import (
    ErrUserNotFound,
    ErrUserAlreadyExists,
    ErrRoleNotFound,
    ErrRoleAlreadyExists,
    ErrClusterNotFound,
    ErrClusterAlreadyRegistered,
    ErrUserInvalidCredentials,
    AuthNError,
)
from aistore.sdk.authn.response_handler import AuthNResponseHandler
from tests.utils import cases, create_api_error_response


# pylint: disable=unused-variable
class TestAuthNResponseHandler(unittest.TestCase):
    @cases(
        ("", AuthNError, None),
        ("generic error message", AuthNError, 500),
        ("generic error message", AuthNError, 399),
        ('user "test-user" does not exist', ErrUserNotFound, 404),
        ('user "test-user" already exists', ErrUserAlreadyExists, 409),
        ('role "test-role" does not exist', ErrRoleNotFound, 404),
        ('role "test-role" already exists', ErrRoleAlreadyExists, 409),
        ("cluster test-cluster does not exist", ErrClusterNotFound, 404),
        (
            "cluster OnBejJEpe[OnBejJEpe] already registered",
            ErrClusterAlreadyRegistered,
            409,
        ),
        ("invalid credentials", ErrUserInvalidCredentials, 401),
    )
    def test_parse_ais_error(self, test_case):
        err_msg, expected_err, err_status = test_case
        test_url = "http://test-url"
        response = create_api_error_response(test_url, err_status, err_msg)

        err = AuthNResponseHandler().parse_error(response)
        self.assertIsInstance(err, AuthNError)
        self.assertEqual(err_status, err.status_code)
        self.assertEqual(err_msg, err.message)
        self.assertEqual(test_url, err.req_url)
