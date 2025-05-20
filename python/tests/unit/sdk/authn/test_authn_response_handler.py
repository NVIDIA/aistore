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
from tests.utils import cases, handler_parse_and_assert


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
    def test_parse_authn_error(self, test_case):
        handler_parse_and_assert(self, AuthNResponseHandler(), AuthNError, test_case)
