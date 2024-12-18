#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

import requests

from aistore.sdk.utils import parse_http_error_or_raise
from aistore.sdk.authn.errors import (
    AuthNError,
    ErrClusterNotFound,
    ErrClusterAlreadyRegistered,
    ErrRoleNotFound,
    ErrRoleAlreadyExists,
    ErrUserNotFound,
    ErrUserAlreadyExists,
    ErrUserInvalidCredentials,
)


def parse_authn_error(resp: requests.Response) -> AuthNError:
    """
    Parse raw text into an appropriate AuthNError object.

    Args:
        resp (requests.Response): The response from the AuthN cluster.

    Raises:
        AuthNError: If the error doesn't match any specific conditions.
        ErrClusterNotFound: If the error message indicates a missing cluster.
        ErrRoleNotFound: If the error message indicates a missing role.
        ErrUserNotFound: If the error message indicates a missing user.
        ErrRoleAlreadyExists: If the error message indicates a role already exists.
        ErrUserAlreadyExists: If the error message indicates a user already exists.
        ErrClusterAlreadyRegistered: If the error message indicates the cluster is already registered.
        ErrUserInvalidCredentials: If the error message indicates invalid user credentials.
    """
    exc_class = AuthNError
    err = parse_http_error_or_raise(resp, exc_class)
    status, message = err.status, err.message

    if status == 401:
        if "invalid credentials" in message:
            exc_class = ErrUserInvalidCredentials
    if status == 404:
        if "cluster " in message:
            exc_class = ErrClusterNotFound
        if "role " in message:
            exc_class = ErrRoleNotFound
        if "user " in message:
            exc_class = ErrUserNotFound
    if status == 409:
        if "cluster " in message:
            exc_class = ErrClusterAlreadyRegistered
        if "role " in message:
            exc_class = ErrRoleAlreadyExists
        if "user " in message:
            exc_class = ErrUserAlreadyExists

    return exc_class(status, message)
