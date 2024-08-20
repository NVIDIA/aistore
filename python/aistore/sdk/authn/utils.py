#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

import pydantic.tools

from aistore.sdk.utils import HttpError
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


def raise_authn_error(text: str):
    """
    Raises an AuthN-specific error based on the API response text.

    Args:
        text (str): The raw text of the API response containing error details.

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
    err = pydantic.tools.parse_raw_as(HttpError, text)
    if 400 <= err.status <= 500:
        if "does not exist" in err.message:
            if "cluster" in err.message:
                raise ErrClusterNotFound(err.status, err.message)
            if "role" in err.message:
                raise ErrRoleNotFound(err.status, err.message)
            if "user" in err.message:
                raise ErrUserNotFound(err.status, err.message)
        elif "already exists" in err.message:
            if "role" in err.message:
                raise ErrRoleAlreadyExists(err.status, err.message)
            if "user" in err.message:
                raise ErrUserAlreadyExists(err.status, err.message)
        elif "already registered" in err.message:
            raise ErrClusterAlreadyRegistered(err.status, err.message)
        elif "invalid credentials" in err.message:
            raise ErrUserInvalidCredentials(err.status, err.message)
    raise AuthNError(err.status, err.message)
