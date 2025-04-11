#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
from typing import Type

import requests

from aistore.sdk.authn.errors import (
    AuthNError,
    ErrUserInvalidCredentials,
    ErrClusterNotFound,
    ErrRoleNotFound,
    ErrUserNotFound,
    ErrClusterAlreadyRegistered,
    ErrRoleAlreadyExists,
    ErrUserAlreadyExists,
)
from aistore.sdk.response_handler import ResponseHandler


class AuthNResponseHandler(ResponseHandler):
    """
    Handle responses from an AuthN server
    """

    @property
    def exc_class(self) -> Type[AuthNError]:
        return AuthNError

    def parse_error(self, r: requests.Response) -> AuthNError:
        """
        Parse raw text into an appropriate AuthNError object.

        Args:
            r (requests.Response): The response from the AuthN cluster.

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
        exc_class = self.exc_class
        status, message, req_url = r.status_code, r.text, r.request.url

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

        return exc_class(status, message, req_url or "")
