#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#


class AuthNError(Exception):
    """
    Raised when an error occurs during a query to the AuthN cluster.
    """

    def __init__(self, status_code: int, message: str):
        super().__init__(f"STATUS:{status_code}, MESSAGE:{message}")
        self.status_code = status_code
        self.message = message


# pylint: disable=unused-variable
class ErrRoleNotFound(AuthNError):
    """
    Raised when a role is expected but not found.
    """


# pylint: disable=unused-variable
class ErrRoleAlreadyExists(AuthNError):
    """
    Raised when a role is created but already exists.
    """


# pylint: disable=unused-variable
class ErrUserNotFound(AuthNError):
    """
    Raised when a user is expected but not found.
    """


# pylint: disable=unused-variable
class ErrUserAlreadyExists(AuthNError):
    """
    Raised when a user is created but already exists.
    """


# pylint: disable=unused-variable
class ErrClusterNotFound(AuthNError):
    """
    Raised when a cluster is expected but not found.
    """


# pylint: disable=unused-variable
class ErrClusterAlreadyRegistered(AuthNError):
    """
    Raised when a cluster is already registered.
    """


# pylint: disable=unused-variable
class ErrUserInvalidCredentials(AuthNError):
    """
    Raised when invalid credentials for a user are provided.
    """
