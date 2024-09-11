#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=too-many-arguments, duplicate-code


from typing import List, Optional
from aistore.sdk.authn.types import UserInfo, RolesList, UsersList
from aistore.sdk.authn.errors import ErrUserNotFound
from aistore.sdk.request_client import RequestClient
from aistore.sdk.authn.role_manager import RoleManager
from aistore.sdk.utils import get_logger
from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    URL_PATH_AUTHN_USERS,
)

logger = get_logger(__name__)


class UserManager:
    """
    UserManager provides methods to manage users in the AuthN service.

    Args:
        client (RequestClient): The RequestClient used to make HTTP requests.
    """

    def __init__(self, client: RequestClient):
        self._client = client
        self._role_manager = RoleManager(client)

    @property
    def client(self) -> RequestClient:
        """Returns the RequestClient instance used by this UserManager."""
        return self._client

    def get(self, username: str) -> UserInfo:
        """
        Retrieve user information from the AuthN Server.

        Args:
            username (str): The username to retrieve.

        Returns:
            UserInfo: The user's information.

        Raises:
            AISError: If the user retrieval request fails.
        """
        logger.info("Getting user with ID: %s", username)
        response = self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_USERS}/{username}",
            res_model=UserInfo,
        )

        return response

    def delete(self, username: str, missing_ok: bool = False) -> None:
        """
        Delete an existing user from the AuthN Server.

        Args:
            username (str): The username of the user to delete.
            missing_ok (bool):  Ignore error if user does not exist. Defaults to False.

        Raises:
            AISError: If the user deletion request fails.
        """
        logger.info("Deleting user with ID: %s", username)
        try:
            self._client.request(
                HTTP_METHOD_DELETE,
                path=f"{URL_PATH_AUTHN_USERS}/{username}",
            )
        except ErrUserNotFound as err:
            if not missing_ok:
                raise err

    def create(
        self,
        username: str,
        roles: List[str],
        password: str,
    ) -> UserInfo:
        """
        Create a new user in the AuthN Server.

        Args:
            username (str): The name or ID of the user to create.
            password (str): The password for the user.
            roles (List[str]): The list of names of roles to assign to the user.

        Returns:
            UserInfo: The created user's information.

        Raises:
            AISError: If the user creation request fails.
        """
        logger.info("Creating user with ID: %s", username)
        roles = self._get_roles_from_names(roles)
        user_info = UserInfo(id=username, password=password, roles=roles)
        self._client.request(
            HTTP_METHOD_POST,
            path=URL_PATH_AUTHN_USERS,
            json=user_info.dict(),
        )

        return self.get(username)

    def list(self):
        """
        List all users in the AuthN Server.

        Returns:
            str: The list of users in the AuthN Server.

        Raises:
            AISError: If the user list request fails.
        """
        logger.info("Listing all users")
        response = self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_AUTHN_USERS,
            res_model=UsersList,
        )

        return response

    def update(
        self,
        username: str,
        password: Optional[str] = None,
        roles: Optional[List[str]] = None,
    ) -> UserInfo:
        """
        Update an existing user's information in the AuthN Server.

        Args:
            username (str): The ID of the user to update.
            password (str, optional): The new password for the user.
            roles (List[str], optional): The list of names of roles to assign to the user.

        Returns:
            UserInfo: The updated user's information.

        Raises:
            AISError: If the user update request fails.
        """
        if not (password or roles):
            raise ValueError("You must change either the password or roles for a user.")

        roles = self._get_roles_from_names(roles) if roles else []

        logger.info("Updating user with ID: %s", username)
        user_info = UserInfo(id=username, password=password, roles=roles)
        self._client.request(
            HTTP_METHOD_PUT,
            path=f"{URL_PATH_AUTHN_USERS}/{username}",
            json=user_info.dict(),
        )

        return self.get(username)

    def _get_roles_from_names(self, role_names: List[str]) -> RolesList:
        """
        Helper function to convert a list of role names into a RolesList.

        Args:
            role_names (List[str]): List of role names to convert.

        Returns:
            RolesList: The corresponding RolesList object.

        Raises:
            ValueError: If any role name is not found.
        """
        roles = []
        for name in role_names:
            role = self._role_manager.get(name)
            roles.append(role)
        return roles
