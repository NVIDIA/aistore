#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Tuple, Union
from urllib3 import Retry
from aistore.sdk.request_client import RequestClient
from aistore.sdk.session_manager import SessionManager
from aistore.sdk.utils import get_logger
from aistore.sdk.authn.types import TokenMsg, LoginMsg
from aistore.sdk.authn.cluster_manager import ClusterManager
from aistore.sdk.authn.role_manager import RoleManager
from aistore.sdk.authn.token_manager import TokenManager
from aistore.sdk.authn.user_manager import UserManager
from aistore.sdk.authn.utils import raise_authn_error
from aistore.sdk.const import (
    HTTP_METHOD_POST,
    URL_PATH_AUTHN_USERS,
)

logger = get_logger(__name__)


class AuthNClient:
    """
    AuthN client for managing authentication.

    This client provides methods to interact with AuthN Server.
    For more info on AuthN Server, see https://github.com/NVIDIA/aistore/blob/main/docs/authn.md

    Args:
        endpoint (str): AuthN service endpoint URL.
        skip_verify (bool, optional): If True, skip SSL certificate verification. Defaults to False.
        ca_cert (str, optional): Path to a CA certificate file for SSL verification.
        timeout (Union[float, Tuple[float, float], None], optional): Request timeout in seconds; a single float
            for both connect/read timeouts (e.g., 5.0), a tuple for separate connect/read timeouts (e.g., (3.0, 10.0)),
            or None to disable timeout.
        retry (urllib3.Retry, optional): Retry configuration object from the urllib3 library.
        token (str, optional): Authorization token.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        endpoint: str,
        skip_verify: bool = False,
        ca_cert: Optional[str] = None,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        retry: Optional[Retry] = None,
        token: Optional[str] = None,
    ):
        logger.info("Initializing AuthNClient")
        session_manager = SessionManager(
            retry=retry, ca_cert=ca_cert, skip_verify=skip_verify
        )
        self._request_client = RequestClient(
            endpoint=endpoint,
            session_manager=session_manager,
            timeout=timeout,
            token=token,
            error_handler=raise_authn_error,
        )
        logger.info("AuthNClient initialized with endpoint: %s", endpoint)

    @property
    def client(self) -> RequestClient:
        """
        Get the request client.

        Returns:
            RequestClient: The client this AuthN client uses to make requests.
        """
        return self._request_client

    def login(
        self,
        username: str,
        password: str,
        expires_in: Optional[Union[int, float]] = None,
    ) -> str:
        """
        Logs in to the AuthN Server and returns an authorization token.

        Args:
            username (str): The username to log in with.
            password (str): The password to log in with.
            expires_in (Optional[Union[int, float]]): The expiration duration of the token in seconds.

        Returns:
            str: An authorization token to use for future requests.

        Raises:
            ValueError: If the password is empty or consists only of spaces.
            AISError: If the login request fails.
        """
        if password.strip() == "":
            raise ValueError("Password cannot be empty or spaces only")

        logger.info("Attempting to log in with username: %s", username)
        login_msg = LoginMsg(password=password, expires_in=expires_in).as_dict()

        try:
            token = self.client.request_deserialize(
                HTTP_METHOD_POST,
                path=f"{URL_PATH_AUTHN_USERS}/{username}",
                json=login_msg,
                res_model=TokenMsg,
            ).token
            logger.info("Login successful for username: %s", username)
            # Update the client token
            self.client.token = token
            return token
        except Exception as err:
            logger.error("Login failed for username: %s, error: %s", username, err)
            raise

    def logout(self) -> None:
        """
        Logs out and revokes current token from the AuthN Server.

        Raises:
            AISError: If the logout request fails.
        """
        if not self.client.token:
            raise ValueError("Must be logged in first (no token)")

        try:
            logger.info("Logging out")
            self.token_manager().revoke(token=self.client.token)
            self.client.token = None
        except Exception as err:
            logger.error("Logout failed, error: %s", err)
            raise

    def cluster_manager(self) -> ClusterManager:
        """
        Factory method to create a ClusterManager instance.

        Returns:
            ClusterManager: An instance to manage cluster operations.
        """
        return ClusterManager(client=self._request_client)

    def role_manager(self) -> RoleManager:
        """
        Factory method to create a RoleManager instance.

        Returns:
            RoleManager: An instance to manage role operations.
        """
        return RoleManager(client=self._request_client)

    def user_manager(self) -> UserManager:
        """
        Factory method to create a UserManager instance.

        Returns:
            UserManager: An instance to manage user operations.
        """
        return UserManager(client=self._request_client)

    def token_manager(self) -> TokenManager:
        """
        Factory method to create a TokenManager instance.

        Returns:
            TokenManager: An instance to manage token operations.
        """
        return TokenManager(client=self._request_client)
