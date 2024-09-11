#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from aistore.sdk.request_client import RequestClient
from aistore.sdk.authn.types import TokenMsg
from aistore.sdk.utils import get_logger
from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    URL_PATH_AUTHN_TOKENS,
)

logger = get_logger(__name__)


class TokenManager:  # pylint: disable=duplicate-code
    """
    Manages token-related operations.

    This class provides methods to interact with tokens in the AuthN server.
    .

    Args:
        client (RequestClient): The RequestClient used to make HTTP requests.
    """

    def __init__(self, client: RequestClient):
        self._client = client

    @property
    def client(self) -> RequestClient:
        """Returns the RequestClient instance used by this TokenManager."""
        return self._client

    def revoke(self, token: str) -> None:
        """
        Revokes the specified authentication token.

        Args:
            token (str): The token to be revoked.

        Raises:
            ValueError: If the token is not provided.
            AISError: If the revoke token request fails.
        """
        if not token:
            raise ValueError("Token must be provided to revoke.")

        logger.info("Revoking token: %s", token)

        self.client.request(
            method=HTTP_METHOD_DELETE,
            path=f"{URL_PATH_AUTHN_TOKENS}",
            json=TokenMsg(token=token).dict(),
        )
