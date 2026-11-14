#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
from typing import Optional

from aistore.sdk.errors import ClientClosedError


class ClientState:
    """Store shared lifecycle state for Client and its RequestClient instances."""

    def __init__(self):
        self._closed = False
        self.ais_version: Optional[str] = None

    @property
    def closed(self) -> bool:
        """Return whether the client is closed."""
        return self._closed

    def ensure_open(self) -> None:
        """Raise ClientClosedError if the client is closed."""
        if self._closed:
            raise ClientClosedError("Client is closed, please create a new client.")

    def mark_closed(self) -> None:
        """Mark the client closed."""
        self._closed = True
