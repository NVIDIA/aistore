#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import os
import sys
import logging
import resource
from abc import ABC, abstractmethod
from typing import Tuple, Union
from urllib.parse import unquote
import requests
from aistore.sdk.const import STATUS_NO_CONTENT, STATUS_OK, HEADER_DIRECT_PUT_LENGTH


class ETLServer(ABC):
    """
    Abstract base class for all ETL servers.

    Provides:
    - `host_target`: the AIS target URL from environment variable `AIS_TARGET_URL`.
    - `logger`: a class-specific logger with INFO level (can be adjusted).
    """

    def __init__(self):
        """
        Initialize the base ETL server.

        Sets up:
        - `host_target`: AIS target URL from the `AIS_TARGET_URL` environment variable.
        - `logger`: A class-specific logger configured to output to stdout.

        Subclasses can extend this method to initialize any transformation-specific
        resources (e.g., preloaded models, hash functions, lookup tables) required
        by the `transform()` method.

        Note:
            If you override `__init__` in a subclass, make sure to call
            `super().__init__()` to ensure proper base class initialization.
        """
        # Raise the open file descriptor soft limit to match the hard limit.
        # CRI-O defaults the soft limit to 1024 which is too low for
        # high-concurrency ETL workloads.
        _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

        self.host_target = os.getenv("AIS_TARGET_URL")
        if not self.host_target:
            raise EnvironmentError("Environment variable 'AIS_TARGET_URL' must be set.")
        self.direct_put = os.getenv("DIRECT_PUT", "false").lower() == "true"
        self.direct_fqn = os.getenv("ETL_DIRECT_FQN", "false").lower() == "true"

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    @abstractmethod
    def transform(self, data: Union[bytes, str], path: str, etl_args: str) -> bytes:
        """
        Transform the data received from a request.

        Args:
            data (Union[bytes, str]): Object bytes by default. When
                `ETL_DIRECT_FQN=true`, the first pipeline stage receives a `str`
                filepath instead; intermediate stages always receive `bytes`.
                See `Etl.init_class(direct_file_access=...)` for full details.

            path (str): The object path (e.g. `"bucket/object-name"`).
            etl_args (str): Optional per-request arguments.

        Returns:
            bytes: Transformed data to return to the caller.
        """

    def sanitize_fqn(self, fqn: str) -> str:
        """Normalize an FQN to a safe absolute path."""
        return os.path.normpath(os.path.join("/", unquote(fqn).lstrip("/")))

    @abstractmethod
    def start(self):
        """
        Start the ETL server (blocking call).
        Typically binds and listens on a port.
        """

    def get_mime_type(self) -> str:
        """
        Optional override to specify MIME type of transformed response.

        Returns:
            str: MIME type (e.g., "application/json", "text/plain").
        """
        return "application/octet-stream"

    def client_put(
        self, url: str, data: bytes, headers: dict, timeout: int = None
    ) -> requests.Response:
        """Simple wrapper for requests.put()."""
        return requests.put(url, data, timeout=timeout, headers=headers)

    def handle_direct_put_response(
        self, resp: requests.Response, data: bytes
    ) -> Tuple[int, bytes, int]:
        """Handle the response from a direct PUT request."""
        if resp.status_code == STATUS_NO_CONTENT:
            return (
                resp.status_code,
                b"",
                int(resp.headers.get(HEADER_DIRECT_PUT_LENGTH, "0")),
            )

        if resp.status_code == STATUS_OK:
            if resp.content:  # from other ETL server, forward the content back
                return resp.status_code, resp.content, 0

            return STATUS_NO_CONTENT, b"", len(data)  # from target, no content

        error = resp.content
        self.logger.error(
            "Direct put failed to %s: HTTP %s - %s",
            resp.url,
            resp.status_code,
            error,
        )
        return resp.status_code, error, 0
