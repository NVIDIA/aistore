#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import os
import logging
from abc import ABC, abstractmethod
import sys


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
        - `arg_type`: Determines how to interpret the input path ("fqn", "url", or "").
        - `logger`: A class-specific logger configured to output to stdout.

        Subclasses can extend this method to initialize any transformation-specific
        resources (e.g., preloaded models, hash functions, lookup tables) required
        by the `transform()` method.

        Note:
            If you override `__init__` in a subclass, make sure to call
            `super().__init__()` to ensure proper base class initialization.
        """
        self.host_target = os.getenv("AIS_TARGET_URL")
        if not self.host_target:
            raise EnvironmentError("Environment variable 'AIS_TARGET_URL' must be set.")
        self.arg_type = os.getenv("ARG_TYPE", "").lower()  # "", "fqn" or "url"
        self.direct_put = os.getenv("DIRECT_PUT", "false").lower() == "true"

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    @abstractmethod
    def transform(self, data: bytes, path: str, etl_args: str) -> bytes:
        """
        Transform the data received from a request.

        Args:
            data (bytes): The original content fetched from AIS.
            path (str): The request path. This is usually the path
                to the object.
            etl_args (str): The arguments passed for the transformation.

        Returns:
            bytes: Transformed data to return to the caller.
        """

    @abstractmethod
    def start(self):
        """
        Start the ETL server (blocking call).
        Typically binds and listens on a port.
        """

    def get_mime_type(self) -> str:
        """
        Optional override to specify MIME type of transformed response.

        Args:
            path (str): Request path or object path.

        Returns:
            str: MIME type (e.g., "application/json", "text/plain").
        """
        return "application/octet-stream"
