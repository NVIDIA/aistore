#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import os
import sys
import logging
import resource
from abc import ABC, abstractmethod
from typing import BinaryIO, Iterator, Tuple, Union
from urllib.parse import unquote
import requests
from aistore.sdk.const import (
    STATUS_NO_CONTENT,
    STATUS_OK,
    HEADER_AUTHORIZATION,
    HEADER_DIRECT_PUT_LENGTH,
    AIS_AUTHN_TOKEN,
)
from aistore.sdk.session_manager import SessionManager


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
        - `session`: A `requests.Session` pre-configured with SSL/auth settings derived
          from environment variables (see below).

        SSL / auth environment variables:
        - `AIS_SKIP_VERIFY`: Set to `"true"`, `"1"`, or `"yes"` to disable SSL
          certificate verification when the server contacts AIS targets.
        - `AIS_CLIENT_CA`: Path to a CA certificate bundle used for SSL verification.
        - `AIS_CRT`: Path to a client certificate PEM file (used for mTLS).
        - `AIS_CRT_KEY`: Path to the client certificate private key (used for mTLS).
        - `AIS_AUTHN_TOKEN`: Bearer token sent in every outbound request header.

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

        self.token = os.environ.get(AIS_AUTHN_TOKEN)
        self.session = self._build_session()

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        # Detect whether the subclass overrides transform_stream (streaming)
        # or transform (buffered). If both are overridden, transform wins.
        has_transform = type(self).transform is not ETLServer.transform
        has_stream = type(self).transform_stream is not ETLServer.transform_stream
        if not has_transform and not has_stream:
            raise TypeError(
                f"{type(self).__name__} must override either transform() or transform_stream()"
            )
        self.use_streaming = has_stream and not has_transform

    def _build_session(self) -> requests.Session:
        """Build a requests.Session by delegating SSL/cert config to SessionManager."""
        session = SessionManager().session
        if self.token:
            session.headers.update({HEADER_AUTHORIZATION: f"Bearer {self.token}"})
        return session

    def transform(self, data: Union[bytes, str], path: str, etl_args: str) -> bytes:
        """
        Transform the data received from a request (buffered mode).

        Override this OR `transform_stream` in your subclass. If both are
        overridden, `transform` takes priority (backward compatibility).

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
        raise NotImplementedError(
            "Subclass must override either transform() or transform_stream()"
        )

    def transform_stream(
        self, reader: BinaryIO, path: str, etl_args: str
    ) -> Iterator[bytes]:
        """
        Transform data in streaming mode — constant memory usage.

        Override this OR `transform` in your subclass. If both are
        overridden, `transform` takes priority (backward compatibility).

        The method receives a file-like input (`reader`) and yields output
        chunks as they become available. This avoids buffering the entire
        object in memory.

        Args:
            reader (BinaryIO): File-like object for reading input data.
            path (str): The object path (e.g. `"bucket/object-name"`).
            etl_args (str): Optional per-request arguments.

        Yields:
            bytes: Chunks of transformed data.
        """
        raise NotImplementedError(
            "Subclass must override either transform() or transform_stream()"
        )

    @staticmethod
    def close_reader(reader):
        """Close a BinaryIO reader if it has a close method."""
        if hasattr(reader, "close"):
            try:
                reader.close()
            except Exception:  # pylint: disable=broad-except
                pass

    @staticmethod
    def iter_and_close(output_iter: Iterator[bytes], reader) -> Iterator[bytes]:
        """Yield from output_iter and close reader when done."""
        try:
            yield from output_iter
        finally:
            ETLServer.close_reader(reader)

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
        """Send a PUT request using the pre-configured session."""
        return self.session.put(url, data=data, timeout=timeout, headers=headers)

    def handle_direct_put_response(
        self, resp: requests.Response, data: bytes, data_length: int = -1
    ) -> Tuple[int, bytes, int]:
        """Handle the response from a direct PUT request.

        Args:
            resp: The HTTP response from the direct PUT.
            data: The original data bytes (used to compute length for the
                200-OK-empty-content case). Can be `b""` for streaming.
            data_length: Explicit byte count override. When >= 0, used instead
                of `len(data)`. Pass this from a `CountingIterator` for
                streaming pipeline PUTs where `data` is empty.
        """
        size = data_length if data_length >= 0 else len(data)

        if resp.status_code == STATUS_NO_CONTENT:
            return (
                resp.status_code,
                b"",
                int(resp.headers.get(HEADER_DIRECT_PUT_LENGTH, "0")),
            )

        if resp.status_code == STATUS_OK:
            if resp.content:  # from other ETL server, forward the content back
                return resp.status_code, resp.content, 0

            return STATUS_NO_CONTENT, b"", size  # from target, no content

        error = resp.content
        self.logger.error(
            "Direct put failed to %s: HTTP %s - %s",
            resp.url,
            resp.status_code,
            error,
        )
        return resp.status_code, error, 0


class CountingIterator:  # pylint: disable=too-few-public-methods
    """Wraps an iterator of `bytes` chunks to count total bytes yielded."""

    def __init__(self, inner: Iterator[bytes]):
        self._inner = inner
        self.bytes_sent = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self._inner:
            self.bytes_sent += len(chunk)
            yield chunk
