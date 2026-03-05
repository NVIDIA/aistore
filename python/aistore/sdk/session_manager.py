"""
Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
"""

import os
import warnings
from typing import Optional, Tuple, Union
from multiprocessing import current_process

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import urllib3

from aistore.sdk.const import (
    AIS_CLIENT_CA,
    AIS_CLIENT_KEY,
    AIS_CLIENT_CRT,
    AIS_SKIP_VERIFY,
    HTTPS,
    HTTP,
)


def resolve_ssl_config(
    skip_verify: bool = False,
    ca_cert: Optional[str] = None,
    client_cert: Optional[Union[str, Tuple[str, str]]] = None,
) -> tuple:
    """
    Set session verify value for validating the server's SSL certificate
    The requests library allows this to be a boolean or a string path to the cert
    If we do not skip verification, the order is:
      1. Provided cert path
      2. Cert path from env var.
      3. True (verify with system's approved CA list)

    Returns:
        (verify, cert) where:
          - verify: False to skip verification, a CA cert path string, or True for default system CA
          - cert: None if no client cert, a path string, or a (cert_path, key_path) tuple for mTLS
    """
    if not skip_verify:
        skip_verify = os.environ.get(AIS_SKIP_VERIFY, "").lower() in (
            "1",
            "true",
            "yes",
        )
    if not client_cert:
        cert = os.getenv(AIS_CLIENT_CRT)
        key = os.getenv(AIS_CLIENT_KEY)
        client_cert = (cert, key) if cert and key else None
    if skip_verify:
        return False, client_cert
    if ca_cert:
        return ca_cert, client_cert
    env_crt = os.getenv(AIS_CLIENT_CA)
    return env_crt if env_crt else True, client_cert


class SessionManager:
    """
    Class for storing and creating requests library sessions.

    Args:
        retry (urllib3.Retry, optional): Defines the HTTP retry strategy using `urllib3.Retry`.
            Defaults to `RetryConfig.default().http_retry`, which handles transient HTTP failures.
        skip_verify (bool, optional): If True, skip SSL certificate verification. Defaults to False.
        ca_cert (str, optional): Path to a CA certificate file for SSL verification. Defaults to None.
        client_cert (Union[str, Tuple[str, str], None], optional): Path to a client certificate PEM file
            or a path pair (cert, key) for mTLS. If not provided, 'AIS_CRT' and 'AIS_CRT_KEY' environment
            variables will be used. Defaults to None.
        max_pool_size (int, optional): Maximum number of connections per host in the connection pool.
            Defaults to 10.
    """

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        retry: Optional[Retry] = None,
        ca_cert: Optional[str] = None,
        skip_verify: bool = False,
        client_cert: Optional[Union[str, Tuple[str, str]]] = None,
        max_pool_size: int = 10,
    ):
        self._retry = retry
        self._ca_cert = ca_cert
        if not skip_verify:
            skip_verify = os.environ.get(AIS_SKIP_VERIFY, "").lower() in (
                "1",
                "true",
                "yes",
            )
        self._skip_verify = skip_verify
        if self._skip_verify:
            warnings.warn(
                "Skipping SSL certificate verification is insecure. "
                "Use a valid SSL certificate instead.",
                UserWarning,
            )
        if not client_cert:
            cert = os.getenv(AIS_CLIENT_CRT)
            key = os.getenv(AIS_CLIENT_KEY)
            client_cert = (cert, key) if cert and key else None
        self._client_cert = client_cert
        self._max_pool_size = max_pool_size

        # Suppress urllib3 SSL warnings when certificate verification is skipped
        if self._skip_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self._session_pool = {current_process().pid: self._create_session()}

    @property
    def retry(self) -> Retry:
        """Returns retry config for this session."""
        return self._retry

    @property
    def ca_cert(self) -> Optional[str]:
        """Returns CA certificate for this session, if any."""
        return self._ca_cert

    @property
    def client_cert(self) -> Optional[Union[str, Tuple[str, str]]]:
        """Returns client certificate for this session, if any."""
        return self._client_cert

    @property
    def skip_verify(self) -> bool:
        """Returns whether this session's requests skip server certificate verification."""
        return self._skip_verify

    @property
    def session(self) -> Session:
        """Acquires an existing `requests` session, creating a new one if needed."""
        pid = current_process().pid

        if pid not in self._session_pool:
            self._session_pool[pid] = self._create_session()

        return self._session_pool[pid]

    def _create_session(self) -> Session:
        """
        Creates a new `requests` session for HTTP requests.

        Returns:
            New HTTP request Session
        """
        request_session = Session()
        request_session.verify, request_session.cert = resolve_ssl_config(
            self._skip_verify, self._ca_cert, self._client_cert
        )

        adapter = HTTPAdapter(
            max_retries=self._retry,
            pool_connections=self._max_pool_size,
            pool_maxsize=self._max_pool_size,
        )

        for protocol in (HTTP, HTTPS):
            request_session.mount(protocol, adapter)
        return request_session
