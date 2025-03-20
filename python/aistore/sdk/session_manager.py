"""
Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

import os
from typing import Optional, Tuple, Union
from multiprocessing import current_process

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from aistore.sdk.const import AIS_CLIENT_CA, AIS_CLIENT_KEY, AIS_CLIENT_CRT, HTTPS, HTTP
from aistore.sdk.retry_config import RetryConfig


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
        retry: Retry = None,
        ca_cert: Optional[str] = None,
        skip_verify: bool = False,
        client_cert: Optional[Union[str, Tuple[str, str]]] = None,
        max_pool_size: int = 10,
    ):
        self._retry = retry or RetryConfig.default().http_retry
        self._ca_cert = ca_cert
        self._skip_verify = skip_verify
        if not client_cert:
            cert = os.getenv(AIS_CLIENT_CRT)
            key = os.getenv(AIS_CLIENT_KEY)
            client_cert = (cert, key) if cert and key else None
        self._client_cert = client_cert
        self._max_pool_size = max_pool_size
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

    def _set_session_verification(self, request_session: Session):
        """
        Set session verify value for validating the server's SSL certificate
        The requests library allows this to be a boolean or a string path to the cert
        If we do not skip verification, the order is:
          1. Provided cert path
          2. Cert path from env var.
          3. True (verify with system's approved CA list)
        """
        if self._skip_verify:
            request_session.verify = False
            return
        if self._ca_cert:
            request_session.verify = self._ca_cert
            return
        env_crt = os.getenv(AIS_CLIENT_CA)
        request_session.verify = env_crt if env_crt else True

    def _create_session(self) -> Session:
        """
        Creates a new `requests` session for HTTP requests.

        Returns:
            New HTTP request Session
        """
        request_session = Session()
        request_session.cert = self._client_cert
        self._set_session_verification(request_session)

        adapter = HTTPAdapter(
            max_retries=self._retry,
            pool_connections=self._max_pool_size,
            pool_maxsize=self._max_pool_size,
        )

        for protocol in (HTTP, HTTPS):
            request_session.mount(protocol, adapter)
        return request_session
