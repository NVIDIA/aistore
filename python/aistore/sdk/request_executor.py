#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
from typing import Any, Dict, Optional, Tuple, Union

from requests import Response

from aistore.sdk.const import (
    HEADER_AUTHORIZATION,
    HEADER_CONTENT_TYPE,
    HEADER_USER_AGENT,
    JSON_CONTENT_TYPE,
    USER_AGENT_BASE,
)
from aistore.sdk.session_manager import SessionManager
from aistore.version import __version__ as sdk_version


class RequestExecutor:
    """
    Executes HTTP requests without retry wrapping against a configured AIS endpoint over a
    provided `SessionManager`.

    Args:
        base_url (str): Fully-qualified endpoint URL including the API
            version prefix (e.g. `https://proxy/v1`).
        session_manager (SessionManager): Shared per-process session pool.
        token (str, optional): Bearer token added as the Authorization header
            when set.
        timeout (Union[float, Tuple[float, float], None], optional): Request timeout in seconds; a single float.
            for both connect/read timeouts (e.g., 5.0), a tuple for separate connect/read timeouts (e.g., (3.0, 10.0)),
            or None to disable timeout.
    """

    def __init__(
        self,
        base_url: str,
        session_manager: SessionManager,
        token: str = "",
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
    ):
        self._base_url = base_url
        self._session_manager = session_manager
        self._token = token
        self._timeout = timeout

    @property
    def base_url(self) -> str:
        """Configured endpoint base URL (already including `/v1`)."""
        return self._base_url

    @property
    def session_manager(self) -> SessionManager:
        """Shared `SessionManager` used to acquire the per-process session."""
        return self._session_manager

    @session_manager.setter
    def session_manager(self, value: SessionManager) -> None:
        self._session_manager = value

    @property
    def token(self) -> str:
        """Current bearer token."""
        return self._token

    @token.setter
    def token(self, value: str) -> None:
        self._token = value

    @property
    def timeout(self) -> Optional[Union[float, Tuple[float, float]]]:
        """Default request timeout applied when caller does not override."""
        return self._timeout

    @timeout.setter
    def timeout(self, value: Optional[Union[float, Tuple[float, float]]]) -> None:
        self._timeout = value

    def request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Response:
        """
        Send `method` to `<base_url>/<path>`.

        Args:
            method (str): HTTP method (GET, POST, ...).
            path (str): Path appended to `base_url` (leading slash optional).
            headers (Dict[str, Any], optional): Caller-supplied headers; the
                AIS default Content-Type, User-Agent and Authorization headers
                are merged on top and will override.
            **kwargs: Additional kwargs forwarded to `requests.Session.request`
                (`params`, `data`, `json`, `stream`, `timeout`, ...).
        """
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._send(method, url, headers, **kwargs)

    def request_absolute(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Response:
        """
        Send `method` to an absolute `url` (skip the `base_url` join).

        Used by callers that have already resolved the full URL (e.g. the
        manual HTTPS redirect path in `RequestClient`, or a one-shot endpoint
        override).
        """
        return self._send(method, url, headers, **kwargs)

    def _send(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, Any]],
        **kwargs,
    ) -> Response:
        merged = self._merge_headers(headers)
        if self._timeout is not None and "timeout" not in kwargs:
            kwargs["timeout"] = self._timeout
        return self._session_manager.session.request(
            method, url, headers=merged, **kwargs
        )

    def _merge_headers(self, headers: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        merged = dict(headers) if headers else {}
        merged[HEADER_CONTENT_TYPE] = JSON_CONTENT_TYPE
        merged[HEADER_USER_AGENT] = f"{USER_AGENT_BASE}/{sdk_version}"
        if self._token:
            merged[HEADER_AUTHORIZATION] = f"Bearer {self._token}"
        return merged
