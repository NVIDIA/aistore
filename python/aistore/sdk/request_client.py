#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#
from urllib.parse import urljoin, urlencode
from typing import Callable, TypeVar, Type, Any, Dict, Optional, Tuple, Union
from requests import Response

from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
    HEADER_AUTHORIZATION,
    HTTPS,
    HEADER_LOCATION,
    STATUS_REDIRECT_PERM,
    STATUS_REDIRECT_TMP,
)
from aistore.sdk.session_manager import SessionManager
from aistore.sdk.utils import raise_ais_error, handle_errors, decode_response
from aistore.version import __version__ as sdk_version

T = TypeVar("T")


class RequestClient:
    """
    Internal client for making requests to an AIS cluster or a specific proxy (e.g. AuthN).

    Args:
        endpoint (str): Endpoint to either the AIS cluster or a specific proxy (e.g. AuthN).
        session_manager (SessionManager): SessionManager for creating and accessing requests session.
        timeout (Union[float, Tuple[float, float], None], optional): Request timeout in seconds; a single float.
            for both connect/read timeouts (e.g., 5.0), a tuple for separate connect/read timeouts (e.g., (3.0, 10.0)),
            or None to disable timeout.
        token (str, optional): Authorization token.
        error_handler (Callable[[str], None], optional): Error handler for managing response errors. Defaults to
            raise_ais_error.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        endpoint: str,
        session_manager: SessionManager,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        token: str = None,
        error_handler: Callable[[str], None] = raise_ais_error,
    ):
        self._base_url = urljoin(endpoint, "v1")
        self._session_manager = session_manager
        self._token = token
        self._timeout = timeout
        self._error_handler = error_handler

    @property
    def base_url(self):
        """Return the base URL."""
        return self._base_url

    @property
    def timeout(self):
        """Return the timeout for requests."""
        return self._timeout

    @timeout.setter
    def timeout(self, timeout: Union[float, Tuple[float, float]]):
        """
        Set the timeout for all requests from this client.

        Args:
            timeout: Timeout for requests.
        """
        self._timeout = timeout

    @property
    def session_manager(self) -> SessionManager:
        """
        Return the SessionManager used to create sessions for this client.
        """
        return self._session_manager

    @session_manager.setter
    def session_manager(self, session_manager):
        self._session_manager = session_manager

    @property
    def token(self) -> str:
        """
        Return the token for authorization.
        """
        return self._token

    @token.setter
    def token(self, token: str):
        """
        Set the token for Authorization.

        Args:
            token (str): Token for authorization.
        """
        self._token = token

    def request_deserialize(
        self, method: str, path: str, res_model: Type[T], **kwargs
    ) -> T:
        """
        Make a request and deserialize the response to a defined type.

        Args:
            method (str): HTTP method (e.g. POST, GET, PUT, DELETE).
            path (str): URL path to call.
            res_model (Type[T]): Resulting type to which the response should be deserialized.
            **kwargs (optional): Optional keyword arguments to pass with the call to request.

        Returns:
            Parsed result of the call to the API, as res_model.
        """
        resp = self.request(method, path, **kwargs)
        return decode_response(res_model, resp)

    def request(
        self,
        method: str,
        path: str,
        endpoint: str = None,
        headers: Dict = None,
        **kwargs,
    ) -> Response:
        """
        Make a request.

        Args:
            method (str): HTTP method (e.g. POST, GET, PUT, DELETE).
            path (str): URL path to call.
            endpoint (str): Alternative endpoint for the AIS cluster (e.g. for connecting to a specific proxy).
            headers (dict): Extra headers to be passed with the request. Content-Type and User-Agent will be overridden.
            **kwargs (optional): Optional keyword arguments to pass with the call to request.

        Returns:
            Raw response from the API.
        """
        base = urljoin(endpoint, "v1") if endpoint else self._base_url
        url = f"{base}/{path.lstrip('/')}"
        if headers is None:
            headers = {}
        headers[HEADER_CONTENT_TYPE] = JSON_CONTENT_TYPE
        headers[HEADER_USER_AGENT] = f"{USER_AGENT_BASE}/{sdk_version}"
        if self.token:
            headers[HEADER_AUTHORIZATION] = f"Bearer {self.token}"

        if url.startswith(HTTPS) and "data" in kwargs:
            resp = self._request_with_manual_redirect(method, url, headers, **kwargs)
        else:
            resp = self._session_request(method, url, headers, **kwargs)

        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp, self._error_handler)

        return resp

    def _request_with_manual_redirect(
        self, method: str, url: str, headers, **kwargs
    ) -> Response:
        """
        Make a request to the proxy, close the session, and use a new session to make a request to the redirected
        target.

        This exists because the current implementation of `requests` does not seem to handle a 307 redirect
        properly from a server with TLS enabled with data in the request, and will error with the following on the
        initial connection to the proxy:
            SSLEOFError(8, 'EOF occurred in violation of protocol (_ssl.c:2406)')
        Instead, this implementation will not send the data to the proxy, and only use it to access the proper target.

        Args:
            method (str): HTTP method (e.g. POST, GET, PUT, DELETE).
            url (str): Initial AIS url.
            headers (dict): Extra headers to be passed with the request. Content-Type and User-Agent will be overridden.
            **kwargs (optional): Optional keyword arguments to pass with the call to request.

        Returns:
            Final response from AIS target

        """
        # Do not include data payload in the initial request to the proxy
        proxy_request_kwargs = {
            "headers": headers,
            "allow_redirects": False,
            **{k: v for k, v in kwargs.items() if k != "data"},
        }

        # Request to proxy, which should redirect
        resp = self.session_manager.session.request(method, url, **proxy_request_kwargs)
        self.session_manager.session.close()
        if resp.status_code in (STATUS_REDIRECT_PERM, STATUS_REDIRECT_TMP):
            target_url = resp.headers.get(HEADER_LOCATION)
            # Redirected request to target
            resp = self._session_request(method, target_url, headers, **kwargs)
        return resp

    def _session_request(self, method, url, headers, **kwargs) -> Response:
        request_kwargs = {"headers": headers, **kwargs}
        if self._timeout is not None:
            request_kwargs["timeout"] = self._timeout

        return self.session_manager.session.request(method, url, **request_kwargs)

    def get_full_url(self, path: str, params: Dict[str, Any]) -> str:
        """
        Get the full URL to the path on the cluster with the given parameters.

        Args:
            path: Path on the cluster.
            params: Query parameters to include.

        Returns:
            URL including cluster base URL and parameters.

        """
        return f"{self._base_url}/{path.lstrip('/')}?{urlencode(params)}"
