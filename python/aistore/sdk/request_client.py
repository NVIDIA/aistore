#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#
from urllib.parse import urljoin, urlencode
from typing import TypeVar, Type, Any, Dict, Optional, Tuple, Union

from requests import Response
from tenacity import Retrying

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
    URL_PATH_DAEMON,
    WHAT_SMAP,
    QPARAM_WHAT,
    HTTP_METHOD_GET,
)
from aistore.sdk.response_handler import ResponseHandler, AISResponseHandler
from aistore.sdk.session_manager import SessionManager
from aistore.version import __version__ as sdk_version
from aistore.sdk.types import Smap
from aistore.sdk.utils import decode_response, get_logger
from aistore.sdk.retry_config import RetryConfig

T = TypeVar("T")

logger = get_logger(__name__)


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
        response_handler (ResponseHandler): Handler for processing HTTP responses. Defaults to AISResponseHandler.
    """

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        endpoint: str,
        session_manager: SessionManager,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        token: str = None,
        response_handler: ResponseHandler = AISResponseHandler(),
        network_retry_config: Retrying = None,
    ):
        self._base_url = urljoin(endpoint, "v1")
        self._session_manager = session_manager
        self._token = token
        self._timeout = timeout
        self._response_handler = response_handler
        # smap is used to calculate the target node for a given object
        self._smap = None
        self._network_retry_config = (
            network_retry_config or RetryConfig.default().network_retry
        )

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

    @property
    def network_retry_config(self) -> Retrying:
        """
        Return the network retry configuration for this client.
        """
        return self._network_retry_config

    @token.setter
    def token(self, token: str):
        """
        Set the token for Authorization.

        Args:
            token (str): Token for authorization.
        """
        self._token = token

    def get_smap(self, force_update: bool = False) -> "Smap":
        """Return the smap."""
        if not self._smap or force_update:
            self._smap = self.request_deserialize(
                HTTP_METHOD_GET,
                path=URL_PATH_DAEMON,
                res_model=Smap,
                params={QPARAM_WHAT: WHAT_SMAP},
            )
        return self._smap

    def clone(self, base_url: Optional[str] = None) -> "RequestClient":
        """
        Create a copy of the current RequestClient instance with an optional new base URL.

        Args:
            base_url (Optional[str]): New base URL for the cloned client. Defaults to the existing base URL.

        Returns:
            RequestClient: A new instance with the same settings but an optional different base URL.
        """

        # Default to the existing base URL if none is provided
        base_url = base_url or self._base_url

        # Ensure the base URL ends with "/v1"
        base_url = base_url if base_url.endswith("/v1") else urljoin(base_url, "v1")

        return RequestClient(
            endpoint=base_url,
            session_manager=self._session_manager,
            timeout=self._timeout,
            token=self._token,
            response_handler=self._response_handler,
        )

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

    def _retryable_session_request(
        self, method: str, url: str, headers: dict, **kwargs
    ) -> Response:
        """
        Makes an HTTP request with automatic retries on connection-related errors.

        Retries the request if it encounters transient network issues such as:
        - ConnectTimeout
        - ReadTimeout
        - ConnectionError (refused connection, reset by peer)
        - AISRetryableError (custom error for AIS)
        - ChunkedEncodingError

        If the request is an HTTPS request with a payload (`data`), it uses `_request_with_manual_redirect`.
        Otherwise, it falls back to `_session_request`.

        **Why `tenacity.retry` over `urllib3.Retry` for this function?**
        `urllib3.Retry` always retries the **same failing URL**, which is problematic if a target is down or restarting.
        Instead, we retry via the **proxy URL** to reach another available target—something `urllib3.Retry` does not
        support.

        Args:
            method (str): HTTP method (e.g., GET, POST).
            url (str): Target URL.
            headers (dict): HTTP headers.
            kwargs: Additional request parameters.

        Returns:
            Response: The HTTP response from the server.
        """

        if url.startswith(HTTPS) and "data" in kwargs:
            response = self._request_with_manual_redirect(
                method, url, headers, **kwargs
            )
        else:
            response = self._session_request(method, url, headers, **kwargs)
        return self._response_handler.handle_response(response)

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
        # If the request is a GET to the daemon, we need to set the User-Agent header to avoid a 403 Forbidden
        return self.network_retry_config(
            self._retryable_session_request, method, url, headers, **kwargs
        )

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
        if resp.status_code in (STATUS_REDIRECT_PERM, STATUS_REDIRECT_TMP):
            target_url = resp.headers.get(HEADER_LOCATION)
            # Redirected request to target
            resp = self._session_request(method, target_url, headers, **kwargs)
        return resp

    def _session_request(self, method, url, headers, **kwargs) -> Response:
        request_kwargs = {"headers": headers, **kwargs}
        if self._timeout is not None and "timeout" not in request_kwargs:
            request_kwargs["timeout"] = self._timeout
        return self.session_manager.session.request(method, url, **request_kwargs)

    def get_full_url(self, path: str, params: Dict[str, Any]) -> str:
        """
        Get the full URL to the path on the cluster with the given parameters.

        Args:
            path (str): Path on the cluster.
            params (Dict[str, Any]): Query parameters to include.

        Returns:
            URL including cluster base URL and parameters.

        """
        return f"{self._base_url}/{path.lstrip('/')}?{urlencode(params)}"
