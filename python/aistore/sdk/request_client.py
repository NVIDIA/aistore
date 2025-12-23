#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#
import os
from urllib.parse import urljoin, urlencode
from typing import TypeVar, Type, Any, Dict, Optional, Tuple, Union

from requests import Response

from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
    HEADER_AUTHORIZATION,
    HEADER_CONNECTION,
    HTTPS,
    HEADER_LOCATION,
    STATUS_REDIRECT_PERM,
    STATUS_REDIRECT_TMP,
    URL_PATH_DAEMON,
    WHAT_SMAP,
    QPARAM_WHAT,
    HTTP_METHOD_GET,
    HEADER_CONTENT_LENGTH,
)

from aistore.sdk.response_handler import ResponseHandler, AISResponseHandler
from aistore.sdk.retry_manager import RetryManager
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
        retry_config: Optional[RetryConfig] = None,
    ):
        self._base_url = urljoin(endpoint, "v1")
        self._session_manager = session_manager
        self._token = token
        self._timeout = timeout
        self._response_handler = response_handler
        # smap is used to calculate the target node for a given object
        self._smap = None
        self._retry_manager = RetryManager(self._make_session_request, retry_config)

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
            retry_config=self._retry_manager.retry_config,
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

    def request(
        self,
        method: str,
        path: str,
        endpoint: str = None,
        headers: Dict[str, Any] = None,
        **kwargs,
    ) -> Response:
        """
        Make a request with the request_client's retry manager.

        If the request is an HTTPS request with a payload (`data`), it uses `_request_with_manual_redirect`.
        Otherwise, it makes a direct call using the current session manager with `_make_session_request`.

        Args:
            method (str): HTTP method (e.g. POST, GET, PUT, DELETE).
            path (str): URL path to call.
            endpoint (str): Alternative endpoint for the AIS cluster (e.g. for connecting to a specific proxy).
            headers (Dict[str, Any]): Extra headers to be passed with the request.
                Content-Type and User-Agent will be overridden.
            **kwargs (optional): Optional keyword arguments to pass with the call to request.

        Returns:
            The HTTP response from the server.
        """
        base = urljoin(endpoint, "v1") if endpoint else self._base_url
        url = f"{base}/{path.lstrip('/')}"
        headers = self._generate_headers(headers)

        def request_op():
            if url.startswith(HTTPS) and "data" in kwargs:
                return self._request_with_manual_redirect(
                    method=method, url=url, headers=headers, **kwargs
                )
            return self._make_session_request(
                method=method, url=url, headers=headers, **kwargs
            )

        response = self._retry_manager.with_retry(request_op)
        return self._response_handler.handle_response(response)

    def _generate_headers(
        self, headers: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if headers is None:
            headers = {}
        headers[HEADER_CONTENT_TYPE] = JSON_CONTENT_TYPE
        headers[HEADER_USER_AGENT] = f"{USER_AGENT_BASE}/{sdk_version}"
        if self.token:
            headers[HEADER_AUTHORIZATION] = f"Bearer {self.token}"
        return headers

    def _calculate_content_length(self, data: Union[bytes, str, Any]) -> Optional[int]:
        """
        Calculate the content length of data for HTTP requests.

        Handles multiple data types including bytes, strings, and file-like objects.

        Args:
            data: The data whose length needs to be calculated. Can be:
                - bytes or str: Direct length calculation
                - File-like object with fileno(): Uses fstat for efficiency
                - File-like object with seek(): Uses seek to determine size
                - Other types: Returns None with a warning

        Returns:
            The content length in bytes, or None if it cannot be determined

        Raises:
            IOError: If file operations fail during content length calculation
        """
        # Handle direct byte-like data
        if isinstance(data, (bytes, bytearray, memoryview)):
            return len(data)

        if isinstance(data, str):
            # Strings need to be encoded to get accurate byte count
            return len(data.encode("utf-8"))

        # Handle file-like objects
        if hasattr(data, "read"):
            # Try to get file size using fstat (most efficient)
            if hasattr(data, "fileno"):
                try:
                    return os.fstat(data.fileno()).st_size
                except (AttributeError, OSError) as e:
                    logger.debug(
                        "Failed to get file size using fstat: %s. Falling back to seek method.",
                        str(e),
                    )

            # Fall back to seek method for file-like objects
            if hasattr(data, "seek") and hasattr(data, "tell"):
                try:
                    current_pos = data.tell()
                    data.seek(0, os.SEEK_END)  # Seek to end (SEEK_END)
                    content_length = data.tell()
                    data.seek(current_pos)  # Restore original position
                    return content_length
                except (OSError, IOError) as e:
                    raise IOError(
                        f"Failed to determine content length using seek: {e}"
                    ) from e

        # Unsupported type - warn but don't fail
        logger.warning(
            "Cannot determine content length for data of type '%s'. "
            "Request will proceed without Content-Length header. "
            "This may cause HMAC signature mismatch errors if cluster authentication is enabled. "
            "For authenticated clusters, use bytes, str, or file-like objects.",
            type(data).__name__,
        )
        return None

    def _prepare_proxy_request(
        self, headers: Dict[str, Any], kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepare the request parameters for the initial proxy request.

        When data is present, the Content-Length header is calculated and set if possible,
        but the actual data is replaced with an empty byte string to avoid
        SSL EOF errors and improve efficiency.

        Args:
            headers: Request headers to be sent
            kwargs: Additional request parameters including optional 'data'

        Returns:
            Dict containing the prepared request parameters for the proxy

        Raises:
            IOError: If file operations fail during content length calculation
        """
        # Create a copy of headers to avoid mutating the original
        proxy_headers = {**headers, HEADER_CONNECTION: "close"}

        if "data" not in kwargs:
            return {
                "headers": proxy_headers,
                "allow_redirects": False,
                **kwargs,
            }

        # Calculate content length for data
        try:
            content_length = self._calculate_content_length(kwargs["data"])
        except IOError as e:
            logger.error("Failed to calculate content length: %s", str(e))
            raise

        # Add Content-Length header only if we could determine it
        if content_length is not None:
            proxy_headers[HEADER_CONTENT_LENGTH] = str(content_length)

        # Prepare proxy request without actual data payload
        # The proxy needs Content-Length for HMAC signature but not the data itself
        return {
            "headers": proxy_headers,
            "allow_redirects": False,
            "data": b"",  # Send empty data to proxy
            **{k: v for k, v in kwargs.items() if k != "data"},
        }

    def _request_with_manual_redirect(
        self, method: str, url: str, headers: Dict[str, Any], **kwargs
    ) -> Response:
        """
        Execute a request with manual redirect handling for HTTPS connections with data.

        This method implements a two-phase request pattern:
        1. Send a request to the proxy to obtain the redirect URL
        2. Send the actual request with data to the redirected target

        This workaround is necessary because the `requests` library does not properly
        handle 307 redirects from TLS-enabled servers when data is present in the request,
        resulting in SSL EOF errors: SSLEOFError(8, 'EOF occurred in violation of protocol')

        When cluster authentication is enabled, the proxy requires the Content-Length header
        to compute the HMAC signature, but the actual data payload is not needed until the
        request reaches the target node.

        Args:
            method: HTTP method (e.g., 'GET', 'POST', 'PUT', 'DELETE')
            url: Initial URL to the AIS proxy
            headers: HTTP headers to be sent with the request
            **kwargs: Additional request parameters (e.g., data, params, timeout)

        Returns:
            Response: The HTTP response from the final target server

        Raises:
            ValueError: If the proxy does not return a redirect or the redirect URL is missing
            TypeError: If data type is not supported
            IOError: If file operations fail during content length calculation
        """
        # Prepare request parameters for proxy
        try:
            proxy_request_kwargs = self._prepare_proxy_request(headers, kwargs)
        except (TypeError, IOError) as e:
            logger.error(
                "Failed to prepare proxy request for %s %s: %s", method, url, str(e)
            )
            raise

        # Send request to proxy to get redirect URL
        logger.debug("Sending %s request to proxy: %s", method, url)
        try:
            proxy_response = self.session_manager.session.request(
                method, url, **proxy_request_kwargs
            )
        except Exception as e:
            logger.error("Proxy request failed for %s %s: %s", method, url, str(e))
            raise

        # Extract redirect URL from response
        target_url = None
        if proxy_response.status_code in (STATUS_REDIRECT_PERM, STATUS_REDIRECT_TMP):
            target_url = proxy_response.headers.get(HEADER_LOCATION)
            logger.debug(
                "Received redirect (status %d) from proxy to: %s",
                proxy_response.status_code,
                target_url,
            )
        else:
            # Not a redirect - return the proxy response and let response handler deal with it
            logger.warning(
                "Proxy did not return a redirect response. Status: %d, URL: %s. "
                "Returning proxy response for downstream handling.",
                proxy_response.status_code,
                url,
            )
            return proxy_response

        # Close the proxy response since we got a valid redirect
        proxy_response.close()

        # Validate that we have a target URL
        if target_url is None:
            raise ValueError(
                f"No redirect URL received from proxy for {method} {url}. "
                f"Expected redirect status code ({STATUS_REDIRECT_PERM} or {STATUS_REDIRECT_TMP}), "
                f"but got {proxy_response.status_code}."
            )

        # Send the actual request with data to the target
        logger.debug("Sending %s request to target: %s", method, target_url)
        try:
            target_response = self._make_session_request(
                method=method, url=target_url, headers=headers, **kwargs
            )
        except Exception as e:
            logger.error(
                "Target request failed for %s %s: %s", method, target_url, str(e)
            )
            raise

        return target_response

    def _make_session_request(
        self, method: str, url: str, headers: Any, **kwargs
    ) -> Response:
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
