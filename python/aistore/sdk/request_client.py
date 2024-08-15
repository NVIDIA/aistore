#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#
import os
from urllib.parse import urljoin, urlencode
from typing import Optional, TypeVar, Tuple, Type, Union, Any, Dict
from requests import session, Session, Response

from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
    AIS_CLIENT_CA,
    HEADER_AUTHORIZATION,
)
from aistore.sdk.utils import handle_errors, decode_response
from aistore.version import __version__ as sdk_version

T = TypeVar("T")


# pylint: disable=unused-variable, duplicate-code, too-many-arguments
class RequestClient:
    """
    Internal client for buckets, objects, jobs, etc. to use for making requests to an AIS cluster.

    Args:
        endpoint (str): AIStore endpoint
        skip_verify (bool, optional): If True, skip SSL certificate verification. Defaults to False.
        ca_cert (str, optional): Path to a CA certificate file for SSL verification.
        timeout (Union[float, Tuple[float, float], None], optional): Request timeout in seconds; a single float
            for both connect/read timeouts (e.g., 5.0), a tuple for separate connect/read timeouts (e.g., (3.0, 10.0)),
            or None to disable timeout.
        token (str, optional): Authorization token.
    """

    def __init__(
        self,
        endpoint: str,
        skip_verify: bool = False,
        ca_cert: str = None,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        token: str = None,
    ):
        self._endpoint = endpoint
        self._base_url = urljoin(endpoint, "v1")
        self._timeout = timeout
        self._skip_verify = skip_verify
        self._ca_cert = ca_cert
        self._session = self.create_new_session()
        self._token = token

    def create_new_session(self) -> Session:
        """
        Creates a new requests session for HTTP requests.

        Returns:
            New HTTP request Session
        """
        request_session = session()
        if "https" in self._endpoint:
            self._set_session_verification(request_session)
        return request_session

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

    @property
    def base_url(self):
        """
        Returns: AIS cluster base url
        """
        return self._base_url

    @property
    def endpoint(self):
        """
        Returns: AIS cluster endpoint
        """
        return self._endpoint

    @property
    def session(self):
        """
        Returns: Active request session
        """
        return self._session

    @property
    def token(self):
        """
        Returns: Token for Authorization
        """
        return self._token

    @token.setter
    def token(self, token: str):
        """
        Set the token for Authorization.

        Args:
            token (str): Token for Authorization. Must be a non-empty string.

        Raises:
            ValueError: If the provided token is empty.
        """
        if not token:
            raise ValueError("Token must be a non-empty string.")
        self._token = token

    def request_deserialize(
        self, method: str, path: str, res_model: Type[T], **kwargs
    ) -> T:
        """
        Make a request to the AIS cluster and deserialize the response to a defined type
        Args:
            method (str): HTTP method, e.g. POST, GET, PUT, DELETE
            path (str): URL path to call
            res_model (Type[T]): Resulting type to which the response should be deserialized
            **kwargs (optional): Optional keyword arguments to pass with the call to request

        Returns:
            Parsed result of the call to the API, as res_model
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
        Make a request to the AIS cluster
        Args:
            method (str): HTTP method, e.g. POST, GET, PUT, DELETE
            path (str): URL path to call
            endpoint (str): Alternative endpoint for the AIS cluster, e.g. for connecting to a specific proxy
            headers (dict): Extra headers to be passed with the request. Content-Type and User-Agent will be overridden
            **kwargs (optional): Optional keyword arguments to pass with the call to request

        Returns:
            Raw response from the API
        """
        base = urljoin(endpoint, "v1") if endpoint else self._base_url
        url = f"{base}/{path.lstrip('/')}"
        if headers is None:
            headers = {}
        headers[HEADER_CONTENT_TYPE] = JSON_CONTENT_TYPE
        headers[HEADER_USER_AGENT] = f"{USER_AGENT_BASE}/{sdk_version}"
        if self.token:
            headers[HEADER_AUTHORIZATION] = f"Bearer {self.token}"

        resp = self.session.request(
            method,
            url,
            headers=headers,
            timeout=self._timeout,
            **kwargs,
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            handle_errors(resp)
        return resp

    def get_full_url(self, path: str, params: Dict[str, Any]):
        """
        Get the full URL to the path on the cluster with the parameters given

        Args:
            path: Path on the cluster
            params: Query parameters to include

        Returns:
            URL including cluster base url and parameters

        """
        return f"{self._base_url}/{path.lstrip('/')}?{urlencode(params)}"
