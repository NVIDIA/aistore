import unittest
from unittest.mock import patch, Mock, call

from requests import Response, Session
from requests.exceptions import (
    ConnectTimeout,
    ConnectionError as RequestsConnectionError,
)
from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
)
from aistore.sdk.request_client import RequestClient
from aistore.sdk.session_manager import SessionManager
from aistore.version import __version__ as sdk_version
from aistore.sdk.errors import AISRetryableError
from tests.utils import cases


class TestRequestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.mock_response = Mock(name="Mock response", spec=Response)
        self.mock_session = Mock(name="Mock session", spec=Session)
        self.mock_session.request.return_value = self.mock_response
        self.mock_session_manager = Mock(spec=SessionManager, session=self.mock_session)
        self.mock_response_handler = Mock()
        self.mock_response_handler.handle_response.return_value = self.mock_response
        self.request_headers = {
            HEADER_CONTENT_TYPE: JSON_CONTENT_TYPE,
            HEADER_USER_AGENT: f"{USER_AGENT_BASE}/{sdk_version}",
        }
        self.default_request_client = RequestClient(
            self.endpoint,
            self.mock_session_manager,
            response_handler=self.mock_response_handler,
        )

    def test_init_default(self):
        self.assertEqual(self.endpoint + "/v1", self.default_request_client.base_url)
        self.assertEqual(
            self.mock_session_manager, self.default_request_client.session_manager
        )
        self.assertIsNone(self.default_request_client.timeout)

    @cases(
        10,
        30.0,
        (10, 30.0),
    )
    def test_init_properties(self, timeout):
        auth_token = "any string"
        request_client = RequestClient(
            self.endpoint, self.mock_session_manager, timeout=timeout, token=auth_token
        )
        self.assertEqual(self.endpoint + "/v1", request_client.base_url)
        self.assertEqual(self.mock_session_manager, request_client.session_manager)
        self.assertEqual(timeout, request_client.timeout)
        self.assertEqual(auth_token, request_client.token)

    def test_update_token(self):
        auth_token = "any string"
        self.default_request_client.token = auth_token
        self.assertEqual(auth_token, self.default_request_client.token)

    @cases(
        10,
        30.0,
        (10, 30.0),
    )
    def test_update_timeout(self, timeout):
        self.default_request_client.timeout = timeout
        self.assertEqual(timeout, self.default_request_client.timeout)

    @patch("aistore.sdk.request_client.decode_response")
    def test_request_deserialize(self, mock_decode):
        method = "method"
        path = "path"
        decoded_value = "test value"
        custom_kw = "arg"
        mock_decode.return_value = decoded_value
        self.mock_response.status_code = 200

        res = self.default_request_client.request_deserialize(
            method, path, str, keyword=custom_kw
        )

        expected_url = self.endpoint + "/v1/" + path
        self.assertEqual(decoded_value, res)
        self.mock_session.request.assert_called_with(
            method,
            expected_url,
            headers=self.request_headers,
            keyword=custom_kw,
        )
        mock_decode.assert_called_with(str, self.mock_response)

    @cases((None, None), ("http://custom_endpoint", 30))
    def test_request(self, test_case):
        endpoint_arg, timeout = test_case
        if timeout:
            self.default_request_client.timeout = timeout
        method = "request_method"
        path = "request_path"
        extra_kw_arg = "arg"
        extra_headers = {"header_1_key": "header_1_val", "header_2_key": "header_2_val"}
        self.request_headers.update(extra_headers)
        if endpoint_arg:
            req_url = f"{endpoint_arg}/v1/{path}"
        else:
            req_url = f"{self.default_request_client.base_url}/{path}"

        self.mock_response.status_code = 200
        if endpoint_arg:
            res = self.default_request_client.request(
                method,
                path,
                endpoint=endpoint_arg,
                headers=extra_headers,
                keyword=extra_kw_arg,
            )
        else:
            res = self.default_request_client.request(
                method, path, headers=extra_headers, keyword=extra_kw_arg
            )
        self._request_assert(method, req_url, timeout, extra_kw_arg)
        self.assertEqual(self.mock_response, res)
        self.mock_response_handler.handle_response.assert_called_with(
            self.mock_response
        )

    def _request_assert(self, method, url, timeout, expected_kw):
        if timeout:
            self.mock_session.request.assert_called_with(
                method,
                url,
                headers=self.request_headers,
                timeout=timeout,
                keyword=expected_kw,
            )
        else:
            self.mock_session.request.assert_called_with(
                method,
                url,
                headers=self.request_headers,
                keyword=expected_kw,
            )

    def test_request_https_data(self):
        method = "request_method"
        path = "request_path"
        extra_kw_arg = "arg"
        data = "my_data"
        expected_url = self.endpoint + "/v1/" + path
        redirect_url = "target" + "/v1/" + path

        redirect_response = Mock(spec=Response)
        redirect_response.status_code = 307
        redirect_response.headers = {"Location": redirect_url}
        self.mock_response.status_code = 200
        self.mock_session.request.side_effect = [redirect_response, self.mock_response]

        response = self.default_request_client.request(
            method, path, data=data, keyword=extra_kw_arg
        )

        self.assertEqual(self.mock_response, response)

        expected_proxy_call = call(
            method,
            expected_url,
            headers=self.request_headers,
            allow_redirects=False,
            keyword=extra_kw_arg,
        )
        expected_target_call = call(
            method,
            redirect_url,
            headers=self.request_headers,
            keyword=extra_kw_arg,
            data=data,
        )

        self.mock_session.request.assert_has_calls(
            [expected_proxy_call, expected_target_call]
        )
        self.mock_response_handler.handle_response.assert_called_with(
            self.mock_response
        )
        self.mock_response_handler.handle_response.assert_called_once()

    def test_get_full_url(self):
        path = "/testpath/to_obj"
        params = {"p1key": "p1val", "p2key": "p2val"}
        res = self.default_request_client.get_full_url(path, params)
        self.assertEqual(
            "https://aistore-endpoint/v1/testpath/to_obj?p1key=p1val&p2key=p2val", res
        )

    @patch("aistore.sdk.request_client.RequestClient._session_request")
    def test_successful_request(self, mock_request):
        """Test successful request with no retries."""

        self.mock_response.status_code = 200
        self.mock_response.text = "Success"
        mock_request.return_value = self.mock_response
        response = self.default_request_client.request("GET", "http://test-url", {})

        # Validate expected attributes
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "Success")

        # Ensure only one request was made
        mock_request.assert_called_once()

    @patch("aistore.sdk.request_client.RequestClient._session_request")
    def test_retry_on_connect_timeout(self, mock_request):
        """Test that the function retries on ConnectTimeout."""
        self.mock_response.status_code = 200
        mock_request.side_effect = [
            ConnectTimeout,
            self.mock_response,
        ]  # Fails once, then succeeds

        response = self.default_request_client.request("GET", "http://test-url", {})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)  # Retries once before success

    @patch("aistore.sdk.request_client.RequestClient._session_request")
    def test_retry_on_connection_error(self, mock_request):
        """Test that the function retries on ConnectionError (e.g., refused connection)."""
        self.mock_response.status_code = 200
        mock_request.side_effect = [
            RequestsConnectionError,
            RequestsConnectionError,
            self.mock_response,
        ]
        response = self.default_request_client.request("GET", "http://test-url", {})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_request.call_count, 3)  # Retries twice before success

    @patch("aistore.sdk.request_client.RequestClient._session_request")
    def test_max_retries_exceeded(self, mock_request):
        """Test that the function raises an error after max retries are exceeded."""
        mock_request.side_effect = RequestsConnectionError  # Always fails

        with self.assertRaises(RequestsConnectionError):
            self.default_request_client.request("GET", "http://test-url", {})

        self.assertEqual(mock_request.call_count, 10)  # Stops at max retry limit

    @patch("aistore.sdk.request_client.RequestClient._session_request")
    def test_unexpected_exception(self, mock_request):
        """Test that an unexpected exception is raised correctly."""
        mock_request.side_effect = ValueError(
            "Unexpected error"
        )  # Simulate unexpected failure

        with self.assertRaises(ValueError):
            self.default_request_client.request("GET", "http://test-url", {})

        mock_request.assert_called_once()  # Should fail immediately, no retries

    @patch("aistore.sdk.request_client.RequestClient._session_request")
    def test_ais_retriable_errors(self, mock_request):
        """Test that the function is retried if it raises AISRetriableError."""
        self.mock_response.status_code = 200
        mock_request.side_effect = [
            AISRetryableError(409, "Conflict", "http://test-url"),
            self.mock_response,
        ]

        response = self.default_request_client.request("GET", "http://test-url", {})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)  # Retries once before success
