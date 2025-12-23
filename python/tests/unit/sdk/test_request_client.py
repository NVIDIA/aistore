import unittest
from unittest.mock import patch, Mock, call

from requests import Response, Session

from aistore.sdk import RetryConfig
from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
)

from aistore.sdk.request_client import RequestClient
from aistore.sdk.retry_manager import RetryManager
from aistore.sdk.session_manager import SessionManager
from aistore.version import __version__ as sdk_version
from tests.utils import cases

TEST_ENDPOINT = "https://aistore-endpoint"


class TestRequestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_response = Mock(name="Mock response", spec=Response, status_code=200)
        self.mock_session = Mock(name="Mock session", spec=Session)
        self.mock_session.request.return_value = self.mock_response
        self.mock_session_manager = Mock(spec=SessionManager, session=self.mock_session)
        self.mock_retry_manager_instance = Mock(spec=RetryManager)
        # Do not actually use the retry manager's retry, but call the function directly
        self.mock_retry_manager_instance.with_retry.side_effect = (
            lambda func, *args, **kwargs: func(*args, **kwargs)
        )
        self.mock_response_handler = Mock()
        self.mock_response_handler.handle_response.return_value = self.mock_response
        self.request_headers = {
            HEADER_CONTENT_TYPE: JSON_CONTENT_TYPE,
            HEADER_USER_AGENT: f"{USER_AGENT_BASE}/{sdk_version}",
        }
        with patch(
            "aistore.sdk.request_client.RetryManager",
            Mock(return_value=self.mock_retry_manager_instance),
        ):
            self.default_request_client = self._create_request_client()

    def _create_request_client(self):
        return RequestClient(
            TEST_ENDPOINT,
            self.mock_session_manager,
            response_handler=self.mock_response_handler,
        )

    def test_init_default(self):
        self.assertEqual(TEST_ENDPOINT + "/v1", self.default_request_client.base_url)
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
            TEST_ENDPOINT, self.mock_session_manager, timeout=timeout, token=auth_token
        )
        self.assertEqual(TEST_ENDPOINT + "/v1", request_client.base_url)
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

        res = self.default_request_client.request_deserialize(
            method, path, str, keyword=custom_kw
        )

        expected_url = TEST_ENDPOINT + "/v1/" + path
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
        self.mock_retry_manager_instance.with_retry.assert_called_once()
        self.mock_retry_manager_instance.reset_mock()
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
        expected_url = TEST_ENDPOINT + "/v1/" + path
        redirect_url = "target" + "/v1/" + path

        redirect_response = Mock(spec=Response)
        redirect_response.status_code = 307
        redirect_response.headers = {"Location": redirect_url}
        self.mock_session.request.side_effect = [redirect_response, self.mock_response]

        response = self.default_request_client.request(
            method, path, data=data, keyword=extra_kw_arg
        )

        self.assertEqual(self.mock_response, response)

        # Proxy call: sends empty data but includes Content-Length and Connection headers
        proxy_headers = self.request_headers.copy()
        proxy_headers.update({"Connection": "close", "Content-Length": "7"})
        expected_proxy_call = call(
            method,
            expected_url,
            headers=proxy_headers,
            allow_redirects=False,
            data=b"",
            keyword=extra_kw_arg,
        )
        # Target call: sends actual data
        expected_target_call = call(
            method,
            redirect_url,
            headers=self.request_headers,
            data=data,
            keyword=extra_kw_arg,
        )

        self.mock_session.request.assert_has_calls(
            [expected_proxy_call, expected_target_call]
        )
        self.mock_retry_manager_instance.with_retry.assert_called_once()
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

    def test_clone_default(self):
        retry_config = RetryConfig.default()
        self.mock_retry_manager_instance.retry_config = retry_config
        with patch(
            "aistore.sdk.request_client.RequestClient", autospec=True
        ) as mock_constructor:
            new_client = self.default_request_client.clone()

            mock_constructor.assert_called_once_with(
                endpoint=self.default_request_client.base_url,
                session_manager=self.mock_session_manager,
                timeout=None,
                token=None,
                response_handler=self.mock_response_handler,
                retry_config=retry_config,
            )
            self.assertEqual(new_client, mock_constructor.return_value)

    def test_clone_with_args(self):
        new_base = "http://new-base-url"
        retry_config = RetryConfig.default()
        # Set some non-default values in the initial client and ensure they are passed through
        retry_config.cold_get_conf.max_cold_wait = 300
        timeout = (20, 30)
        token = "some-token-string"
        initial_client = RequestClient(
            TEST_ENDPOINT,
            self.mock_session_manager,
            timeout,
            token,
            self.mock_response_handler,
            retry_config,
        )
        with patch(
            "aistore.sdk.request_client.RequestClient", autospec=True
        ) as mock_constructor:
            new_client = initial_client.clone(new_base)

            mock_constructor.assert_called_once_with(
                endpoint=new_base + "/v1",
                session_manager=self.mock_session_manager,
                timeout=timeout,
                token=token,
                response_handler=self.mock_response_handler,
                retry_config=retry_config,
            )
            self.assertEqual(new_client, mock_constructor.return_value)

    @patch("aistore.sdk.request_client.RequestClient._make_session_request")
    def test_successful_request(self, mock_request):
        """Test successful request with no retries."""
        self.mock_response.text = "Success"
        mock_request.return_value = self.mock_response
        response = self.default_request_client.request("GET", "http://test-url")

        # Validate expected attributes
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "Success")

        # Ensure only one request was made
        mock_request.assert_called_once()
        self.mock_retry_manager_instance.with_retry.assert_called_once()
