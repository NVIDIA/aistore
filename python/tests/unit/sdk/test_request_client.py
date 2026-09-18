import io
import unittest
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch, Mock, MagicMock, call

from requests import Response, Session
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
    UnrewindableBodyError,
)
from tenacity import stop_after_attempt, wait_none

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

    def _upload_retry_client(self, scheme, uploads, fail_first=True):
        # Model SDK retries, not Requests' automatic HTTP redirect handling.
        retry_config = RetryConfig.default()
        retry_config.network_retry = retry_config.network_retry.copy(
            wait=wait_none(), stop=stop_after_attempt(2), before_sleep=None
        )

        def send(_method, _url, **kwargs):
            if kwargs.get("allow_redirects") is False:
                return Mock(
                    spec=Response,
                    status_code=307,
                    headers={"Location": f"{scheme}://target/upload"},
                )
            body = kwargs["data"]
            uploads.append(body.read() if hasattr(body, "read") else b"".join(body))
            if fail_first and len(uploads) == 1:
                raise RequestsConnectionError("connection lost after upload")
            return self.mock_response

        self.mock_session.request.side_effect = send
        return RequestClient(
            f"{scheme}://proxy",
            self.mock_session_manager,
            retry_config=retry_config,
            response_handler=self.mock_response_handler,
        )

    @cases("http", "https")
    def test_upload_network_retry_rewinds_stream(self, scheme):
        for offset in (0, 3):
            with self.subTest(offset=offset):
                uploads = []
                body = io.BytesIO(b"abcdefgh")
                body.seek(offset)
                client = self._upload_retry_client(scheme, uploads)
                client.request("put", "objects/bucket/object", data=body)
                self.assertEqual(uploads, [b"abcdefgh"[offset:]] * 2)

    def test_upload_http_redirect_and_network_retry(self):
        """Exercise Requests redirect rewinding and the SDK retry with real HTTP."""
        uploads = []

        class UploadHandler(BaseHTTPRequestHandler):
            def do_PUT(self):  # pylint: disable=invalid-name
                if self.headers.get("Transfer-Encoding") == "chunked":
                    chunks = []
                    while True:
                        size = int(self.rfile.readline(), 16)
                        if not size:
                            self.rfile.readline()
                            break
                        chunks.append(self.rfile.read(size))
                        self.rfile.read(2)
                    body = b"".join(chunks)
                else:
                    body = self.rfile.read(int(self.headers["Content-Length"]))
                uploads.append((self.path, body))
                if self.path != "/target":
                    self.send_response(307)
                    self.send_header("Location", "/target")
                elif len(uploads) == 2:
                    # Drop the first target response after consuming the body.
                    return
                else:
                    self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_args):
                pass

        config = RetryConfig.default()
        config.network_retry = config.network_retry.copy(
            wait=wait_none(), stop=stop_after_attempt(2), before_sleep=None
        )
        manager = SessionManager(retry=config.http_retry)
        manager.session.trust_env = False
        with HTTPServer(("127.0.0.1", 0), UploadHandler) as server:
            worker = threading.Thread(target=server.serve_forever)
            worker.start()
            try:
                client = RequestClient(
                    f"http://127.0.0.1:{server.server_port}",
                    manager,
                    retry_config=config,
                    timeout=2,
                )
                body = io.BytesIO(b"abcdefgh")
                body.seek(3)
                response = client.request("put", "objects/bucket/object", data=body)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    uploads,
                    [("/v1/objects/bucket/object", b"defgh"), ("/target", b"defgh")]
                    * 2,
                )
            finally:
                manager.session.close()
                server.shutdown()
                worker.join()

    @cases("http", "https")
    def test_upload_nonseekable_network_retry(self, scheme):
        for fail_first, stream_kind in (
            (False, "file"),
            (True, "file"),
            (False, "iterator"),
            (True, "iterator"),
            (False, "iterable"),
            (True, "iterable"),
        ):
            with self.subTest(fail_first=fail_first, stream_kind=stream_kind):
                uploads = []
                if stream_kind == "file":
                    body = Mock(spec=["read"])
                    body.read.side_effect = io.BytesIO(b"abcdefgh").read
                elif stream_kind == "iterator":
                    body = iter([b"abcdefgh"])
                else:
                    body = MagicMock(spec=["__iter__"])
                    body.__iter__.return_value = iter([b"abcdefgh"])
                client = self._upload_retry_client(scheme, uploads, fail_first)
                if fail_first:
                    with self.assertRaises(UnrewindableBodyError):
                        client.request("put", "objects/bucket/object", data=body)
                else:
                    client.request("put", "objects/bucket/object", data=body)
                self.assertEqual(uploads, [b"abcdefgh"])

    def test_reusable_request_data_uses_existing_retry_path(self):
        for data in (
            None,
            b"data",
            "data",
            bytearray(b"data"),
            memoryview(b"data"),
            {"key": "value"},
            [("key", "value")],
            (("key", "value"),),
        ):
            with self.subTest(data_type=type(data).__name__):
                with patch.object(
                    self.default_request_client, "_request_with_stream_retry"
                ) as stream_retry:
                    self.default_request_client.request(
                        "put", "objects/bucket/object", data=data
                    )
                    stream_retry.assert_not_called()

    def test_upload_failed_position_network_retry(self):
        for operation in ("tell", "seek"):
            with self.subTest(operation=operation):
                uploads = []
                body = Mock(
                    wraps=io.BytesIO(b"abcdefgh"), spec=["read", "tell", "seek"]
                )
                getattr(body, operation).side_effect = OSError("stream is not seekable")
                client = self._upload_retry_client("http", uploads)
                with self.assertRaises(UnrewindableBodyError):
                    client.request("put", "objects/bucket/object", data=body)
                self.assertEqual(uploads, [b"abcdefgh"])

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
                token="",
                response_handler=self.mock_response_handler,
                retry_config=retry_config,
            )
            self.assertEqual(new_client, mock_constructor.return_value)

    def test_clone_with_args(self):
        new_base = "http://new-base-url"
        new_response_handler = MagicMock()
        new_response_handler.__bool__.return_value = False
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
            new_client = initial_client.clone(new_base, new_response_handler)

            mock_constructor.assert_called_once_with(
                endpoint=new_base + "/v1",
                session_manager=self.mock_session_manager,
                timeout=timeout,
                token=token,
                response_handler=new_response_handler,
                retry_config=retry_config,
            )
            self.assertEqual(new_client, mock_constructor.return_value)

    def test_successful_request(self):
        """Test successful request with no retries."""
        self.mock_response.text = "Success"
        response = self.default_request_client.request("GET", "http://test-url")

        # Validate expected attributes
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "Success")

        # Ensure only one request was made through the underlying session
        self.mock_session.request.assert_called_once()
        self.mock_retry_manager_instance.with_retry.assert_called_once()
