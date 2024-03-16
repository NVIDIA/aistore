import unittest
from unittest.mock import patch, Mock

from requests import Response

from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
    AIS_SERVER_CRT,
)
from aistore.sdk.request_client import RequestClient
from aistore.version import __version__ as sdk_version
from tests.utils import test_cases


class TestRequestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.mock_session = Mock()
        with patch("aistore.sdk.request_client.requests") as mock_requests_lib:
            mock_requests_lib.sessions.session.return_value = self.mock_session
            self.request_client = RequestClient(
                self.endpoint, skip_verify=True, ca_cert=""
            )

        self.request_headers = {
            HEADER_CONTENT_TYPE: JSON_CONTENT_TYPE,
            HEADER_USER_AGENT: f"{USER_AGENT_BASE}/{sdk_version}",
        }

    def test_default_session(self):
        with patch(
            "aistore.sdk.request_client.os.getenv", return_value=None
        ) as mock_getenv:
            self.request_client = RequestClient(self.endpoint)
            mock_getenv.assert_called_with(AIS_SERVER_CRT)
            self.assertEqual(True, self.request_client.session.verify)

    @test_cases(
        (("env-cert", "arg-cert", False), "arg-cert"),
        (("env-cert", "arg-cert", True), False),
        (("env-cert", None, False), "env-cert"),
        ((True, None, False), True),
        ((None, None, True), False),
    )
    def test_session(self, test_case):
        env_cert, arg_cert, skip_verify = test_case[0]
        with patch(
            "aistore.sdk.request_client.os.getenv", return_value=env_cert
        ) as mock_getenv:
            self.request_client = RequestClient(
                self.endpoint, skip_verify=skip_verify, ca_cert=arg_cert
            )
            if not skip_verify and not arg_cert:
                mock_getenv.assert_called_with(AIS_SERVER_CRT)
            self.assertEqual(test_case[1], self.request_client.session.verify)

    def test_properties(self):
        self.assertEqual(self.endpoint + "/v1", self.request_client.base_url)
        self.assertEqual(self.endpoint, self.request_client.endpoint)

    @patch("aistore.sdk.request_client.RequestClient.request")
    @patch("aistore.sdk.request_client.decode_response")
    def test_request_deserialize(self, mock_decode, mock_request):
        method = "method"
        path = "path"
        decoded_value = "test value"
        custom_kw = "arg"
        mock_decode.return_value = decoded_value
        mock_response = Mock(Response)
        mock_request.return_value = mock_response

        res = self.request_client.request_deserialize(
            method, path, str, keyword=custom_kw
        )

        self.assertEqual(decoded_value, res)
        mock_request.assert_called_with(method, path, keyword=custom_kw)
        mock_decode.assert_called_with(str, mock_response)

    @test_cases(None, "http://custom_endpoint")
    def test_request(self, endpoint_arg):
        method = "request_method"
        path = "request_path"
        extra_kw_arg = "arg"
        extra_headers = {"header_1_key": "header_1_val", "header_2_key": "header_2_val"}
        self.request_headers.update(extra_headers)
        if endpoint_arg:
            req_url = f"{endpoint_arg}/v1/{path}"
        else:
            req_url = f"{self.request_client.base_url}/{path}"

        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_session.request.return_value = mock_response
        if endpoint_arg:
            res = self.request_client.request(
                method,
                path,
                endpoint=endpoint_arg,
                headers=extra_headers,
                keyword=extra_kw_arg,
            )
        else:
            res = self.request_client.request(
                method, path, headers=extra_headers, keyword=extra_kw_arg
            )
        self.mock_session.request.assert_called_with(
            method,
            req_url,
            headers=self.request_headers,
            timeout=None,
            keyword=extra_kw_arg,
        )
        self.assertEqual(mock_response, res)

        for response_code in [199, 300]:
            with patch("aistore.sdk.request_client.handle_errors") as mock_handle_err:
                mock_response.status_code = response_code
                self.mock_session.request.return_value = mock_response
                res = self.request_client.request(
                    method,
                    path,
                    endpoint=endpoint_arg,
                    headers=extra_headers,
                    keyword=extra_kw_arg,
                )
                self.mock_session.request.assert_called_with(
                    method,
                    req_url,
                    headers=self.request_headers,
                    timeout=None,
                    keyword=extra_kw_arg,
                )
                self.assertEqual(mock_response, res)
                mock_handle_err.assert_called_once()

    def test_get_full_url(self):
        path = "/testpath/to_obj"
        params = {"p1key": "p1val", "p2key": "p2val"}
        res = self.request_client.get_full_url(path, params)
        self.assertEqual(
            "https://aistore-endpoint/v1/testpath/to_obj?p1key=p1val&p2key=p2val", res
        )
