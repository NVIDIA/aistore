import unittest
from unittest.mock import patch, Mock

from aistore.sdk.request_client import RequestClient


class TestRequestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.mock_session = Mock()
        with patch("aistore.sdk.request_client.requests") as mock_requests_lib:
            mock_requests_lib.session.return_value = self.mock_session
            self.request_client = RequestClient(self.endpoint)

    def test_properties(self):
        self.assertEqual(self.endpoint + "/v1", self.request_client.base_url)
        self.assertEqual(self.endpoint, self.request_client.endpoint)
        self.assertEqual(self.mock_session, self.request_client.session)

    @patch("aistore.sdk.request_client.parse_raw_as")
    def test_request_deserialize(self, mock_parse):
        method = "method"
        path = "path"
        req_url = self.request_client.base_url + "/" + path
        app_header = {"Accept": "application/json"}

        deserialized_response = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "response_text"
        self.mock_session.request.return_value = mock_response
        mock_parse.return_value = deserialized_response
        res = self.request_client.request_deserialize(
            "method", "path", str, keyword="arg"
        )
        self.mock_session.request.assert_called_with(
            method, req_url, headers=app_header, keyword="arg"
        )
        self.assertEqual(deserialized_response, res)

    def test_request(self):
        method = "method"
        path = "path"
        req_url = self.request_client.base_url + "/" + path
        app_header = {"Accept": "application/json"}

        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_session.request.return_value = mock_response
        res = self.request_client.request("method", "path", keyword="arg")
        self.mock_session.request.assert_called_with(
            method, req_url, headers=app_header, keyword="arg"
        )
        self.assertEqual(mock_response, res)

        for response_code in [199, 300]:
            with patch("aistore.sdk.request_client.handle_errors") as mock_handle_err:
                mock_response.status_code = response_code
                self.mock_session.request.return_value = mock_response
                res = self.request_client.request("method", "path", keyword="arg")
                self.mock_session.request.assert_called_with(
                    method, req_url, headers=app_header, keyword="arg"
                )
                self.assertEqual(mock_response, res)
                mock_handle_err.assert_called_once()
