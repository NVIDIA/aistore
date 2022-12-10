import unittest
from unittest.mock import patch, Mock

from aistore import Client
from aistore.client.cluster import Cluster
from aistore.client.etl import Etl
from aistore.client.types import Namespace
from aistore.client.xaction import Xaction


class TestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.mock_session = Mock()
        with patch("aistore.client.api.requests") as mock_requests_lib:
            mock_requests_lib.session.return_value = self.mock_session
            self.client = Client(self.endpoint)

    def test_properties(self):
        self.assertEqual(self.endpoint + "/v1", self.client.base_url)
        self.assertEqual(self.endpoint, self.client.endpoint)
        self.assertEqual(self.mock_session, self.client.session)

    @patch("aistore.client.api.parse_raw_as")
    def test_request_deserialize(self, mock_parse):
        method = "method"
        path = "path"
        req_url = self.client.base_url + "/" + path
        app_header = {"Accept": "application/json"}

        deserialized_response = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "response_text"
        self.mock_session.request.return_value = mock_response
        mock_parse.return_value = deserialized_response
        res = self.client.request_deserialize("method", "path", str, keyword="arg")
        self.mock_session.request.assert_called_with(
            method, req_url, headers=app_header, keyword="arg"
        )
        self.assertEqual(deserialized_response, res)

    def test_request(self):
        method = "method"
        path = "path"
        req_url = self.client.base_url + "/" + path
        app_header = {"Accept": "application/json"}

        mock_response = Mock()
        mock_response.status_code = 200
        self.mock_session.request.return_value = mock_response
        res = self.client.request("method", "path", keyword="arg")
        self.mock_session.request.assert_called_with(
            method, req_url, headers=app_header, keyword="arg"
        )
        self.assertEqual(mock_response, res)

        for response_code in [199, 300]:
            with patch("aistore.client.api.handle_errors") as mock_handle_err:
                mock_response.status_code = response_code
                self.mock_session.request.return_value = mock_response
                res = self.client.request("method", "path", keyword="arg")
                self.mock_session.request.assert_called_with(
                    method, req_url, headers=app_header, keyword="arg"
                )
                self.assertEqual(mock_response, res)
                mock_handle_err.assert_called_once()

    def test_bucket(self):
        bck_name = "name"
        provider = "provider"
        namespace = Namespace(uuid="id", name="namespace")
        result = self.client.bucket(bck_name, provider, namespace)
        self.assertEqual(self.client, result.client)
        self.assertEqual(bck_name, result.bck.name)
        self.assertEqual(provider, result.provider)
        self.assertEqual(namespace, result.namespace)

    def test_cluster(self):
        res = self.client.cluster()
        self.assertEqual(self.client, res.client)
        self.assertIsInstance(res, Cluster)

    def test_xaction(self):
        res = self.client.xaction()
        self.assertEqual(self.client, res.client)
        self.assertIsInstance(res, Xaction)

    def test_etl(self):
        res = self.client.etl()
        self.assertEqual(self.client, res.client)
        self.assertIsInstance(res, Etl)
