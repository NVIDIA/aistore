import os
import unittest
from http.server import BaseHTTPRequestHandler
from unittest.mock import MagicMock
from aistore.sdk.etl.webserver import HTTPMultiThreadedServer


class DummyETLServer(HTTPMultiThreadedServer):
    """
    Dummy ETL server for testing transform and MIME type override.
    """

    def transform(self, data: bytes, path: str) -> bytes:
        return data.upper()

    def get_mime_type(self) -> str:
        return "text/caps"


# pylint: disable=super-init-not-called
class DummyRequestHandler(BaseHTTPRequestHandler):
    """
    Fake handler that mocks HTTP methods for isolated testing of _set_headers().
    """

    def __init__(self):
        # Don't call super().__init__(), just mock the necessary parts
        self.send_response = MagicMock()
        self.send_header = MagicMock()
        self.end_headers = MagicMock()
        self.server = MagicMock()
        self.server.etl_server = MagicMock()
        self.server.etl_server.get_mime_type.return_value = "application/test"

    def set_headers(self, status_code: int = 200):
        self.send_response(status_code)
        self.send_header("Content-Type", self.server.etl_server.get_mime_type())
        self.end_headers()


class TestETLServerLogic(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.etl = DummyETLServer()

    def test_transform_uppercase(self):
        """Ensure transform() converts content to uppercase."""
        input_data = b"hello world"
        expected = b"HELLO WORLD"
        result = self.etl.transform(input_data, "/foo/bar")
        self.assertEqual(result, expected)

    def test_get_mime_type_override(self):
        """Test overridden MIME type returned by ETL server."""
        self.assertEqual(self.etl.get_mime_type(), "text/caps")


class TestRequestHandlerHelpers(unittest.TestCase):
    def test_set_headers_calls_expected_methods(self):
        """Ensure set_headers sets correct status and content-type."""
        handler = DummyRequestHandler()
        handler.set_headers(202)

        handler.send_response.assert_called_once_with(202)
        handler.send_header.assert_called_once_with("Content-Type", "application/test")
        handler.end_headers.assert_called_once()
