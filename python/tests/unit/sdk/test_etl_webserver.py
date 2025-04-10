import os
import sys
import unittest
from http.server import BaseHTTPRequestHandler
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient
from aistore.sdk.etl.webserver import HTTPMultiThreadedServer
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer


class DummyETLServer(HTTPMultiThreadedServer):
    """Dummy ETL server for testing transform and MIME type override."""

    def transform(self, data: bytes, path: str) -> bytes:
        return data.upper()

    def get_mime_type(self) -> str:
        return "text/caps"


class DummyRequestHandler(BaseHTTPRequestHandler):
    """Fake handler that mocks HTTP methods for isolated testing of `set_headers()`."""

    # pylint: disable=super-init-not-called
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


class DummyFastAPIServer(FastAPIServer):
    def transform(self, data: bytes, path: str) -> bytes:
        return data[::-1]  # Simple reverse transform for testing purposes

    def get_mime_type(self) -> str:
        return "application/test"


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


class TestFastAPIServer(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.etl_server = DummyFastAPIServer()
        self.client = TestClient(self.etl_server.app)

    def test_health_check(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"Running")

    # pylint: disable=protected-access
    async def test_get_network_content(self):
        path = "test/path"
        fake_content = b"fake data"

        with patch.object(self.etl_server, "client", AsyncMock()) as mock_client:
            mock_response = AsyncMock()
            mock_response.content = fake_content
            mock_response.raise_for_status = MagicMock()

            mock_client.get.return_value = mock_response

            result = await self.etl_server._get_network_content(path)

            self.assertEqual(result, fake_content)
            mock_client.get.assert_called_once()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_handle_get_request(self):
        self.etl_server.arg_type = ""
        path = "test/object"
        original_content = b"original data"
        transformed_content = original_content[::-1]

        with patch.object(
            self.etl_server,
            "_get_network_content",
            AsyncMock(return_value=original_content),
        ):
            response = self.client.get(f"/{path}")

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.content, transformed_content)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_handle_put_request(self):
        self.etl_server.arg_type = ""
        path = "test/object"
        input_content = b"input data"
        transformed_content = input_content[::-1]

        response = self.client.put(f"/{path}", content=input_content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, transformed_content)


class TestBaseEnforcement(unittest.TestCase):
    def test_fastapi_server_without_target_url(self):
        if "AIS_TARGET_URL" in os.environ:
            del os.environ["AIS_TARGET_URL"]

        class MinimalFastAPIServer(FastAPIServer):
            def transform(self, data: bytes, path: str) -> bytes:
                return data

        with self.assertRaises(EnvironmentError) as context:
            MinimalFastAPIServer()
        self.assertIn("AIS_TARGET_URL", str(context.exception))

    def test_http_server_server_without_target_url(self):
        if "AIS_TARGET_URL" in os.environ:
            del os.environ["AIS_TARGET_URL"]

        class MinimalHTTPServer(HTTPMultiThreadedServer):
            def transform(self, data: bytes, path: str) -> bytes:
                return data

        with self.assertRaises(EnvironmentError) as context:
            MinimalHTTPServer()
        self.assertIn("AIS_TARGET_URL", str(context.exception))

    def test_http_multithreaded_server_without_transform(self):
        if "AIS_TARGET_URL" in os.environ:
            del os.environ["AIS_TARGET_URL"]

        class IncompleteETLServer(HTTPMultiThreadedServer):
            pass

        with self.assertRaises(TypeError) as context:
            IncompleteETLServer()  # pylint: disable=abstract-class-instantiated
        self.assertIn("Can't instantiate abstract class", str(context.exception))

    def test_fastapi_server_without_transform(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost"

        class IncompleteFastAPIServer(FastAPIServer):
            pass

        with self.assertRaises(TypeError) as context:
            IncompleteFastAPIServer()  # pylint: disable=abstract-class-instantiated
        self.assertIn("Can't instantiate abstract class", str(context.exception))
