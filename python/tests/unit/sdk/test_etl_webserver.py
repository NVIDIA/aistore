import os
import sys
import unittest
from http.server import BaseHTTPRequestHandler
from unittest.mock import MagicMock, patch, AsyncMock

from fastapi.testclient import TestClient
from flask.testing import FlaskClient

from aistore.sdk.const import HEADER_NODE_URL
from aistore.sdk.etl.webserver import (
    HTTPMultiThreadedServer,
    FlaskServer,
    FastAPIServer,
)


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


class DummyFlaskServer(FlaskServer):
    def transform(self, data: bytes, path: str) -> bytes:
        return b"flask: " + data

    def get_mime_type(self) -> str:
        return "application/flask"


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
        os.environ["ARG_TYPE"] = ""
        os.environ["DIRECT_PUT"] = "false"
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

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_websocket(self):
        with self.client.websocket_connect("/ws") as websocket:
            original_data = b"abcdef"
            websocket.send_bytes(original_data)
            result = websocket.receive_bytes()
            self.assertEqual(result, original_data[::-1])


class TestFastAPIServerWithDirectPut(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["ARG_TYPE"] = ""
        os.environ["DIRECT_PUT"] = "true"
        self.etl_server = DummyFastAPIServer()
        self.client = TestClient(self.etl_server.app)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_direct_put(self):
        self.etl_server.arg_type = ""
        path = "test/object"
        input_content = b"input data"

        # Mock the direct delivery response (simulate 200 OK)
        mock_response_success = AsyncMock()
        mock_response_success.status_code = 200
        self.etl_server.client = AsyncMock()
        self.etl_server.client.put.return_value = mock_response_success

        headers = {HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"}
        response = self.client.put(f"/{path}", content=input_content, headers=headers)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")  # No content returned
        self.etl_server.client.put.assert_awaited_once()

        # Mock the direct delivery response (simulate 500 FAIL)
        mock_response_fail = AsyncMock()
        mock_response_fail.status_code = 500
        self.etl_server.client = AsyncMock()
        self.etl_server.client.put.return_value = mock_response_fail

        headers = {HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"}
        response = self.client.put(f"/{path}", content=input_content, headers=headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.content, input_content[::-1]
        )  # Original content returned
        self.etl_server.client.put.assert_awaited_once()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_websocket_with_direct_put(self):
        self.etl_server.arg_type = ""
        input_data = b"testdata"
        direct_put_url = "http://localhost:8080/ais/@/etl_dst/final"

        # Mock the direct put response (simulate 200 OK) => return "direct put success" as ACK
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_client.put.return_value = mock_resp

            with self.client.websocket_connect("/ws") as websocket:
                websocket.send_text(direct_put_url)
                websocket.send_bytes(input_data)
                result = websocket.receive_text()
                self.assertEqual(result, "direct put success")

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url, data=input_data[::-1]
            )

        # Mock the direct put response (simulate 500 FAIL) => return transformed object
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 500
            mock_client.put.return_value = mock_resp

            with self.client.websocket_connect("/ws") as websocket:
                websocket.send_text(direct_put_url)
                websocket.send_bytes(input_data)
                result = websocket.receive_bytes()
                self.assertEqual(result, input_data[::-1])

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url, data=input_data[::-1]
            )

        # Mock the empty direct put url (don't need direct put on this object) => return transformed object
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            with self.client.websocket_connect("/ws") as websocket:
                websocket.send_text("")  # empty direct put url
                websocket.send_bytes(input_data)
                result = websocket.receive_bytes()
                self.assertEqual(result, input_data[::-1])
            mock_client.put.assert_not_called()  # direct put shouldn't be called

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_websocket_with_direct_put_and_fqn(self):
        fqn = "test/object"
        original_content = b"original data"
        direct_put_url = "http://localhost:8080/ais/@/etl_dst/final"
        self.etl_server.arg_type = "fqn"  # Use fqn
        # Mock the direct put response (simulate 200 OK)
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_client.put.return_value = mock_resp

            with patch.object(
                self.etl_server,
                "_get_fqn_content",
                AsyncMock(return_value=original_content),
            ) as get_fqn_mock:
                with self.client.websocket_connect("/ws") as websocket:
                    websocket.send_text(direct_put_url)
                    websocket.send_text(fqn)
                    result = websocket.receive_text()
                    self.assertEqual(result, "direct put success")

                get_fqn_mock.assert_called_once_with(fqn)

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url, data=original_content[::-1]
            )

        # Mock the direct put response (simulate 500 FAIL) => return transformed object
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 500
            mock_client.put.return_value = mock_resp

            with patch.object(
                self.etl_server,
                "_get_fqn_content",
                AsyncMock(return_value=original_content),
            ) as get_fqn_mock:
                with self.client.websocket_connect("/ws") as websocket:
                    websocket.send_text(direct_put_url)
                    websocket.send_text(fqn)
                    result = websocket.receive_bytes()
                    self.assertEqual(result, original_content[::-1])

                get_fqn_mock.assert_called_once_with(fqn)

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url, data=original_content[::-1]
            )

        # Mock the empty direct put url (don't need direct put on this object) => return transformed object
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            with patch.object(
                self.etl_server,
                "_get_fqn_content",
                AsyncMock(return_value=original_content),
            ) as get_fqn_mock:
                with self.client.websocket_connect("/ws") as websocket:
                    websocket.send_text("")
                    websocket.send_text(fqn)
                    result = websocket.receive_bytes()
                    self.assertEqual(result, original_content[::-1])

                get_fqn_mock.assert_called_once_with(fqn)
            mock_client.put.assert_not_called()


class TestFlaskServer(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost"
        self.etl_server = DummyFlaskServer()
        self.client: FlaskClient = self.etl_server.app.test_client()

    def test_health_check(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"Running")
        assert "Content-Length" in response.headers
        assert int(response.headers["Content-Length"]) == len(response.data)

    def test_transform_put(self):
        input_data = b"hello"
        response = self.client.put("/some/key", data=input_data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"flask: " + input_data)
        assert "Content-Length" in response.headers
        assert int(response.headers["Content-Length"]) == len(response.data)

    def test_transform_get(self):
        input_data = b"flask get data"
        path = "/some/key"
        with patch("aistore.sdk.etl.webserver.flask_server.requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                content=input_data, raise_for_status=MagicMock()
            )
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, b"flask: " + input_data)
            assert "Content-Length" in response.headers
            assert int(response.headers["Content-Length"]) == len(response.data)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_direct_put_delivery(self):
        self.etl_server.arg_type = ""
        path = "test/object"
        input_content = b"input data"
        headers = {HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"}

        with patch("aistore.sdk.etl.webserver.flask_server.requests.put") as mock_put:
            # Mock the direct delivery response (simulate 200 OK)
            mock_put.return_value = MagicMock(status_code=200)
            response = self.client.put(f"/{path}", data=input_content, headers=headers)

            self.assertEqual(response.status_code, 204)
            self.assertEqual(response.data, b"")  # No content returned

        with patch("aistore.sdk.etl.webserver.flask_server.requests.put") as mock_put:
            # Mock the direct delivery response (simulate 500 FAIL)
            mock_put.return_value = MagicMock(status_code=500)
            response = self.client.put(f"/{path}", data=input_content, headers=headers)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.data, b"flask: " + input_content
            )  # Original content returned
            assert "Content-Length" in response.headers
            assert int(response.headers["Content-Length"]) == len(response.data)


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

    def test_flask_server_without_target_url(self):
        if "AIS_TARGET_URL" in os.environ:
            del os.environ["AIS_TARGET_URL"]

        class MinimalFlaskServer(FlaskServer):
            def transform(self, data: bytes, path: str) -> bytes:
                return data

        with self.assertRaises(EnvironmentError) as context:
            MinimalFlaskServer()
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

    def test_flask_server_without_transform(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost"

        class IncompleteFlaskServer(FlaskServer):
            pass

        with self.assertRaises(TypeError) as context:
            IncompleteFlaskServer()  # pylint: disable=abstract-class-instantiated
        self.assertIn("Can't instantiate abstract class", str(context.exception))
