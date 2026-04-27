# pylint: disable=too-many-lines

import os
import sys
import io
import unittest
from urllib.parse import quote as url_quote
from unittest import mock
from unittest.mock import MagicMock, patch, AsyncMock

import requests
import httpx
from fastapi.testclient import TestClient
from flask.testing import FlaskClient

from aistore.sdk.const import (
    HEADER_NODE_URL,
    HEADER_CONTENT_LENGTH,
    ETL_WS_FQN,
    ETL_WS_PIPELINE,
    HEADER_DIRECT_PUT_LENGTH,
    QPARAM_ETL_FQN,
    AIS_DIRECT_PUT_RETRIES,
)
from aistore.sdk.etl.webserver.base_etl_server import (
    CountingIterator,
    _is_connection_refused,
)
from aistore.sdk.errors import ETLDirectPutTransientError
from aistore.sdk.etl.webserver.http_multi_threaded_server import HTTPMultiThreadedServer
from aistore.sdk.etl.webserver.flask_server import FlaskServer
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer


class DummyHTTPETLServer(HTTPMultiThreadedServer):
    """Dummy ETL server for testing transform and MIME type override."""

    def transform(self, data: bytes, *_args) -> bytes:
        return data.upper()

    def get_mime_type(self) -> str:
        return "text/caps"


class DummyRequestHandler(HTTPMultiThreadedServer.RequestHandler):
    """Fake handler that mocks HTTP methods for isolated testing of `set_headers()`."""

    # pylint: disable=super-init-not-called, too-many-instance-attributes
    def __init__(self):
        # Don't call super().__init__(), just mock the necessary parts
        self.path = "/test/object"
        self.rfile = io.BytesIO(b"test input")
        self.wfile = io.BytesIO()
        self.headers = {}

        self.send_response = MagicMock()
        self.send_header = MagicMock()
        self.end_headers = MagicMock()
        self.send_error = MagicMock()
        # self._direct_put = MagicMock()

        self.server = MagicMock()
        self.server.etl_server = MagicMock()
        self.server.etl_server.host_target = "http://localhost:8080"
        self.server.etl_server.get_mime_type.return_value = "application/test"
        self.server.etl_server.transform.return_value = b"transformed"
        self.server.etl_server.use_streaming = False

        s = DummyHTTPETLServer()
        self.server.etl_server.handle_direct_put_response = s.handle_direct_put_response

    def set_headers(self, status_code: int = 200):
        self.send_response(status_code)
        self.send_header("Content-Type", self.server.etl_server.get_mime_type())
        self.end_headers()


class DummyFastAPIServer(FastAPIServer):
    def transform(self, data: bytes, _path: str, _etl_args: str) -> bytes:
        return data[::-1]

    def get_mime_type(self) -> str:
        return "application/test"


class DummyFlaskServer(FlaskServer):
    def transform(self, data: bytes, _path: str, etl_args: str) -> bytes:
        return b"flask: " + data + etl_args.encode()

    def get_mime_type(self) -> str:
        return "application/flask"


class TestETLServerLogic(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.etl = DummyHTTPETLServer()

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
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"

    def test_set_headers_calls_expected_methods(self):
        """Ensure set_headers sets correct status and content-type."""
        handler = DummyRequestHandler()
        handler.set_headers(202)

        handler.send_response.assert_called_once_with(202)
        handler.send_header.assert_called_once_with("Content-Type", "application/test")
        handler.end_headers.assert_called_once()

    def test_transform_get(self):
        handler = DummyRequestHandler()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"original"
        handler.server.etl_server.session.get.return_value = mock_resp

        handler.do_GET()

        handler.server.etl_server.session.get.assert_called_once()
        handler.server.etl_server.transform.assert_called_with(
            b"original", "/test/object", ""
        )
        self.assertIn(b"transformed", handler.wfile.getvalue())

    def test_transform_put(self):
        handler = DummyRequestHandler()
        handler.headers = {"Content-Length": "10"}
        handler.rfile = io.BytesIO(b"1234567890")

        handler.do_PUT()

        handler.server.etl_server.transform.assert_called_with(
            b"1234567890", "/test/object", ""
        )
        self.assertIn(b"transformed", handler.wfile.getvalue())

    def test_transform_get_with_direct_put(self):
        direct_put_url = "http://some-target/put/object"
        handler = DummyRequestHandler()
        handler.headers = {HEADER_NODE_URL: direct_put_url}

        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.content = b"original"
        handler.server.etl_server.session.get.return_value = mock_get_resp

        # Simulate direct put success (200)
        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 200
        mock_put_resp.content = b""
        handler.server.etl_server.client_put.return_value = mock_put_resp
        handler.do_GET()
        handler.server.etl_server.client_put.assert_called_with(
            direct_put_url, b"transformed", headers={}
        )
        handler.send_response.assert_called_with(204)
        handler.send_header.assert_called_with(
            HEADER_DIRECT_PUT_LENGTH, str(len(b"transformed"))
        )
        self.assertEqual(handler.wfile.getvalue(), b"")

        # Simulate direct put fail (500)
        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 500
        mock_put_resp.content = b"error message"
        handler.server.etl_server.client_put.return_value = mock_put_resp
        handler.do_GET()
        handler.server.etl_server.client_put.assert_called_with(
            direct_put_url, b"transformed", headers={}
        )
        handler.send_response.assert_called_with(500)
        self.assertEqual(handler.wfile.getvalue(), b"error message")

    def test_transform_put_with_direct_put(self):
        direct_put_url = "http://some-target/put/object"
        handler = DummyRequestHandler()
        handler.headers = {HEADER_NODE_URL: direct_put_url}

        # Simulate direct put success (200)
        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 200
        mock_put_resp.content = b""
        handler.server.etl_server.client_put.return_value = mock_put_resp
        handler.do_PUT()
        handler.server.etl_server.client_put.assert_called_with(
            direct_put_url, b"transformed", headers={}
        )
        handler.send_response.assert_called_with(204)
        handler.send_header.assert_called_with(
            HEADER_DIRECT_PUT_LENGTH, str(len(b"transformed"))
        )
        self.assertEqual(handler.wfile.getvalue(), b"")

        # Simulate direct put fail (500)
        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 500
        mock_put_resp.content = b"error message"
        handler.server.etl_server.client_put.return_value = mock_put_resp
        handler.do_PUT()
        handler.server.etl_server.client_put.assert_called_with(
            direct_put_url, b"transformed", headers={}
        )
        handler.send_response.assert_called_with(500)
        self.assertEqual(handler.wfile.getvalue(), b"error message")


class TestFastAPIServer(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["DIRECT_PUT"] = "false"
        self.etl_server = DummyFastAPIServer()
        self.client = TestClient(self.etl_server.app)

    def test_health_check(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"Running")

    # pylint: disable=protected-access
    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_get_network_content(self):
        path = "test/path?etl_args=arg"
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
        path = "test/object?etl_args=arg"
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
            websocket.send_json(data={}, mode="binary")
            websocket.send_bytes(original_data)
            result = websocket.receive_bytes()
            self.assertEqual(result, original_data[::-1])

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_startup_event_default_retries(self):
        """startup_event() defaults to 3 transport retries when AIS_DIRECT_PUT_RETRIES is not set."""
        os.environ.pop(AIS_DIRECT_PUT_RETRIES, None)
        with patch(
            "aistore.sdk.etl.webserver.fastapi_server.httpx.AsyncHTTPTransport"
        ) as mock_transport_cls:
            with patch("aistore.sdk.etl.webserver.fastapi_server.httpx.AsyncClient"):
                mock_transport_cls.return_value = MagicMock()
                await self.etl_server.startup_event()
                self.assertEqual(mock_transport_cls.call_args.kwargs["retries"], 3)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_startup_event_custom_retries(self):
        """startup_event() passes AIS_DIRECT_PUT_RETRIES value to AsyncHTTPTransport."""
        os.environ[AIS_DIRECT_PUT_RETRIES] = "7"
        try:
            with patch(
                "aistore.sdk.etl.webserver.fastapi_server.httpx.AsyncHTTPTransport"
            ) as mock_transport_cls:
                with patch(
                    "aistore.sdk.etl.webserver.fastapi_server.httpx.AsyncClient"
                ):
                    mock_transport_cls.return_value = MagicMock()
                    await self.etl_server.startup_event()
                    self.assertEqual(mock_transport_cls.call_args.kwargs["retries"], 7)
        finally:
            os.environ.pop(AIS_DIRECT_PUT_RETRIES, None)

    def test_direct_put_retries_default(self):
        """direct_put_retries field defaults to 3 when AIS_DIRECT_PUT_RETRIES is not set."""
        os.environ.pop(AIS_DIRECT_PUT_RETRIES, None)
        self.assertEqual(DummyFastAPIServer().direct_put_retries, 3)

    def test_direct_put_retries_custom(self):
        """direct_put_retries field reads AIS_DIRECT_PUT_RETRIES at construction time."""
        os.environ[AIS_DIRECT_PUT_RETRIES] = "7"
        try:
            self.assertEqual(DummyFastAPIServer().direct_put_retries, 7)
        finally:
            os.environ.pop(AIS_DIRECT_PUT_RETRIES, None)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_direct_put_raises_transient_error_on_connect_error(self):
        """_direct_put() raises ETLDirectPutTransientError on ConnectError for caller retry."""
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["DIRECT_PUT"] = "true"
        server = DummyFastAPIServer()
        mock_client = AsyncMock()
        mock_client.put.side_effect = httpx.ConnectError(
            "DNS resolution failed: EAI_AGAIN"
        )
        server.client = mock_client

        with self.assertRaises(ETLDirectPutTransientError) as ctx:
            await server._direct_put(  # pylint: disable=protected-access
                "http://localhost:8080/ais/@/dst/obj", b"hello"
            )

        mock_client.put.assert_awaited_once()
        self.assertIsInstance(ctx.exception.cause, httpx.ConnectError)


class TestFastAPIServerWithDirectPut(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["DIRECT_PUT"] = "true"
        self.etl_server = DummyFastAPIServer()
        self.client = TestClient(self.etl_server.app)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_hpush_with_direct_put(self):
        path = "test/object"
        input_content = b"input data"
        transformed_content = self.etl_server.transform(input_content, path, "")

        # Mock the direct delivery response (simulate 200 OK)
        mock_response_success = AsyncMock()
        mock_response_success.content = b""
        mock_response_success.status_code = 200
        self.etl_server.client = AsyncMock()
        self.etl_server.client.put.return_value = mock_response_success

        headers = {HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"}
        response = self.client.put(f"/{path}", content=input_content, headers=headers)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")  # No content returned
        self.assertEqual(
            response.headers.get(HEADER_DIRECT_PUT_LENGTH),
            str(len(transformed_content)),
        )
        self.etl_server.client.put.assert_awaited_once()

        # Mock the direct delivery response (simulate 500 FAIL)
        mock_response_fail = AsyncMock()
        mock_response_fail.status_code = 500
        mock_response_fail.content = b"error message"
        self.etl_server.client = AsyncMock()
        self.etl_server.client.put.return_value = mock_response_fail

        headers = {HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"}
        response = self.client.put(f"/{path}", content=input_content, headers=headers)

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.content, b"error message")
        self.etl_server.client.put.assert_awaited_once()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_hpush_with_direct_put_and_fqn(self):
        path = "test/object"
        fqn = "test@some%fqn"
        input_content = b"input data"
        transformed_content = self.etl_server.transform(input_content, path, "")

        # Mock the direct put response (simulate 200 OK)
        with patch.object(
            self.etl_server,
            "_get_fqn_content",
            AsyncMock(return_value=input_content),
        ) as get_fqn_mock:
            mock_response_success = AsyncMock()
            mock_response_success.content = b""
            mock_response_success.status_code = 200
            self.etl_server.client = AsyncMock()
            self.etl_server.client.put.return_value = mock_response_success

            headers = {
                HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"
            }
            params = {QPARAM_ETL_FQN: fqn}
            response = self.client.put(
                f"/{path}", content=input_content, headers=headers, params=params
            )

            self.assertEqual(response.status_code, 204)
            self.assertEqual(response.content, b"")  # No content returned
            self.assertEqual(
                response.headers.get(HEADER_DIRECT_PUT_LENGTH),
                str(len(transformed_content)),
            )
            self.etl_server.client.put.assert_awaited_once()
            get_fqn_mock.assert_called_once_with(fqn)

        # Mock the direct put response (simulate 500 FAIL)
        with patch.object(
            self.etl_server,
            "_get_fqn_content",
            AsyncMock(return_value=input_content),
        ) as get_fqn_mock:
            mock_response_fail = AsyncMock()
            mock_response_fail.status_code = 500
            mock_response_fail.content = b"error message"
            self.etl_server.client = AsyncMock()
            self.etl_server.client.put.return_value = mock_response_fail

            headers = {
                HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"
            }
            params = {QPARAM_ETL_FQN: fqn}
            response = self.client.put(
                f"/{path}", content=input_content, headers=headers, params=params
            )

            self.assertEqual(response.status_code, 500)
            self.assertEqual(response.content, b"error message")
            self.etl_server.client.put.assert_awaited_once()
            get_fqn_mock.assert_called_once_with(fqn)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_websocket_with_direct_put(self):
        input_data = b"testdata"
        direct_put_url = "http://localhost:8080/ais/@/etl_dst/final"

        # Mock the direct put response (simulate 200 OK) => return length as ACK
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_resp.content = b""
            mock_client.put.return_value = mock_resp

            with self.client.websocket_connect("/ws") as websocket:
                websocket.send_json(
                    data={
                        ETL_WS_PIPELINE: direct_put_url,
                    },
                    mode="binary",
                )
                websocket.send_bytes(input_data)
                result = websocket.receive_text()
                self.assertEqual(
                    result, str(len(input_data))
                )  # Expecting length of input data as ACK

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url,
                content=mock.ANY,
                headers={HEADER_CONTENT_LENGTH: str(len(input_data[::-1]))},
            )

        # Mock the direct put response (simulate 500 FAIL) => return transformed data
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 500
            mock_resp.content = b"error message"
            mock_client.put.return_value = mock_resp

            with self.client.websocket_connect("/ws") as websocket:
                websocket.send_json(
                    data={
                        ETL_WS_PIPELINE: direct_put_url,
                    },
                    mode="binary",
                )
                websocket.send_bytes(input_data)
                error_msg = websocket.receive_text()
                self.assertEqual(
                    error_msg, str(0)
                )  # 0 length indicating websocket received an error

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url,
                content=mock.ANY,
                headers={HEADER_CONTENT_LENGTH: str(len(input_data[::-1]))},
            )

        # Mock the empty direct put url (don't need direct put on this object) => return transformed data
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            with self.client.websocket_connect("/ws") as websocket:
                websocket.send_json(data={}, mode="binary")
                websocket.send_bytes(input_data)
                result = websocket.receive_bytes()
                self.assertEqual(result, input_data[::-1])
            mock_client.put.assert_not_called()  # direct put shouldn't be called

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_websocket_with_direct_put_and_fqn(self):
        fqn = "test/object"
        original_content = b"original data"
        direct_put_url = "http://localhost:8080/ais/@/etl_dst/final"
        transformed_content = self.etl_server.transform(original_content, fqn, "")
        # Mock the direct put response (simulate 200 OK)
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_resp.content = b""
            mock_client.put.return_value = mock_resp

            with patch.object(
                self.etl_server,
                "_get_fqn_content",
                AsyncMock(return_value=original_content),
            ) as get_fqn_mock:
                with self.client.websocket_connect("/ws") as websocket:
                    websocket.send_json(
                        data={
                            ETL_WS_PIPELINE: direct_put_url,
                            ETL_WS_FQN: fqn,
                        },
                        mode="binary",
                    )
                    result = websocket.receive_text()
                    self.assertEqual(
                        result, str(len(transformed_content))
                    )  # Expecting length of original content as ACK

                get_fqn_mock.assert_called_once_with(fqn)

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url,
                content=mock.ANY,
                headers={HEADER_CONTENT_LENGTH: str(len(transformed_content))},
            )

        # Mock the direct put response (simulate 500 FAIL) => return transformed data
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 500
            mock_resp.content = b"error message"
            mock_client.put.return_value = mock_resp

            with patch.object(
                self.etl_server,
                "_get_fqn_content",
                AsyncMock(return_value=original_content),
            ) as get_fqn_mock:
                with self.client.websocket_connect("/ws") as websocket:
                    websocket.send_json(
                        data={
                            ETL_WS_PIPELINE: direct_put_url,
                            ETL_WS_FQN: fqn,
                        },
                        mode="binary",
                    )
                    result = websocket.receive_text()
                    self.assertEqual(
                        result, str(0)
                    )  # 0 length indicating websocket received an error

                get_fqn_mock.assert_called_once_with(fqn)

            mock_client.put.assert_awaited_once()
            mock_client.put.assert_called_once_with(
                direct_put_url,
                content=mock.ANY,
                headers={HEADER_CONTENT_LENGTH: str(len(transformed_content))},
            )

        # Mock the empty direct put url (don't need direct put on this object) => return transformed object
        with patch.object(self.etl_server, "client", new=AsyncMock()) as mock_client:
            with patch.object(
                self.etl_server,
                "_get_fqn_content",
                AsyncMock(return_value=original_content),
            ) as get_fqn_mock:
                with self.client.websocket_connect("/ws") as websocket:
                    websocket.send_json(
                        data={
                            ETL_WS_PIPELINE: "",
                            ETL_WS_FQN: fqn,
                        },
                        mode="binary",
                    )
                    result = websocket.receive_bytes()
                    self.assertEqual(result, transformed_content)

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
        response = self.client.put("/some/key?etl_args=arg", data=input_data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"flask: " + input_data + b"arg")
        assert "Content-Length" in response.headers
        assert int(response.headers["Content-Length"]) == len(response.data)

    def test_transform_get(self):
        input_data = b"flask get data"
        path = "/some/key?etl_args=arg"
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = MagicMock(
                content=input_data, raise_for_status=MagicMock()
            )
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, b"flask: " + input_data + b"arg")
            assert "Content-Length" in response.headers
            assert int(response.headers["Content-Length"]) == len(response.data)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_direct_put_delivery(self):
        path = "test/object"
        input_content = b"input data"
        transformed_content = self.etl_server.transform(input_content, path, "")
        headers = {HEADER_NODE_URL: "http://localhost:8080/ais/@/etl_dst/test/object"}

        with patch("requests.Session.put") as mock_put:
            # Mock the direct delivery response (simulate 200 OK)
            mock_put.return_value = MagicMock(status_code=200, content=b"")
            response = self.client.put(f"/{path}", data=input_content, headers=headers)

            self.assertEqual(response.status_code, 204)
            self.assertEqual(
                response.headers.get(HEADER_DIRECT_PUT_LENGTH),
                str(len(transformed_content)),
            )
            self.assertEqual(response.data, b"")  # No content returned

        with patch("requests.Session.put") as mock_put:
            # Mock the direct delivery response (simulate 500 FAIL)
            mock_put.return_value = MagicMock(status_code=500, content=b"error message")
            response = self.client.put(f"/{path}", data=input_content, headers=headers)

            self.assertEqual(response.status_code, 500)
            self.assertEqual(response.data, b"error message")


class TestBaseEnforcement(unittest.TestCase):
    def test_fastapi_server_without_target_url(self):
        if "AIS_TARGET_URL" in os.environ:
            del os.environ["AIS_TARGET_URL"]

        class MinimalFastAPIServer(FastAPIServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return data

        with self.assertRaises(EnvironmentError) as context:
            MinimalFastAPIServer()
        self.assertIn("AIS_TARGET_URL", str(context.exception))

    def test_flask_server_without_target_url(self):
        if "AIS_TARGET_URL" in os.environ:
            del os.environ["AIS_TARGET_URL"]

        class MinimalFlaskServer(FlaskServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return data

        with self.assertRaises(EnvironmentError) as context:
            MinimalFlaskServer()
        self.assertIn("AIS_TARGET_URL", str(context.exception))

    def test_http_server_server_without_target_url(self):
        if "AIS_TARGET_URL" in os.environ:
            del os.environ["AIS_TARGET_URL"]

        class MinimalHTTPServer(HTTPMultiThreadedServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return data

        with self.assertRaises(EnvironmentError) as context:
            MinimalHTTPServer()
        self.assertIn("AIS_TARGET_URL", str(context.exception))

    def test_http_multithreaded_server_without_transform(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost"

        class IncompleteETLServer(HTTPMultiThreadedServer):
            pass

        with self.assertRaises(TypeError) as context:
            IncompleteETLServer()
        self.assertIn(
            "must override either transform() or transform_stream()",
            str(context.exception),
        )

    def test_fastapi_server_without_transform(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost"

        class IncompleteFastAPIServer(FastAPIServer):
            pass

        with self.assertRaises(TypeError) as context:
            IncompleteFastAPIServer()
        self.assertIn(
            "must override either transform() or transform_stream()",
            str(context.exception),
        )

    def test_flask_server_without_transform(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost"

        class IncompleteFlaskServer(FlaskServer):
            pass

        with self.assertRaises(TypeError) as context:
            IncompleteFlaskServer()
        self.assertIn(
            "must override either transform() or transform_stream()",
            str(context.exception),
        )


class TestFastAPIServerETLArgs(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["DIRECT_PUT"] = "false"

        class ArgFastAPI(FastAPIServer):
            def transform(self, _data: bytes, _path: str, etl_args: str) -> bytes:
                return etl_args.encode()

        self.etl_server = ArgFastAPI()
        self.client = TestClient(self.etl_server.app)

    # pylint: disable=protected-access
    async def test_get_with_etl_args(self):
        path = "test/path?etl_args=arg"

        with patch.object(self.etl_server, "client", AsyncMock()) as mock_client:
            mock_response = AsyncMock()
            mock_response.content = b"arg"
            mock_response.raise_for_status = MagicMock()

            mock_client.get.return_value = mock_response

            result = await self.etl_server._get_network_content(path)

            self.assertEqual(result, b"arg")
            mock_client.get.assert_called_once()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_put_with_etl_args(self):
        path = "test/object?etl_args=arg"
        input_content = b"input data"
        transformed_content = b"arg"

        response = self.client.put(f"/{path}", content=input_content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, transformed_content)


# ---------------------------------------------------------------------------
# ETL_DIRECT_FQN tests — verify that transform() receives str (path) when the
# env var is set and FQN is provided, and bytes otherwise.
# ---------------------------------------------------------------------------


class _CapturingServer:  # pylint: disable=too-few-public-methods
    """Mixin that records the type and value of the last `data` arg to transform()."""

    last_data = None

    def transform(self, data, _path, _etl_args):
        type(self).last_data = data
        # Return bytes for any input so the response pipeline stays happy.
        return data.encode() if isinstance(data, str) else data


class CapturingFastAPIServer(_CapturingServer, FastAPIServer):
    pass


class CapturingFlaskServer(_CapturingServer, FlaskServer):
    pass


class CapturingHTTPServer(_CapturingServer, HTTPMultiThreadedServer):
    pass


class TestFastAPIDirectFQN(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["ETL_DIRECT_FQN"] = "true"
        self.etl_server = CapturingFastAPIServer()
        self.client = TestClient(self.etl_server.app)

    def tearDown(self):
        os.environ.pop("ETL_DIRECT_FQN", None)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_put_with_fqn_receives_path_string(self):
        """ETL_DIRECT_FQN=true: transform() receives the sanitized file path as str."""
        fqn = "/local/data/object.bin"
        self.client.put("/test/object", content=b"", params={QPARAM_ETL_FQN: fqn})
        self.assertIsInstance(CapturingFastAPIServer.last_data, str)
        self.assertEqual(CapturingFastAPIServer.last_data, os.path.normpath(fqn))

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_put_without_fqn_receives_bytes(self):
        """ETL_DIRECT_FQN=true but no FQN (pipeline stage): transform() receives bytes."""
        input_data = b"pipeline bytes"
        self.client.put("/test/object", content=input_data)
        self.assertIsInstance(CapturingFastAPIServer.last_data, bytes)
        self.assertEqual(CapturingFastAPIServer.last_data, input_data)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_websocket_with_fqn_receives_path_string(self):
        """ETL_DIRECT_FQN=true via WebSocket: transform() receives the file path as str."""
        fqn = "/local/data/object.bin"
        with self.client.websocket_connect("/ws") as ws:
            ws.send_json(data={ETL_WS_FQN: fqn}, mode="binary")
            ws.receive_bytes()
        self.assertIsInstance(CapturingFastAPIServer.last_data, str)
        self.assertEqual(CapturingFastAPIServer.last_data, os.path.normpath(fqn))

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_websocket_without_fqn_receives_bytes(self):
        """ETL_DIRECT_FQN=true via WebSocket but no FQN: transform() receives bytes."""
        input_data = b"ws bytes"
        with self.client.websocket_connect("/ws") as ws:
            ws.send_json(data={}, mode="binary")
            ws.send_bytes(input_data)
            ws.receive_bytes()
        self.assertIsInstance(CapturingFastAPIServer.last_data, bytes)
        self.assertEqual(CapturingFastAPIServer.last_data, input_data)


class TestFlaskDirectFQN(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["ETL_DIRECT_FQN"] = "true"
        self.etl_server = CapturingFlaskServer()
        self.client = self.etl_server.app.test_client()

    def tearDown(self):
        os.environ.pop("ETL_DIRECT_FQN", None)

    def test_put_with_fqn_receives_path_string(self):
        """ETL_DIRECT_FQN=true: Flask transform() receives the file path as str."""
        fqn = "/local/data/object.bin"
        self.client.put("/test/object", data=b"", query_string={QPARAM_ETL_FQN: fqn})
        self.assertIsInstance(CapturingFlaskServer.last_data, str)
        self.assertEqual(CapturingFlaskServer.last_data, os.path.normpath(fqn))

    def test_put_without_fqn_receives_bytes(self):
        """ETL_DIRECT_FQN=true but no FQN: Flask transform() receives bytes."""
        input_data = b"pipeline bytes"
        self.client.put("/test/object", data=input_data)
        self.assertIsInstance(CapturingFlaskServer.last_data, bytes)
        self.assertEqual(CapturingFlaskServer.last_data, input_data)

    def test_get_with_fqn_receives_path_string(self):
        """ETL_DIRECT_FQN=true GET: Flask transform() receives the file path as str."""
        fqn = "/local/data/object.bin"
        self.client.get("/test/object", query_string={QPARAM_ETL_FQN: fqn})
        self.assertIsInstance(CapturingFlaskServer.last_data, str)
        self.assertEqual(CapturingFlaskServer.last_data, os.path.normpath(fqn))


class TestHTTPDirectFQN(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["ETL_DIRECT_FQN"] = "true"

    def tearDown(self):
        os.environ.pop("ETL_DIRECT_FQN", None)

    def _make_handler(self):
        handler = DummyRequestHandler()
        handler.server.etl_server.direct_fqn = True
        handler.server.etl_server.sanitize_fqn.side_effect = (
            lambda fqn: os.path.normpath(os.path.join("/", fqn.lstrip("/")))
        )
        return handler

    def test_put_with_fqn_receives_path_string(self):
        """ETL_DIRECT_FQN=true: HTTP transform() receives the file path as str."""
        fqn = "/local/data/object.bin"
        handler = self._make_handler()
        handler.path = f"/test/object?{QPARAM_ETL_FQN}={fqn}"
        handler.headers = {"Content-Length": "0"}
        handler.rfile = io.BytesIO(b"")
        handler.do_PUT()
        handler.server.etl_server.transform.assert_called_with(
            os.path.normpath(fqn), "/test/object", ""
        )

    def test_put_without_fqn_receives_bytes(self):
        """ETL_DIRECT_FQN=true but no FQN: HTTP transform() receives bytes."""
        input_data = b"pipeline bytes"
        handler = self._make_handler()
        handler.path = "/test/object"
        handler.headers = {"Content-Length": str(len(input_data))}
        handler.rfile = io.BytesIO(input_data)
        handler.do_PUT()
        handler.server.etl_server.transform.assert_called_with(
            input_data, "/test/object", ""
        )

    @patch("requests.get")
    def test_get_with_fqn_receives_path_string(self, _mock_get):
        """ETL_DIRECT_FQN=true GET: HTTP transform() receives the file path as str."""
        fqn = "/local/data/object.bin"
        handler = self._make_handler()
        handler.path = f"/test/object?{QPARAM_ETL_FQN}={fqn}"
        handler.do_GET()
        handler.server.etl_server.transform.assert_called_with(
            os.path.normpath(fqn), "/test/object", ""
        )


# ---------------------------------------------------------------------------
# Streaming transform tests
# ---------------------------------------------------------------------------


class DummyStreamingFastAPIServer(FastAPIServer):
    """FastAPI server that uses transform_stream instead of transform."""

    def transform_stream(self, reader, _path, _etl_args):
        data = reader.read()
        yield data[::-1]

    def get_mime_type(self):
        return "application/test"


class TestStreamingDetection(unittest.TestCase):
    """Test that use_streaming detection works correctly."""

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"

    def test_stream_only_uses_streaming(self):
        server = DummyStreamingFastAPIServer()
        self.assertTrue(server.use_streaming)

    def test_transform_only_uses_buffered(self):
        server = DummyFastAPIServer()
        self.assertFalse(server.use_streaming)

    def test_both_overridden_prefers_transform(self):
        class BothServer(FastAPIServer):
            def transform(self, data, _path, _etl_args):
                return data

            def transform_stream(self, reader, _path, _etl_args):
                yield reader.read()

        server = BothServer()
        self.assertFalse(server.use_streaming)

    def test_neither_overridden_raises(self):
        class EmptyServer(FastAPIServer):
            pass

        with self.assertRaises(TypeError) as ctx:
            EmptyServer()
        self.assertIn(
            "must override either transform() or transform_stream()", str(ctx.exception)
        )


class TestStreamingFastAPIServer(unittest.IsolatedAsyncioTestCase):
    """Sync def tests drive the app through FastAPI's sync TestClient / websocket_connect.
    Async def tests directly await async server helpers (e.g. _get_stream_reader)."""

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["DIRECT_PUT"] = "false"
        self.etl_server = DummyStreamingFastAPIServer()
        self.client = TestClient(self.etl_server.app)

    def test_health_check(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"Running")

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_put(self):
        # TODO: non-FQN PUT body is still buffered; "streaming PUT" refers to the
        # transform_stream path, not request-body streaming.
        input_content = b"input data"
        response = self.client.put("/test/object", content=input_content)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, input_content[::-1])

    def _make_mock_session_get(self, raw_data=b"", status_code=200, side_effect=None):
        """Return a mock requests.Response with .raw configured for streaming."""
        mock_raw = MagicMock()
        mock_raw.read.side_effect = [raw_data, b""]  # one chunk then EOF
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.raw = mock_raw
        if side_effect:
            mock_resp.raise_for_status.side_effect = side_effect
        else:
            mock_resp.raise_for_status = MagicMock()
        return mock_resp

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_get(self):
        """No-FQN GET streams via the shared sync session; output is transformed."""
        input_data = b"streaming get data"
        mock_resp = self._make_mock_session_get(raw_data=input_data)

        with patch.object(
            self.etl_server.session, "get", return_value=mock_resp
        ) as mock_get:
            response = self.client.get("/test/object")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, input_data[::-1])
        mock_get.assert_called_once_with(
            "http://localhost:8080/test%2Fobject", stream=True, timeout=None
        )

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_get_does_not_buffer(self):
        """Streaming GET must not use _get_network_content (the buffering path)."""
        mock_resp = self._make_mock_session_get(raw_data=b"data")

        with patch.object(self.etl_server.session, "get", return_value=mock_resp):
            with patch.object(
                self.etl_server, "_get_network_content", new_callable=AsyncMock
            ) as mock_net:
                self.client.get("/test/object")
                mock_net.assert_not_called()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_get_forwards_upstream_error_status(self):
        """Upstream HTTP errors must be forwarded, not collapsed into 502."""
        mock_requests_resp = MagicMock()
        mock_requests_resp.status_code = 404
        err = requests.HTTPError(response=mock_requests_resp)
        mock_resp = self._make_mock_session_get(status_code=404, side_effect=err)

        with patch.object(self.etl_server.session, "get", return_value=mock_resp):
            response = self.client.get("/test/object")

        self.assertEqual(response.status_code, 404)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_get_network_error_returns_502(self):
        """Network-level failures on GET must return 502 Bad Gateway."""
        with patch.object(
            self.etl_server.session,
            "get",
            side_effect=requests.ConnectionError("refused"),
        ):
            response = self.client.get("/test/object")

        self.assertEqual(response.status_code, 502)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_get_closes_response_on_upstream_error(self):
        """Upstream errors must close the response to release the connection."""
        mock_requests_resp = MagicMock()
        mock_requests_resp.status_code = 404
        err = requests.HTTPError(response=mock_requests_resp)
        mock_resp = self._make_mock_session_get(status_code=404, side_effect=err)

        with patch.object(self.etl_server.session, "get", return_value=mock_resp):
            response = self.client.get("/test/object")

        self.assertEqual(response.status_code, 404)
        mock_resp.close.assert_called_once()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_get_upstream_500_forwarded_and_closes(self):
        """Upstream 500 is forwarded as-is and the response is closed to avoid leaks."""
        mock_requests_resp = MagicMock()
        mock_requests_resp.status_code = 500
        err = requests.HTTPError(response=mock_requests_resp)
        mock_resp = self._make_mock_session_get(status_code=500, side_effect=err)

        with patch.object(self.etl_server.session, "get", return_value=mock_resp):
            response = self.client.get("/test/object")

        self.assertEqual(response.status_code, 500)
        mock_resp.close.assert_called_once()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_get_uname_path_preserved(self):
        """Uname path with @, slashes, spaces, and non-ASCII is forwarded byte-exact.

        Guards against any future re-introduction of bucket/object path parsing.
        The ETL target contract requires quote(path, safe='@') appended verbatim
        to host_target — not a /v1/objects/... rewrite.
        """
        uname_path = "ais/@ns/mybucket/path with ñ/obj"
        expected_url = f"http://localhost:8080/{url_quote(uname_path, safe='@')}"

        mock_resp = self._make_mock_session_get(raw_data=b"data")
        with patch.object(
            self.etl_server.session, "get", return_value=mock_resp
        ) as mock_get:
            srv = self.etl_server
            get_reader = srv._get_stream_reader  # pylint: disable=protected-access
            reader = await get_reader(
                fqn="", path=uname_path, request=None, is_get=True
            )
            reader.close()

        mock_get.assert_called_once_with(expected_url, stream=True, timeout=None)
        # No SDK-style path rewriting should occur.
        self.assertNotIn("/v1/objects/", mock_get.call_args[0][0])

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_get_reader_returns_binary_io(self):
        """_get_stream_reader returns a sync BinaryIO whose read() yields upstream bytes."""
        upstream_bytes = b"hello from upstream"
        mock_resp = self._make_mock_session_get(raw_data=upstream_bytes)

        with patch.object(self.etl_server.session, "get", return_value=mock_resp):
            srv = self.etl_server
            get_reader = srv._get_stream_reader  # pylint: disable=protected-access
            reader = await get_reader(
                fqn="", path="bucket/obj", request=None, is_get=True
            )

        self.assertTrue(hasattr(reader, "read"))
        self.assertTrue(hasattr(reader, "close"))
        self.assertEqual(reader.read(), upstream_bytes)
        reader.close()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_get_reader_close_releases_connection_pool(self):
        """reader.close() must call resp.close(), not just resp.raw.close().

        resp.close() calls release_conn() which returns the socket to the
        keep-alive pool. Closing only raw skips that and leaks connections on
        early-close (mid-stream errors, client disconnect).
        """
        mock_resp = self._make_mock_session_get(raw_data=b"bytes")

        with patch.object(self.etl_server.session, "get", return_value=mock_resp):
            srv = self.etl_server
            reader = await srv._get_stream_reader(  # pylint: disable=protected-access
                fqn="", path="bucket/obj", request=None, is_get=True
            )

        reader.close()
        mock_resp.close.assert_called_once()
        mock_resp.raw.close.assert_not_called()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_streaming_put_no_pipeline_round_trip(self):
        """Non-pipeline streaming PUT round-trips correctly."""
        # TODO: non-FQN PUT body is still buffered; "streaming PUT" refers to the
        # transform_stream path, not request-body streaming.
        input_content = b"streaming put no-pipeline"
        resp = self.client.put("/test/object", content=input_content)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content, input_content[::-1])

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_websocket_raises_for_streaming_server(self):
        """WebSocket should fail for streaming-only servers."""
        with self.assertRaises(Exception):
            with self.client.websocket_connect("/ws") as websocket:
                websocket.send_json(data={}, mode="binary")
                websocket.send_bytes(b"test")
                websocket.receive_bytes()


class TestCountingIterator(unittest.TestCase):
    def test_counts_bytes(self):
        data = [b"hello", b" ", b"world"]
        counted = CountingIterator(iter(data))
        result = b"".join(counted)
        self.assertEqual(result, b"hello world")
        self.assertEqual(counted.bytes_sent, 11)

    def test_empty_iterator(self):
        counted = CountingIterator(iter([]))
        result = b"".join(counted)
        self.assertEqual(result, b"")
        self.assertEqual(counted.bytes_sent, 0)


# ---------------------------------------------------------------------------
# Flask and HTTP streaming tests
# ---------------------------------------------------------------------------


class DummyStreamingFlaskServer(FlaskServer):
    """Flask server that uses transform_stream instead of transform."""

    def transform_stream(self, reader, _path, _etl_args):
        data = reader.read()
        yield b"flask: " + data

    def get_mime_type(self):
        return "application/flask"


class DummyStreamingHTTPServer(HTTPMultiThreadedServer):
    """HTTP server that uses transform_stream instead of transform."""

    def transform_stream(self, reader, _path, _etl_args):
        data = reader.read()
        yield data.upper()

    def get_mime_type(self):
        return "text/caps"


class TestStreamingFlaskServer(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost"
        self.etl_server = DummyStreamingFlaskServer()
        self.client = self.etl_server.app.test_client()

    def test_streaming_put(self):
        input_data = b"hello"
        response = self.client.put("/some/key", data=input_data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"flask: " + input_data)

    def test_streaming_get(self):
        input_data = b"flask get data"
        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.raise_for_status = MagicMock()
            mock_resp.raw = io.BytesIO(input_data)
            mock_get.return_value = mock_resp
            response = self.client.get("/some/key")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, b"flask: " + input_data)


class TestStreamingHTTPServer(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"

    def _make_streaming_handler(self):
        handler = DummyRequestHandler()
        handler.server.etl_server.use_streaming = True
        handler.server.etl_server.transform_stream.return_value = iter([b"TRANSFORMED"])
        handler.server.etl_server.close_reader = MagicMock()
        return handler

    def test_streaming_put(self):
        handler = self._make_streaming_handler()
        handler.headers = {HEADER_CONTENT_LENGTH: "10"}
        handler.rfile = io.BytesIO(b"test input")
        handler.do_PUT()
        handler.server.etl_server.transform_stream.assert_called_once()
        handler.server.etl_server.close_reader.assert_called_once()

    def test_streaming_get(self):
        handler = self._make_streaming_handler()
        handler.path = "/test/object"
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raw = io.BytesIO(b"target data")
        handler.server.etl_server.session.get.return_value = mock_resp
        handler.do_GET()
        handler.server.etl_server.transform_stream.assert_called_once()
        handler.server.etl_server.close_reader.assert_called_once()


# ---------------------------------------------------------------------------
# _direct_put_stream_with_retry unit tests
# ---------------------------------------------------------------------------


class TestStreamingDirectPutRetry(unittest.IsolatedAsyncioTestCase):
    """Unit tests for _direct_put_stream_with_retry: retry logic and reader lifecycle."""

    _DIRECT_PUT_URL = "http://target:51081/ais/@/dst/test/obj"

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["DIRECT_PUT"] = "true"
        self.server = DummyStreamingFastAPIServer()
        self.server.direct_put_retries = 3
        self.server.client = AsyncMock()

    def _make_request(self, body: bytes = b"input") -> MagicMock:
        req = MagicMock()
        req.body = AsyncMock(return_value=body)

        async def _stream():
            yield body

        req.stream.return_value = _stream()
        return req

    def _ok_response(self) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b""
        resp.headers = {}
        return resp

    async def _call(self, req, retries=None):
        if retries is not None:
            self.server.direct_put_retries = retries
        return await self.server._direct_put_stream_with_retry(  # pylint: disable=protected-access
            "", "test/obj", req, False, "", self._DIRECT_PUT_URL, ""
        )

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_succeeds_on_first_attempt(self):
        """No retry when first attempt succeeds."""
        self.server.client.put.return_value = self._ok_response()
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await self._call(self._make_request())
        # 200 + empty content → handle_direct_put_response returns 204
        self.assertEqual(result[0], 204)
        self.server.client.put.assert_awaited_once()
        mock_sleep.assert_not_called()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_retries_on_read_error_then_succeeds(self):
        """Retries on ReadError and succeeds on the third attempt."""
        call_count = 0

        async def mock_put(*_, **__):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ReadError("connection closed")
            return self._ok_response()

        self.server.client.put.side_effect = mock_put
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await self._call(self._make_request())
        self.assertEqual(result[0], 204)  # 200 + empty content → 204
        self.assertEqual(call_count, 3)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_raises_after_exhausting_retries(self):
        """ETLDirectPutTransientError is raised after all retries are exhausted."""
        self.server.client.put.side_effect = httpx.ConnectError("refused")
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(ETLDirectPutTransientError):
                await self._call(self._make_request(), retries=2)
        # initial attempt + 2 retries = 3 total
        self.assertEqual(self.server.client.put.await_count, 3)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_non_transient_error_not_retried(self):
        """A non-transient exception in client.put returns 500 without retrying."""
        self.server.client.put.side_effect = ValueError("unexpected")
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await self._call(self._make_request())
        self.assertEqual(result[0], 500)
        self.server.client.put.assert_awaited_once()
        mock_sleep.assert_not_called()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_get_reader_reopened_on_each_retry(self):
        """For GET (non-replayable), _get_stream_reader is called once per attempt."""
        call_count = 0

        async def mock_put(*_, **__):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise httpx.ReadError("dropped")
            return self._ok_response()

        self.server.client.put.side_effect = mock_put

        get_reader_calls = []

        async def tracking(*_args, **_kwargs):
            reader = MagicMock()
            reader.read.return_value = b"data"
            get_reader_calls.append(reader)
            return reader

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with patch.object(self.server, "_get_stream_reader", side_effect=tracking):
                # is_get=True → non-replayable, reader reopened on each retry
                await self.server._direct_put_stream_with_retry(  # pylint: disable=protected-access
                    "",
                    "test/obj",
                    self._make_request(),
                    True,
                    "",
                    self._DIRECT_PUT_URL,
                    "",
                )

        # initial + 2 retries = 3 readers opened
        self.assertEqual(len(get_reader_calls), 3)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_reader_always_closed_on_success(self):
        """close_reader is called exactly once (via finally) after success."""
        self.server.client.put.return_value = self._ok_response()
        with patch.object(
            self.server, "close_reader", wraps=self.server.close_reader
        ) as mc:
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await self._call(self._make_request())
        mc.assert_called_once()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_reader_always_closed_on_exhausted_retries(self):
        """Reader is closed after each failed attempt and again by the finally block."""
        self.server.client.put.side_effect = httpx.ReadError("dropped")

        close_calls = []
        original = self.server.close_reader

        def tracking(reader):
            close_calls.append(reader)
            return original(reader)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with patch.object(self.server, "close_reader", side_effect=tracking):
                with self.assertRaises(ETLDirectPutTransientError):
                    await self._call(self._make_request(), retries=2)

        # 2 retries -> 2 mid-loop closes + 1 finally close = 3 total
        self.assertEqual(len(close_calls), 3)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_exponential_backoff_delays(self):
        """asyncio.sleep is called with exponentially increasing delays."""
        call_count = 0

        async def mock_put(*_, **__):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise httpx.ReadError("dropped")
            return self._ok_response()

        self.server.client.put.side_effect = mock_put
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await self._call(self._make_request(), retries=3)

        delays = [call.args[0] for call in mock_sleep.call_args_list]
        self.assertEqual(delays, [1.0, 2.0, 4.0])  # 2**0, 2**1, 2**2


# ---------------------------------------------------------------------------
# Sync (Flask) direct-put retry unit tests
# ---------------------------------------------------------------------------


class TestFlaskDirectPutRetry(unittest.TestCase):
    """Unit tests for FlaskServer retry logic on buffered direct-put."""

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.server = DummyFlaskServer()
        self.server.direct_put_retries = 3

    def _retry(self, url="http://target/obj", data=b"data"):
        return self.server._direct_put_with_retry(  # pylint: disable=protected-access
            url, data
        )

    def _put(self, url="http://target/obj", data=b"data"):
        return self.server._direct_put(url, data)  # pylint: disable=protected-access

    def test_succeeds_on_first_attempt(self):
        """No retry when first attempt succeeds."""
        with patch.object(self.server, "_direct_put", return_value=(200, b"ok", 2)):
            with patch("time.sleep") as mock_sleep:
                result = self._retry()
        self.assertEqual(result, (200, b"ok", 2))
        mock_sleep.assert_not_called()

    def test_retries_on_transient_error_then_succeeds(self):
        """Retries on ETLDirectPutTransientError and succeeds on second attempt."""
        self.server.direct_put_retries = 2
        ok = (200, b"", 4)
        side_effects = [
            ETLDirectPutTransientError("http://target/obj", requests.ConnectionError()),
            ok,
        ]
        with patch.object(self.server, "_direct_put", side_effect=side_effects):
            with patch("time.sleep"):
                result = self._retry()
        self.assertEqual(result, ok)

    def test_raises_after_exhausting_retries(self):
        """ETLDirectPutTransientError is raised after all retries exhausted."""
        self.server.direct_put_retries = 2
        err = ETLDirectPutTransientError("url", requests.ConnectionError())
        with patch.object(self.server, "_direct_put", side_effect=err):
            with patch("time.sleep"):
                with self.assertRaises(ETLDirectPutTransientError):
                    self._retry()

    def test_direct_put_wraps_connection_error(self):
        """_direct_put() wraps requests.ConnectionError as ETLDirectPutTransientError."""
        with self.server.app.test_request_context():
            with patch.object(
                self.server, "client_put", side_effect=requests.ConnectionError("lost")
            ):
                with self.assertRaises(ETLDirectPutTransientError):
                    self._put()

    def test_direct_put_wraps_chunked_encoding_error(self):
        """_direct_put() wraps ChunkedEncodingError as ETLDirectPutTransientError."""
        with self.server.app.test_request_context():
            with patch.object(
                self.server,
                "client_put",
                side_effect=requests.exceptions.ChunkedEncodingError(),
            ):
                with self.assertRaises(ETLDirectPutTransientError):
                    self._put()

    def test_non_transient_error_returns_500(self):
        """Non-transient exceptions in _direct_put return 500 without retrying."""
        with patch.object(self.server, "_direct_put", return_value=(500, b"err", 0)):
            with patch("time.sleep") as mock_sleep:
                result = self._retry()
        self.assertEqual(result[0], 500)
        mock_sleep.assert_not_called()

    def test_exponential_backoff_delays(self):
        """time.sleep is called with exponentially increasing delays."""
        self.server.direct_put_retries = 3
        call_count = 0

        def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise ETLDirectPutTransientError("url", requests.ConnectionError())
            return (200, b"", 0)

        with patch.object(self.server, "_direct_put", side_effect=side_effect):
            with patch("time.sleep") as mock_sleep:
                self._retry()

        delays = [c.args[0] for c in mock_sleep.call_args_list]
        self.assertEqual(len(delays), 3)
        self.assertEqual(delays[0], min(2.0**0, 30.0))
        self.assertEqual(delays[1], min(2.0**1, 30.0))
        self.assertEqual(delays[2], min(2.0**2, 30.0))

    def test_zero_retries_raises_on_first_failure(self):
        """With direct_put_retries=0, no sleep and error raised immediately."""
        self.server.direct_put_retries = 0
        err = ETLDirectPutTransientError("url", requests.ConnectionError())
        with patch.object(self.server, "_direct_put", side_effect=err):
            with patch("time.sleep") as mock_sleep:
                with self.assertRaises(ETLDirectPutTransientError):
                    self._retry()
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# Sync (HTTP multi-threaded) direct-put retry unit tests
# ---------------------------------------------------------------------------


class TestHTTPDirectPutRetry(unittest.TestCase):
    """Unit tests for HTTPMultiThreadedServer.RequestHandler retry logic."""

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.handler = DummyRequestHandler()
        self.handler.server.etl_server.direct_put_retries = 3

    def _retry(self, url="http://target/obj", data=b"data"):
        return self.handler._direct_put_with_retry(  # pylint: disable=protected-access
            url, data
        )

    def _put(self, url="http://target/obj", data=b"data", path="/obj"):
        return self.handler._direct_put(  # pylint: disable=protected-access
            url, data, "", path
        )

    def test_succeeds_on_first_attempt(self):
        """No retry when first attempt succeeds."""
        with patch.object(self.handler, "_direct_put", return_value=(200, b"ok", 5)):
            with patch("time.sleep") as mock_sleep:
                result = self._retry()
        self.assertEqual(result, (200, b"ok", 5))
        mock_sleep.assert_not_called()

    def test_retries_on_transient_error_then_succeeds(self):
        """Retries on ETLDirectPutTransientError and succeeds on second attempt."""
        self.handler.server.etl_server.direct_put_retries = 2
        ok = (200, b"ok", 3)
        side_effects = [
            ETLDirectPutTransientError("url", requests.ConnectionError()),
            ok,
        ]
        with patch.object(self.handler, "_direct_put", side_effect=side_effects):
            with patch("time.sleep"):
                result = self._retry()
        self.assertEqual(result, ok)

    def test_raises_after_exhausting_retries(self):
        """ETLDirectPutTransientError is raised after all retries exhausted."""
        self.handler.server.etl_server.direct_put_retries = 2
        err = ETLDirectPutTransientError("url", requests.ConnectionError())
        with patch.object(self.handler, "_direct_put", side_effect=err):
            with patch("time.sleep"):
                with self.assertRaises(ETLDirectPutTransientError):
                    self._retry()

    def test_direct_put_wraps_connection_error(self):
        """_direct_put() wraps requests.ConnectionError as ETLDirectPutTransientError."""
        self.handler.server.etl_server.client_put = MagicMock(
            side_effect=requests.ConnectionError("lost")
        )
        with self.assertRaises(ETLDirectPutTransientError):
            self._put()

    def test_direct_put_wraps_chunked_encoding_error(self):
        """_direct_put() wraps ChunkedEncodingError as ETLDirectPutTransientError."""
        self.handler.server.etl_server.client_put = MagicMock(
            side_effect=requests.exceptions.ChunkedEncodingError()
        )
        with self.assertRaises(ETLDirectPutTransientError):
            self._put()

    def test_non_transient_error_returns_500(self):
        """Non-transient exceptions in _direct_put return 500 without retrying."""
        with patch.object(self.handler, "_direct_put", return_value=(500, b"err", 0)):
            with patch("time.sleep") as mock_sleep:
                result = self._retry()
        self.assertEqual(result[0], 500)
        mock_sleep.assert_not_called()

    def test_exponential_backoff_delays(self):
        """time.sleep is called with exponentially increasing delays."""
        self.handler.server.etl_server.direct_put_retries = 3
        call_count = 0

        def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise ETLDirectPutTransientError("url", requests.ConnectionError())
            return (200, b"", 0)

        with patch.object(self.handler, "_direct_put", side_effect=side_effect):
            with patch("time.sleep") as mock_sleep:
                self._retry()

        delays = [c.args[0] for c in mock_sleep.call_args_list]
        self.assertEqual(len(delays), 3)
        self.assertEqual(delays[0], min(2.0**0, 30.0))
        self.assertEqual(delays[1], min(2.0**1, 30.0))
        self.assertEqual(delays[2], min(2.0**2, 30.0))

    def test_zero_retries_raises_on_first_failure(self):
        """With direct_put_retries=0, no sleep and error raised immediately."""
        self.handler.server.etl_server.direct_put_retries = 0
        err = ETLDirectPutTransientError("url", requests.ConnectionError())
        with patch.object(self.handler, "_direct_put", side_effect=err):
            with patch("time.sleep") as mock_sleep:
                with self.assertRaises(ETLDirectPutTransientError):
                    self._retry()
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# Sync (Flask) streaming direct-put retry unit tests
# ---------------------------------------------------------------------------


class TestFlaskStreamingDirectPutRetry(unittest.TestCase):
    """Unit tests for FlaskServer._direct_put_stream_with_retry reader lifecycle and retry."""

    _DIRECT_PUT_URL = "http://target:51081/ais/@/dst/test/obj"

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.server = DummyStreamingFlaskServer()
        self.server.direct_put_retries = 3

    def _make_reader(self, data=b"input"):
        return io.BytesIO(data)

    def _call(self, path="test/obj"):
        return self.server._direct_put_stream_with_retry(  # pylint: disable=protected-access
            self._DIRECT_PUT_URL, path
        )

    def test_succeeds_on_first_attempt(self):
        """No retry when first attempt succeeds."""
        with patch.object(
            self.server, "_get_stream_reader", return_value=self._make_reader()
        ):
            with patch.object(
                self.server, "_direct_put_stream", return_value=(204, b"", 5)
            ):
                with patch("time.sleep") as mock_sleep:
                    result = self._call()
        self.assertEqual(result, (204, b"", 5))
        mock_sleep.assert_not_called()

    def test_retries_on_transient_error_then_succeeds(self):
        """Retries on ETLDirectPutTransientError and succeeds on second attempt."""
        self.server.direct_put_retries = 2
        ok = (204, b"", 4)
        call_count = 0

        def put_side(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ETLDirectPutTransientError(
                    self._DIRECT_PUT_URL, requests.ConnectionError()
                )
            return ok

        readers = [self._make_reader(), self._make_reader()]
        reader_iter = iter(readers)
        with patch.object(
            self.server,
            "_get_stream_reader",
            side_effect=lambda *_, **__: next(reader_iter),
        ):
            with patch.object(self.server, "_direct_put_stream", side_effect=put_side):
                with patch("time.sleep"):
                    result = self._call()
        self.assertEqual(result, ok)

    def test_raises_after_exhausting_retries(self):
        """ETLDirectPutTransientError raised after all retries exhausted."""
        self.server.direct_put_retries = 2
        err = ETLDirectPutTransientError(
            self._DIRECT_PUT_URL, requests.ConnectionError()
        )
        readers = [self._make_reader() for _ in range(3)]
        reader_iter = iter(readers)
        with patch.object(
            self.server,
            "_get_stream_reader",
            side_effect=lambda *_, **__: next(reader_iter),
        ):
            with patch.object(self.server, "_direct_put_stream", side_effect=err):
                with patch("time.sleep"):
                    with self.assertRaises(ETLDirectPutTransientError):
                        self._call()

    def test_reader_reopened_on_each_retry(self):
        """_get_stream_reader is called once per attempt (initial + each retry)."""
        self.server.direct_put_retries = 2
        call_count = 0

        def put_side(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ETLDirectPutTransientError(
                    self._DIRECT_PUT_URL, requests.ConnectionError()
                )
            return (204, b"", 0)

        readers = [self._make_reader() for _ in range(3)]
        reader_iter = iter(readers)
        with patch.object(
            self.server,
            "_get_stream_reader",
            side_effect=lambda *_, **__: next(reader_iter),
        ) as mock_reader:
            with patch.object(self.server, "_direct_put_stream", side_effect=put_side):
                with patch("time.sleep"):
                    self._call()
        # initial + 2 retries = 3 total reader opens
        self.assertEqual(mock_reader.call_count, 3)

    def test_reader_always_closed_on_success(self):
        """close_reader is called exactly once (via finally) after success."""
        mock_reader = self._make_reader()
        with patch.object(self.server, "_get_stream_reader", return_value=mock_reader):
            with patch.object(
                self.server, "_direct_put_stream", return_value=(204, b"", 0)
            ):
                with patch.object(
                    self.server, "close_reader", wraps=self.server.close_reader
                ) as mc:
                    self._call()
        mc.assert_called_once()

    def test_reader_always_closed_on_exhausted_retries(self):
        """All readers opened during retries are closed; finally closes the last one."""
        self.server.direct_put_retries = 2
        err = ETLDirectPutTransientError(
            self._DIRECT_PUT_URL, requests.ConnectionError()
        )
        readers = [self._make_reader() for _ in range(3)]
        reader_iter = iter(readers)
        close_calls = []
        original_close = self.server.close_reader

        with patch.object(
            self.server,
            "_get_stream_reader",
            side_effect=lambda *_, **__: next(reader_iter),
        ):
            with patch.object(self.server, "_direct_put_stream", side_effect=err):
                with patch.object(
                    self.server,
                    "close_reader",
                    side_effect=lambda r: close_calls.append(r) or original_close(r),
                ):
                    with patch("time.sleep"):
                        with self.assertRaises(ETLDirectPutTransientError):
                            self._call()
        # 2 retries × close in except + 1 finally close = 3 total
        self.assertEqual(len(close_calls), 3)

    def test_exponential_backoff_delays(self):
        """time.sleep is called with exponentially increasing delays."""
        self.server.direct_put_retries = 3
        call_count = 0

        def put_side(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise ETLDirectPutTransientError(
                    self._DIRECT_PUT_URL, requests.ConnectionError()
                )
            return (204, b"", 0)

        readers = [self._make_reader() for _ in range(4)]
        reader_iter = iter(readers)
        with patch.object(
            self.server,
            "_get_stream_reader",
            side_effect=lambda *_, **__: next(reader_iter),
        ):
            with patch.object(self.server, "_direct_put_stream", side_effect=put_side):
                with patch("time.sleep") as mock_sleep:
                    self._call()

        delays = [c.args[0] for c in mock_sleep.call_args_list]
        self.assertEqual(len(delays), 3)
        self.assertEqual(delays[0], min(2.0**0, 30.0))
        self.assertEqual(delays[1], min(2.0**1, 30.0))
        self.assertEqual(delays[2], min(2.0**2, 30.0))

    def test_put_no_fqn_retry_sends_original_bytes(self):
        """Regression: Flask streaming PUT without FQN replays original body on retry.

        Before the fix, _get_stream_reader returned request.stream which is exhausted
        after the first read. A second call to _get_stream_reader would return an empty
        stream, causing the retry to PUT zero bytes. After the fix, get_data() buffers
        the body so each retry gets a fresh BytesIO over the original bytes.
        """
        original_body = b"hello regression bytes"
        bytes_read_by_transform = []

        def spy_transform_stream(reader, _path, _etl_args):
            data = reader.read()
            bytes_read_by_transform.append(data)
            yield data

        call_count = 0

        def mock_direct_put_stream(url, data_iter, *_a, **_kw):
            nonlocal call_count
            call_count += 1
            # Consume the generator so transform_stream reads from reader.
            b"".join(data_iter)
            if call_count == 1:
                raise ETLDirectPutTransientError(url, requests.ConnectionError())
            return (204, b"", len(original_body))

        self.server.direct_put_retries = 1
        with self.server.app.test_request_context(
            "/test/obj", method="PUT", data=original_body
        ):
            with patch.object(
                self.server, "transform_stream", side_effect=spy_transform_stream
            ):
                with patch.object(
                    self.server,
                    "_direct_put_stream",
                    side_effect=mock_direct_put_stream,
                ):
                    with patch("time.sleep"):
                        self.server._direct_put_stream_with_retry(  # pylint: disable=protected-access
                            self._DIRECT_PUT_URL, "/test/obj"
                        )

        self.assertEqual(len(bytes_read_by_transform), 2, "expected 2 attempts")
        self.assertEqual(bytes_read_by_transform[0], original_body)
        self.assertEqual(
            bytes_read_by_transform[1],
            original_body,
            "retry must send original bytes, not empty bytes",
        )


# ---------------------------------------------------------------------------
# Sync (HTTP multi-threaded) streaming direct-put retry unit tests
# ---------------------------------------------------------------------------


class TestHTTPStreamingDirectPutRetry(unittest.TestCase):
    """Unit tests for HTTPMultiThreadedServer.RequestHandler._direct_put_stream_with_retry."""

    _DIRECT_PUT_URL = "http://target:51081/ais/@/dst/test/obj"

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.handler = DummyRequestHandler()
        self.handler.server.etl_server.direct_put_retries = 3

    def _make_reader(self, data=b"input"):
        return io.BytesIO(data)

    def _call(self, fqn="", raw_path="test/obj", is_get=False):
        return self.handler._direct_put_stream_with_retry(  # pylint: disable=protected-access
            self._DIRECT_PUT_URL, fqn, raw_path, "", is_get
        )

    def test_succeeds_on_first_attempt(self):
        """No retry when first attempt succeeds."""
        with patch.object(
            self.handler, "_get_stream_reader", return_value=self._make_reader()
        ):
            with patch.object(
                self.handler, "_direct_put_stream", return_value=(204, b"", 5)
            ):
                with patch("time.sleep") as mock_sleep:
                    result = self._call()
        self.assertEqual(result, (204, b"", 5))
        mock_sleep.assert_not_called()

    def test_retries_on_transient_error_then_succeeds(self):
        """Retries on ETLDirectPutTransientError and succeeds on second attempt."""
        self.handler.server.etl_server.direct_put_retries = 2
        ok = (204, b"", 4)
        call_count = 0

        def put_side(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ETLDirectPutTransientError(
                    self._DIRECT_PUT_URL, requests.ConnectionError()
                )
            return ok

        readers = [self._make_reader(), self._make_reader()]
        reader_iter = iter(readers)
        with patch.object(
            self.handler, "_get_stream_reader", side_effect=lambda *_: next(reader_iter)
        ):
            with patch.object(self.handler, "_direct_put_stream", side_effect=put_side):
                with patch("time.sleep"):
                    result = self._call()
        self.assertEqual(result, ok)

    def test_raises_after_exhausting_retries(self):
        """ETLDirectPutTransientError raised after all retries exhausted."""
        self.handler.server.etl_server.direct_put_retries = 2
        err = ETLDirectPutTransientError(
            self._DIRECT_PUT_URL, requests.ConnectionError()
        )
        readers = [self._make_reader() for _ in range(3)]
        reader_iter = iter(readers)
        with patch.object(
            self.handler, "_get_stream_reader", side_effect=lambda *_: next(reader_iter)
        ):
            with patch.object(self.handler, "_direct_put_stream", side_effect=err):
                with patch("time.sleep"):
                    with self.assertRaises(ETLDirectPutTransientError):
                        self._call()

    def test_reader_none_returns_error_tuple(self):
        """When _get_stream_reader returns None, a 502 error tuple is returned (no send_error)."""
        with patch.object(self.handler, "_get_stream_reader", return_value=None):
            result = self._call()
        self.assertEqual(result[0], 502)
        # send_error must NOT be called — that would write a second response
        self.handler.send_error.assert_not_called()

    def test_reader_none_on_retry_raises(self):
        """When reader reopening returns None on retry, the transient error is re-raised."""
        self.handler.server.etl_server.direct_put_retries = 2
        err = ETLDirectPutTransientError(
            self._DIRECT_PUT_URL, requests.ConnectionError()
        )
        # First reader open succeeds; reopening on retry returns None
        readers = [self._make_reader(), None]
        reader_iter = iter(readers)
        with patch.object(
            self.handler, "_get_stream_reader", side_effect=lambda *_: next(reader_iter)
        ):
            with patch.object(self.handler, "_direct_put_stream", side_effect=err):
                with patch("time.sleep"):
                    with self.assertRaises(ETLDirectPutTransientError):
                        self._call()

    def test_reader_reopened_on_each_retry(self):
        """For BytesIO readers (PUT-no-FQN), seek(0) is used on retry instead of
        reopening; _get_stream_reader is called only once (initial open)."""
        self.handler.server.etl_server.direct_put_retries = 2
        call_count = 0

        def put_side(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ETLDirectPutTransientError(
                    self._DIRECT_PUT_URL, requests.ConnectionError()
                )
            return (204, b"", 0)

        readers = [self._make_reader() for _ in range(3)]
        reader_iter = iter(readers)
        with patch.object(
            self.handler,
            "_get_stream_reader",
            side_effect=lambda *_: next(reader_iter),
        ) as mock_reader:
            with patch.object(self.handler, "_direct_put_stream", side_effect=put_side):
                with patch("time.sleep"):
                    self._call()
        # BytesIO readers are rewound with seek(0) on retry; _get_stream_reader
        # is called only once (initial open), not once per attempt.
        self.assertEqual(mock_reader.call_count, 1)

    def test_reader_always_closed_on_success(self):
        """close_reader called once (via finally) after success."""
        mock_reader = self._make_reader()
        with patch.object(self.handler, "_get_stream_reader", return_value=mock_reader):
            with patch.object(
                self.handler, "_direct_put_stream", return_value=(204, b"", 0)
            ):
                self._call()
        self.handler.server.etl_server.close_reader.assert_called_once_with(mock_reader)

    def test_reader_always_closed_on_exhausted_retries(self):
        """For BytesIO readers (PUT-no-FQN), seek(0) is used on retry instead of
        close+reopen; close_reader is called once in the finally block."""
        self.handler.server.etl_server.direct_put_retries = 2
        err = ETLDirectPutTransientError(
            self._DIRECT_PUT_URL, requests.ConnectionError()
        )
        readers = [self._make_reader() for _ in range(3)]
        reader_iter = iter(readers)
        with patch.object(
            self.handler, "_get_stream_reader", side_effect=lambda *_: next(reader_iter)
        ):
            with patch.object(self.handler, "_direct_put_stream", side_effect=err):
                with patch("time.sleep"):
                    with self.assertRaises(ETLDirectPutTransientError):
                        self._call()
        # BytesIO: no close in except (seek instead); 1 close in finally = 1 total
        self.assertEqual(self.handler.server.etl_server.close_reader.call_count, 1)

    def test_exponential_backoff_delays(self):
        """time.sleep is called with exponentially increasing delays."""
        self.handler.server.etl_server.direct_put_retries = 3
        call_count = 0

        def put_side(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise ETLDirectPutTransientError(
                    self._DIRECT_PUT_URL, requests.ConnectionError()
                )
            return (204, b"", 0)

        readers = [self._make_reader() for _ in range(4)]
        reader_iter = iter(readers)
        with patch.object(
            self.handler, "_get_stream_reader", side_effect=lambda *_: next(reader_iter)
        ):
            with patch.object(self.handler, "_direct_put_stream", side_effect=put_side):
                with patch("time.sleep") as mock_sleep:
                    self._call()

        delays = [c.args[0] for c in mock_sleep.call_args_list]
        self.assertEqual(len(delays), 3)
        self.assertEqual(delays[0], min(2.0**0, 30.0))
        self.assertEqual(delays[1], min(2.0**1, 30.0))
        self.assertEqual(delays[2], min(2.0**2, 30.0))

    def test_put_no_fqn_retry_sends_original_bytes(self):
        """Regression: HTTP streaming PUT without FQN replays original body on retry.

        Before the fix, calling _get_stream_reader again on retry would drain rfile
        a second time and return an empty BytesIO. After the fix, seek(0) is called on
        the existing BytesIO so the retry reads the same original bytes.
        """
        original_body = b"hello http regression bytes"
        bytes_read_by_transform = []

        def spy_transform_stream(reader, _path, _etl_args):
            data = reader.read()
            bytes_read_by_transform.append(data)
            yield data

        call_count = 0

        def mock_direct_put_stream(url, data_iter, *_a, **_kw):
            nonlocal call_count
            call_count += 1
            # Consume the generator so transform_stream reads from reader.
            b"".join(data_iter)
            if call_count == 1:
                raise ETLDirectPutTransientError(url, requests.ConnectionError())
            return (204, b"", len(original_body))

        handler = DummyRequestHandler()
        handler.headers = {HEADER_CONTENT_LENGTH: str(len(original_body))}
        handler.rfile = io.BytesIO(original_body)
        handler.server.etl_server.direct_put_retries = 1
        handler.server.etl_server.transform_stream = spy_transform_stream
        handler.server.etl_server.close_reader = MagicMock()

        with patch.object(
            handler, "_direct_put_stream", side_effect=mock_direct_put_stream
        ):
            with patch("time.sleep"):
                handler._direct_put_stream_with_retry(  # pylint: disable=protected-access
                    self._DIRECT_PUT_URL, "", "/test/obj", "", is_get=False
                )

        self.assertEqual(len(bytes_read_by_transform), 2, "expected 2 attempts")
        self.assertEqual(bytes_read_by_transform[0], original_body)
        self.assertEqual(
            bytes_read_by_transform[1],
            original_body,
            "retry must send original bytes, not empty bytes",
        )


# ---------------------------------------------------------------------------
# ConnectionRefused permanent-error guard tests
# ---------------------------------------------------------------------------


def _make_connection_refused_error():
    """Build a realistic requests.ConnectionError wrapping a ConnectionRefused chain."""
    from urllib3.exceptions import (  # pylint: disable=import-outside-toplevel
        MaxRetryError,
        NewConnectionError,
    )

    conn_refused = ConnectionRefusedError(111, "Connection refused")
    new_conn_err = NewConnectionError(
        None, "Failed to establish a new connection: [Errno 111] Connection refused"
    )
    new_conn_err.__cause__ = conn_refused
    max_retry = MaxRetryError(pool=None, url="/nonexistent", reason=new_conn_err)
    return requests.ConnectionError(max_retry)


def _make_connection_refused_error_context_only():
    """Build a ConnectionError where ConnectionRefusedError is only on __context__.

    Mirrors urllib3 v1.x / Python 3.9 behavior: NewConnectionError is raised
    inside an ``except ConnectionRefusedError`` block without an explicit ``from``,
    so __cause__ is None and the root cause is only reachable via __context__.
    """
    from urllib3.exceptions import (  # pylint: disable=import-outside-toplevel
        MaxRetryError,
        NewConnectionError,
    )

    conn_refused = ConnectionRefusedError(111, "Connection refused")
    new_conn_err = NewConnectionError(
        None, "Failed to establish a new connection: [Errno 111] Connection refused"
    )
    # Simulate urllib3 v1.x: no explicit chaining, only implicit __context__
    new_conn_err.__cause__ = None
    new_conn_err.__context__ = conn_refused
    max_retry = MaxRetryError(pool=None, url="/nonexistent", reason=new_conn_err)
    return requests.ConnectionError(max_retry)


class TestIsConnectionRefused(unittest.TestCase):
    """Unit tests for _is_connection_refused() covering both urllib3 v1.x and v2.x chains."""

    def test_cause_chain_returns_true(self):
        """__cause__ chain (urllib3 v2.x) is detected as ConnectionRefused."""
        exc = _make_connection_refused_error()
        self.assertTrue(_is_connection_refused(exc))

    def test_context_only_chain_returns_true(self):
        """__context__-only chain (urllib3 v1.x / Python 3.9) is detected as ConnectionRefused."""
        exc = _make_connection_refused_error_context_only()
        self.assertTrue(_is_connection_refused(exc))

    def test_bare_connection_error_returns_false(self):
        """Bare ConnectionError('lost') is not a ConnectionRefused — must return False."""
        self.assertFalse(_is_connection_refused(requests.ConnectionError("lost")))


class TestFlaskConnectionRefusedGuard(unittest.TestCase):
    """ConnectionRefused is a permanent error — must return 502, never retry."""

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.server = DummyFlaskServer()
        self.server.direct_put_retries = 3

    def _put(self, url="http://localhost:19999/nonexistent", data=b"data"):
        with self.server.app.test_request_context():
            # pylint: disable=protected-access
            return self.server._direct_put(url, data)

    def _retry(self, url="http://localhost:19999/nonexistent", data=b"data"):
        with self.server.app.test_request_context():
            # pylint: disable=protected-access
            return self.server._direct_put_with_retry(url, data)

    def test_connection_refused_returns_502(self):
        """_direct_put() returns (502, ...) for ConnectionRefused — not ETLDirectPutTransientError."""
        exc = _make_connection_refused_error()
        with patch.object(self.server, "client_put", side_effect=exc):
            status, body, _ = self._put()
        self.assertEqual(status, 502)
        self.assertIn(b"ConnectionError", body)
        self.assertIn(b"/nonexistent", body)

    def test_connection_refused_not_retried(self):
        """_direct_put_with_retry() does not sleep for ConnectionRefused."""
        exc = _make_connection_refused_error()
        with patch.object(self.server, "client_put", side_effect=exc):
            with patch("time.sleep") as mock_sleep:
                status, _, _ = self._retry()
        self.assertEqual(status, 502)
        mock_sleep.assert_not_called()

    def test_bare_connection_error_still_retried(self):
        """Bare ConnectionError('lost') is still wrapped as transient (existing behavior)."""
        with self.server.app.test_request_context():
            with patch.object(
                self.server,
                "client_put",
                side_effect=requests.ConnectionError("lost"),
            ):
                with self.assertRaises(ETLDirectPutTransientError):
                    self.server._direct_put(  # pylint: disable=protected-access
                        "http://target/obj", b"data"
                    )

    def test_connection_refused_stream_not_retried(self):
        """Streaming direct-put with ConnectionRefused returns 502 without sleeping.

        Patches session.put so the guard inside _direct_put_stream fires and returns
        (502, ...) — which _direct_put_stream_with_retry forwards without retrying.
        Uses DummyStreamingFlaskServer to provide a real transform_stream.
        """
        streaming_server = DummyStreamingFlaskServer()
        streaming_server.direct_put_retries = 3
        exc = _make_connection_refused_error()

        with streaming_server.app.test_request_context(
            "/test/obj", method="PUT", data=b"hello"
        ):
            with patch.object(streaming_server.session, "put", side_effect=exc):
                with patch("time.sleep") as mock_sleep:
                    result = streaming_server._direct_put_stream_with_retry(  # pylint: disable=protected-access
                        "http://localhost:19999/nonexistent", "/test/obj"
                    )
        self.assertEqual(result[0], 502)
        mock_sleep.assert_not_called()


class TestHTTPConnectionRefusedGuard(unittest.TestCase):
    """ConnectionRefused is a permanent error — HTTP server must return 502, never retry."""

    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.handler = DummyRequestHandler()
        self.handler.server.etl_server.direct_put_retries = 3

    def _put(self, url="http://localhost:19999/nonexistent", data=b"data"):
        # pylint: disable=protected-access
        return self.handler._direct_put(url, data, "", "/nonexistent")

    def _retry(self, url="http://localhost:19999/nonexistent", data=b"data"):
        # pylint: disable=protected-access
        return self.handler._direct_put_with_retry(url, data)

    def test_connection_refused_returns_502(self):
        """_direct_put() returns (502, ...) for ConnectionRefused — not ETLDirectPutTransientError."""
        exc = _make_connection_refused_error()
        self.handler.server.etl_server.client_put = MagicMock(side_effect=exc)
        status, body, _ = self._put()
        self.assertEqual(status, 502)
        self.assertIn(b"ConnectionError", body)
        self.assertIn(b"/nonexistent", body)

    def test_connection_refused_not_retried(self):
        """_direct_put_with_retry() does not sleep for ConnectionRefused."""
        exc = _make_connection_refused_error()
        self.handler.server.etl_server.client_put = MagicMock(side_effect=exc)
        with patch("time.sleep") as mock_sleep:
            status, _, _ = self._retry()
        self.assertEqual(status, 502)
        mock_sleep.assert_not_called()

    def test_bare_connection_error_still_retried(self):
        """Bare ConnectionError('lost') is still wrapped as transient (existing behavior)."""
        self.handler.server.etl_server.client_put = MagicMock(
            side_effect=requests.ConnectionError("lost")
        )
        with self.assertRaises(ETLDirectPutTransientError):
            self._put()
