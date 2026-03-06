import os
import sys
import io
import unittest
from unittest import mock
from unittest.mock import MagicMock, patch, AsyncMock

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
        """startup_event() defaults to 3 retries when AIS_DIRECT_PUT_RETRIES is not set."""
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

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    async def test_direct_put_returns_500_when_all_retries_exhausted(self):
        """_direct_put() returns 500 when ConnectError propagates after all transport retries.

        Retries are handled inside httpx.AsyncHTTPTransport — our code calls client.put()
        once. If the transport exhausts its retries and surfaces ConnectError, _direct_put
        must catch it gracefully and return a 500 rather than crash.
        """
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        os.environ["DIRECT_PUT"] = "true"
        server = DummyFastAPIServer()
        mock_client = AsyncMock()
        mock_client.put.side_effect = httpx.ConnectError(
            "DNS resolution failed: EAI_AGAIN"
        )
        server.client = mock_client

        status, body, length = (
            await server._direct_put(  # pylint: disable=protected-access
                "http://localhost:8080/ais/@/dst/obj", b"hello"
            )
        )

        mock_client.put.assert_awaited_once()  # transport retries internally; we call once
        self.assertEqual(status, 500)
        self.assertIn(b"DNS resolution failed", body)
        self.assertEqual(length, 0)


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
