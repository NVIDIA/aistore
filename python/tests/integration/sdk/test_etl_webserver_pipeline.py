import os
import sys
import unittest
import time
import threading
import concurrent.futures
from http.server import HTTPServer
from unittest.mock import AsyncMock, Mock, patch
from fastapi.testclient import TestClient

import requests
import pytest

from aistore.sdk.const import (
    HEADER_NODE_URL,
    HEADER_DIRECT_PUT_LENGTH,
    HEADER_CONTENT_LENGTH,
    ETL_WS_PIPELINE,
)
from aistore.sdk.etl.webserver.http_multi_threaded_server import HTTPMultiThreadedServer
from aistore.sdk.etl.webserver.flask_server import FlaskServer
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer
from aistore.sdk.etl.webserver.utils import parse_etl_pipeline
from aistore.sdk.errors import InvalidPipelineError

CLEANUP_DELAY = 0.1  # Minimal delay for cleanup operations


def wait_for_server_ready(
    host: str,
    port: int,
    server_type: str,
    max_retries: int = 5,
    retry_interval: float = 0.2,
) -> None:
    """
    Poll server health endpoint until ready or max retries reached.

    Args:
        host: Server host (e.g., "127.0.0.1")
        port: Server port
        server_type: Type of server for error messages ("FastAPI", "Flask", "HTTP")
        max_retries: Maximum number of polling attempts (default: 5)
        retry_interval: Time between polling attempts in seconds (default: 0.2)

    Raises:
        RuntimeError: If server doesn't become ready within max_retries
    """
    health_url = f"http://{host}:{port}/health"

    for attempt in range(max_retries):
        try:
            response = requests.get(health_url, timeout=1.0)
            if response.status_code == 200:
                # Server is ready
                return
        except (requests.exceptions.RequestException, requests.exceptions.Timeout):
            # Server not ready yet, continue polling
            pass

        if attempt < max_retries - 1:  # Don't sleep after the last attempt
            time.sleep(retry_interval)

    # If we get here, server didn't become ready
    raise RuntimeError(
        f"{server_type} server at {host}:{port} failed to become ready after {max_retries} attempts"
    )


class MockHTTPETLServer(HTTPMultiThreadedServer):
    """Mock ETL server for testing transform and MIME type override."""

    def transform(self, data: bytes, *_args) -> bytes:
        return data.upper()

    def get_mime_type(self) -> str:
        return "text/caps"


class MockFastAPIServer(FastAPIServer):
    def transform(self, data: bytes, _path: str, _etl_args: str) -> bytes:
        return data[::-1]

    def get_mime_type(self) -> str:
        return "application/test"


class MockFlaskServer(FlaskServer):
    def transform(self, data: bytes, _path: str, etl_args: str) -> bytes:
        return b"flask: " + data + etl_args.encode()

    def get_mime_type(self) -> str:
        return "application/flask"


class TestPipelineBase(unittest.TestCase):
    def setUp(self):
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"
        self.started_servers = []
        self.server_threads = []

    def tearDown(self):
        """Clean up all started servers."""
        # Servers are running in daemon threads, they'll clean up automatically
        # Just wait a bit for any ongoing operations to complete
        time.sleep(CLEANUP_DELAY)

        # Clean up environment
        self.started_servers.clear()
        self.server_threads.clear()

    def _start_http_server(
        self, port: int, transform_prefix: str = "http"
    ) -> MockHTTPETLServer:
        """Start an HTTP ETL server on specified port."""

        class TestHTTPServer(MockHTTPETLServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return f"{transform_prefix}:{port}:".encode() + data

        etl_server = TestHTTPServer()
        # Create HTTP server directly without signal handlers
        server = HTTPServer(("localhost", port), etl_server.RequestHandler)
        server.etl_server = etl_server

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        self.started_servers.append(server)
        self.server_threads.append(thread)

        # Wait for HTTP server to be ready using health check polling
        wait_for_server_ready("127.0.0.1", port, "HTTP")

        return etl_server

    def _start_flask_server(
        self, port: int, transform_prefix: str = "flask"
    ) -> MockFlaskServer:
        """Start a Flask ETL server on specified port."""

        class TestFlaskServer(MockFlaskServer):
            def transform(self, data: bytes, _path: str, _etl_args: str) -> bytes:
                return f"{transform_prefix}:{port}:".encode() + data

        server = TestFlaskServer(host="localhost", port=port)

        thread = threading.Thread(target=server.start, daemon=True)
        thread.start()

        self.server_threads.append(thread)

        # Wait for Flask server to be ready using health check polling
        wait_for_server_ready("127.0.0.1", port, "Flask")

        return server

    def _start_fastapi_server(
        self, port: int, transform_prefix: str = "fastapi"
    ) -> MockFastAPIServer:
        """Start a FastAPI ETL server on specified port."""

        class TestFastAPIServer(MockFastAPIServer):
            def transform(self, data: bytes, _path: str, _etl_args: str) -> bytes:
                return f"{transform_prefix}:{port}:".encode() + data

        server = TestFastAPIServer(host="localhost", port=port)

        thread = threading.Thread(target=server.start, daemon=True)
        thread.start()

        self.server_threads.append(thread)

        # Wait for FastAPI server to be ready using health check polling
        wait_for_server_ready("127.0.0.1", port, "FastAPI")

        return server


class TestPipelineParsing(TestPipelineBase):
    """Test the pipeline parsing utility function."""

    @pytest.mark.etl
    def test_parse_etl_pipeline_single_url(self):
        """Test parsing a single URL."""
        result = parse_etl_pipeline("http://node1:8080/transform")
        self.assertEqual(result, ("http://node1:8080/transform", ""))

    @pytest.mark.etl
    def test_parse_etl_pipeline_multiple_urls(self):
        """Test parsing multiple URLs."""

        pipeline = "http://node1:8080/transform1,http://node2:8080/transform2,http://node3:8080/final"
        first_url, remaining = parse_etl_pipeline(pipeline)
        self.assertEqual(first_url, "http://node1:8080/transform1")
        self.assertEqual(
            remaining, "http://node2:8080/transform2,http://node3:8080/final"
        )

    @pytest.mark.etl
    def test_parse_etl_pipeline_empty(self):
        """Test parsing empty pipeline."""

        result = parse_etl_pipeline("")
        self.assertEqual(result, ("", ""))

        result = parse_etl_pipeline(None)
        self.assertEqual(result, ("", ""))

    @pytest.mark.etl
    def test_parse_etl_pipeline_with_spaces(self):
        """Test parsing pipeline with spaces around URLs."""

        pipeline = "  http://node1:8080/transform1  ,  http://node2:8080/transform2  "
        first_url, remaining = parse_etl_pipeline(pipeline)
        self.assertEqual(first_url, "http://node1:8080/transform1")
        self.assertEqual(remaining, "http://node2:8080/transform2")

    @pytest.mark.etl
    def test_parse_etl_pipeline_invalid_leading_comma(self):
        """Test pipeline validation with leading comma."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(",http://node1:8080/transform")
        self.assertIn("empty entry", str(cm.exception))

    @pytest.mark.etl
    def test_parse_etl_pipeline_invalid_trailing_comma(self):
        """Test pipeline validation with trailing comma."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline("http://node1:8080/transform,")
        self.assertIn("empty entry", str(cm.exception))

    @pytest.mark.etl
    def test_parse_etl_pipeline_invalid_double_comma(self):
        """Test pipeline validation with double comma."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(
                "http://node1:8080/transform,,http://node2:8080/transform"
            )
        self.assertIn("empty entry", str(cm.exception))

    @pytest.mark.etl
    def test_parse_etl_pipeline_invalid_multiple_empty_entries(self):
        """Test pipeline validation with multiple empty entries."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(",,,")
        self.assertIn("empty entry", str(cm.exception))

    @pytest.mark.etl
    def test_parse_etl_pipeline_invalid_mixed_empty_entries(self):
        """Test pipeline validation with mixed valid and empty entries."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(
                "http://node1:8080/transform,,   ,http://node2:8080/transform"
            )
        self.assertIn("empty entry", str(cm.exception))

    @pytest.mark.etl
    def test_parse_etl_pipeline_invalid_only_spaces(self):
        """Test pipeline validation with only spaces between commas."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline("   ,   ")
        self.assertIn("empty entry", str(cm.exception))

    @pytest.mark.etl
    def test_http_server_invalid_pipeline_validation(self):
        """Test HTTP server properly validates malformed pipeline headers."""

        self._start_http_server(9201, "validation")

        # Test different malformed pipeline patterns
        malformed_patterns = [
            ",http://server:8080/path",  # leading comma
            "http://server:8080/path,",  # trailing comma
            "http://server1:8080/path,,http://server2:8080/path",  # double comma
            ",,,",  # only commas
        ]

        for pattern in malformed_patterns:
            with self.subTest(pattern=pattern):
                headers = {HEADER_NODE_URL: pattern}
                response = requests.put(
                    "http://localhost:9201/test",
                    data=b"test_data",
                    headers=headers,
                    timeout=5,
                )
                self.assertEqual(response.status_code, 400)
                self.assertIn("Invalid pipeline", response.text)

    @pytest.mark.etl
    def test_flask_server_invalid_pipeline_validation(self):
        """Test Flask server properly validates malformed pipeline headers."""

        self._start_flask_server(9202, "validation")

        headers = {HEADER_NODE_URL: ",,invalid,,"}
        response = requests.put(
            "http://localhost:9202/test", data=b"test_data", headers=headers, timeout=5
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid pipeline", response.text)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_fastapi_server_invalid_pipeline_validation(self):
        """Test FastAPI server properly validates malformed pipeline headers."""

        self._start_fastapi_server(9203, "validation")

        headers = {HEADER_NODE_URL: "http://server:8080/path,,other"}
        response = requests.put(
            "http://localhost:9203/test", data=b"test_data", headers=headers, timeout=5
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid pipeline", response.text)


class TestMultiServerPipelineIntegration(TestPipelineBase):
    """Integration tests with actual servers running on different ports."""

    @pytest.mark.etl
    def test_http_to_http_pipeline_chain(self):
        """Test pipeline forwarding between multiple HTTP servers."""

        # Start three HTTP servers on different ports
        server1 = self._start_http_server(19001, "step1")
        server2 = self._start_http_server(19002, "step2")
        server3 = self._start_http_server(19003, "step3")

        # Create pipeline header: server1 -> server2 -> server3
        pipeline = "http://localhost:19002/transform,http://localhost:19003/transform"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server3.transform(server2.transform(server1.transform(content)))

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:19001/test", data=content, headers=headers, timeout=5
        )

        # Should get 200 (pipeline processed to the final ETL server)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(HEADER_DIRECT_PUT_LENGTH, response.headers)
        self.assertEqual(response.content, result)
        self.assertEqual(response.headers.get(HEADER_CONTENT_LENGTH), str(len(result)))

    @pytest.mark.etl
    def test_http_to_target_pipeline_chain(self):
        """Test pipeline forwarding between HTTP servers ending with a target server."""

        # Start two ETL servers
        server1 = self._start_http_server(12001, "step1")
        server2 = self._start_http_server(12002, "step2")

        # Create mock response for target server call
        target_response = Mock()
        target_response.status_code = 200  # Will be converted to 204 by server
        target_response.content = b""
        target_response.headers = {}

        server2.client_put = Mock()
        server2.client_put.return_value = target_response

        # Create pipeline header: server1 -> server2 -> target
        pipeline = "http://localhost:12002/transform,http://localhost:12003/target"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server2.transform(server1.transform(content))

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:12001/test", data=content, headers=headers, timeout=5
        )

        # Should get 204 (server converts target's 200 with empty content to 204)
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        self.assertEqual(
            response.headers.get(HEADER_DIRECT_PUT_LENGTH), str(len(result))
        )

        server2.client_put.assert_called_once_with(
            "http://localhost:12003/target", result, headers={}
        )

    @pytest.mark.etl
    def test_flask_to_flask_pipeline_chain(self):
        """Test pipeline forwarding between multiple Flask servers."""

        # Start three Flask servers
        server1 = self._start_flask_server(19011, "flask1")
        server2 = self._start_flask_server(19012, "flask2")
        server3 = self._start_flask_server(19013, "flask3")

        # Create pipeline header: server1 -> server2 -> server3
        pipeline = "http://localhost:19012/transform,http://localhost:19013/transform"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server3.transform(
            server2.transform(server1.transform(content, "", ""), "", ""), "", ""
        )

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:19011/test", data=content, headers=headers, timeout=5
        )

        # Should get 200 (pipeline processed to the final ETL server)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(HEADER_DIRECT_PUT_LENGTH, response.headers)
        self.assertEqual(response.content, result)
        self.assertEqual(response.headers.get(HEADER_CONTENT_LENGTH), str(len(result)))

    @pytest.mark.etl
    def test_flask_to_target_pipeline_chain(self):
        """Test pipeline forwarding between Flask servers ending with a target server."""

        # Start two ETL servers
        server1 = self._start_flask_server(19021, "step1")
        server2 = self._start_flask_server(19022, "step2")

        # Create mock response for target server call
        target_response = Mock()
        target_response.status_code = 200  # Will be converted to 204 by server
        target_response.content = b""
        target_response.headers = {}

        server2.client_put = Mock()
        server2.client_put.return_value = target_response

        # Create pipeline header: server1 -> server2 -> target
        pipeline = "http://localhost:19022/transform,http://localhost:19023/target"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server2.transform(server1.transform(content, "", ""), "", "")

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:19021/test", data=content, headers=headers, timeout=5
        )

        # Should get 204 (server converts target's 200 with empty content to 204)
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        self.assertEqual(
            response.headers.get(HEADER_DIRECT_PUT_LENGTH), str(len(result))
        )

        server2.client_put.assert_called_once_with(
            "http://localhost:19023/target", result, headers={}
        )

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_fastapi_to_fastapi_pipeline_chain(self):
        """Test pipeline forwarding between multiple FastAPI servers."""

        # Start three FastAPI servers
        server1 = self._start_fastapi_server(11031, "fast1")
        server2 = self._start_fastapi_server(11032, "fast2")
        server3 = self._start_fastapi_server(11033, "fast3")

        # Create pipeline: server1 -> server2 -> server3
        pipeline = "http://localhost:11032/transform,http://localhost:11033/transform"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server3.transform(
            server2.transform(server1.transform(content, "", ""), "", ""), "", ""
        )

        response = requests.put(
            "http://localhost:11031/test", data=content, headers=headers, timeout=5
        )

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(HEADER_DIRECT_PUT_LENGTH, response.headers)
        self.assertEqual(response.content, result)
        self.assertEqual(response.headers.get(HEADER_CONTENT_LENGTH), str(len(result)))

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_fastapi_to_target_pipeline_chain(self):
        """Test pipeline forwarding between FastAPI servers ending with a target server."""

        # Start two ETL servers
        server1 = self._start_fastapi_server(19041, "step1")
        server2 = self._start_fastapi_server(19042, "step2")

        # Create mock response for target server call
        target_response = AsyncMock()
        target_response.status_code = 200  # Will be converted to 204 by server
        target_response.content = b""
        target_response.headers = {}

        # Mock the direct delivery response (simulate 200 OK)
        server2.client.put = AsyncMock()
        server2.client.put.return_value = target_response

        # Create pipeline header: server1 -> server2 -> target
        pipeline = "http://localhost:19042/transform,http://localhost:19043/target"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server2.transform(server1.transform(content, "", ""), "", "")

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:19041/test", data=content, headers=headers, timeout=5
        )

        # Should get 204 (server converts target's 200 with empty content to 204)
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        self.assertEqual(
            response.headers.get(HEADER_DIRECT_PUT_LENGTH), str(len(result))
        )

        server2.client.put.assert_awaited_once_with(
            "http://localhost:19043/target", content=result, headers={}
        )

    @pytest.mark.etl
    def test_mixed_server_type_pipeline(self):
        """Test pipeline forwarding across different server types."""

        # Start one server of each type
        http_server = self._start_http_server(19031, "http")
        flask_server = self._start_flask_server(19032, "flask")

        # Pipeline: HTTP -> Flask
        pipeline = "http://localhost:19032/transform"
        headers = {HEADER_NODE_URL: pipeline}
        result = flask_server.transform(
            http_server.transform(b"mixed_data", "", ""), "", ""
        )

        response = requests.put(
            "http://localhost:19031/mixed",
            data=b"mixed_data",
            headers=headers,
            timeout=5,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, result)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_three_way_mixed_pipeline(self):
        """Test HTTP -> Flask -> FastAPI pipeline."""

        http_server = self._start_http_server(13001, "step1")
        flask_server = self._start_flask_server(13002, "step2")
        fastapi_server = self._start_fastapi_server(13003, "step3")

        # Three-stage pipeline across all server types
        pipeline = "http://localhost:13002/stage2,http://localhost:13003/stage3"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = fastapi_server.transform(
            flask_server.transform(http_server.transform(content, "", ""), "", ""),
            "",
            "",
        )

        response = requests.put(
            "http://localhost:13001/multi", data=content, headers=headers, timeout=8
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, result)

    @pytest.mark.etl
    def test_pipeline_error_handling(self):
        """Test error handling when pipeline target is unreachable."""

        self._start_http_server(19061, "error_test")

        # Pipeline to non-existent server
        pipeline = "http://localhost:19999/nonexistent"
        headers = {HEADER_NODE_URL: pipeline}

        response = requests.put(
            "http://localhost:19061/error",
            data=b"error_data",
            headers=headers,
            timeout=5,
        )

        self.assertEqual(response.status_code, 500)
        self.assertIn(b"/nonexistent", response.content)
        self.assertIn(b"ConnectionError", response.content)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_long_pipeline_chain(self):
        """Test a longer pipeline with 5 servers."""

        # Start 5 HTTP servers
        content = b"original"
        result = content
        servers = []
        for i in range(5):
            port = 19070 + i
            server = self._start_fastapi_server(port, f"stage{i+1}")
            servers.append(server)
            result = server.transform(result, "", "")

        # Create 5-stage pipeline
        pipeline_stages = [f"http://localhost:{19071 + i}/stage{i+2}" for i in range(4)]
        pipeline = ",".join(pipeline_stages)
        headers = {HEADER_NODE_URL: pipeline}

        response = requests.put(
            "http://localhost:19070/long", data=content, headers=headers, timeout=10
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, result)

    @pytest.mark.etl
    def test_empty_pipeline_header(self):
        """Test behavior with empty pipeline header."""

        server = self._start_http_server(19081, "empty")
        content = b"original"
        result = server.transform(content, "", "")

        # Empty pipeline header should return transformed data normally
        response = requests.put(
            "http://localhost:19081/empty", data=content, headers={}, timeout=5
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, result)

    @pytest.mark.etl
    def test_malformed_pipeline_header(self):
        """Test behavior with malformed pipeline header."""

        self._start_http_server(19091, "malformed")

        # Malformed pipeline (just commas)
        headers = {HEADER_NODE_URL: ",,,,"}

        response = requests.put(
            "http://localhost:19091/malformed",
            data=b"malformed_test",
            headers=headers,
            timeout=5,
        )

        # Should return 400 error due to malformed pipeline
        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid pipeline", response.text)

    @pytest.mark.etl
    def test_concurrent_pipeline_requests(self):
        """Test multiple concurrent requests through pipeline."""

        self._start_http_server(19101, "concurrent1")
        self._start_http_server(19102, "concurrent2")

        pipeline = "http://localhost:19102/concurrent"
        headers = {HEADER_NODE_URL: pipeline}

        def make_request(i):
            return requests.put(
                "http://localhost:19101/concurrent",
                data=f"request_{i}".encode(),
                headers=headers,
                timeout=5,
            )

        # Send 10 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_request, i) for i in range(10)]
            results = [
                future.result() for future in concurrent.futures.as_completed(futures)
            ]

        # All should succeed
        for response in results:
            self.assertEqual(response.status_code, 200)


class TestWebSocketPipelineIntegration(TestPipelineBase):
    """Test WebSocket ETL pipeline integration with various server types."""

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_to_http_pipeline(self):
        """Test WebSocket client connecting to FastAPI server with HTTP pipeline."""

        # Start FastAPI and HTTP servers
        fastapi_server = self._start_fastapi_server(14001, "ws_fastapi")
        http_server = self._start_http_server(14002, "ws_http")

        client = TestClient(fastapi_server.app)

        # Test pipeline to HTTP server
        with client.websocket_connect("/ws") as websocket:
            test_data = b"websocket_test_data"
            pipeline = "http://localhost:14002/transform"

            websocket.send_json(data={ETL_WS_PIPELINE: pipeline}, mode="binary")
            websocket.send_bytes(test_data)
            result = websocket.receive_bytes()

            # Data should be transformed by both servers
            expected = http_server.transform(
                fastapi_server.transform(test_data, "", ""), "", ""
            )
            self.assertEqual(result, expected)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_to_flask_pipeline(self):
        """Test WebSocket client connecting to FastAPI server with Flask pipeline."""

        # Start FastAPI and Flask servers
        fastapi_server = self._start_fastapi_server(14011, "ws_fastapi")
        flask_server = self._start_flask_server(14012, "ws_flask")

        client = TestClient(fastapi_server.app)

        # Test pipeline to Flask server
        with client.websocket_connect("/ws") as websocket:
            test_data = b"websocket_flask_data"
            pipeline = "http://localhost:14012/transform"

            websocket.send_json(data={ETL_WS_PIPELINE: pipeline}, mode="binary")
            websocket.send_bytes(test_data)
            result = websocket.receive_bytes()

            # Data should be transformed by both servers
            expected = flask_server.transform(
                fastapi_server.transform(test_data, "", ""), "", ""
            )
            self.assertEqual(result, expected)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_to_fastapi_pipeline(self):
        """Test WebSocket client connecting to FastAPI server with FastAPI pipeline."""

        # Start two FastAPI servers
        fastapi_server1 = self._start_fastapi_server(14021, "ws_fastapi1")
        fastapi_server2 = self._start_fastapi_server(14022, "ws_fastapi2")

        client = TestClient(fastapi_server1.app)

        # Test pipeline to another FastAPI server
        with client.websocket_connect("/ws") as websocket:
            test_data = b"websocket_fastapi_data"
            pipeline = "http://localhost:14022/transform"

            websocket.send_json(data={ETL_WS_PIPELINE: pipeline}, mode="binary")
            websocket.send_bytes(test_data)
            result = websocket.receive_bytes()

            # Data should be transformed by both servers
            expected = fastapi_server2.transform(
                fastapi_server1.transform(test_data, "", ""), "", ""
            )
            self.assertEqual(result, expected)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_to_target_direct_pipeline(self):
        """Test WebSocket client connecting to FastAPI server with direct target pipeline."""

        # Start FastAPI server
        fastapi_server = self._start_fastapi_server(14031, "ws_target")
        client = TestClient(fastapi_server.app)

        # Mock target response
        with patch.object(fastapi_server, "client", new=AsyncMock()) as mock_client:
            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_resp.content = b""
            mock_client.put.return_value = mock_resp

            # Test direct pipeline to target
            with client.websocket_connect("/ws") as websocket:
                test_data = b"websocket_target_data"
                pipeline = "http://localhost:14032/target"

                websocket.send_json(data={ETL_WS_PIPELINE: pipeline}, mode="binary")
                websocket.send_bytes(test_data)
                result = websocket.receive_text()

                # Should receive data length as acknowledgment
                expected_data = fastapi_server.transform(test_data, "", "")
                self.assertEqual(result, str(len(expected_data)))

                # Verify direct put was called
                mock_client.put.assert_awaited_once_with(
                    "http://localhost:14032/target", content=expected_data, headers={}
                )

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_multi_stage_pipeline(self):
        """Test WebSocket with multi-stage pipeline (HTTP->Flask->FastAPI)."""

        # Start all three server types
        ws_server = self._start_fastapi_server(14041, "ws_origin")
        http_server = self._start_http_server(14042, "stage1")
        flask_server = self._start_flask_server(14043, "stage2")
        fastapi_server = self._start_fastapi_server(14044, "stage3")

        client = TestClient(ws_server.app)

        # Test three-stage pipeline
        with client.websocket_connect("/ws") as websocket:
            test_data = b"multi_stage_data"
            pipeline = "http://localhost:14042/trans,http://localhost:14043/trans,http://localhost:14044/trans"

            websocket.send_json(data={ETL_WS_PIPELINE: pipeline}, mode="binary")
            websocket.send_bytes(test_data)
            result = websocket.receive_bytes()

            # Data should be transformed by all four servers in sequence
            step1 = ws_server.transform(test_data, "", "")
            step2 = http_server.transform(step1, "", "")
            step3 = flask_server.transform(step2, "", "")
            expected = fastapi_server.transform(step3, "", "")

            self.assertEqual(result, expected)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_pipeline_error_handling(self):
        """Test WebSocket error handling when pipeline target fails."""

        # Start FastAPI server
        fastapi_server = self._start_fastapi_server(14051, "ws_error")
        client = TestClient(fastapi_server.app)

        # Test pipeline to non-existent server
        with client.websocket_connect("/ws") as websocket:
            test_data = b"error_test_data"
            pipeline = "http://localhost:19999/nonexistent"

            websocket.send_json(data={ETL_WS_PIPELINE: pipeline}, mode="binary")
            websocket.send_bytes(test_data)
            result = websocket.receive_text()

            # Should receive error message
            self.assertIn("0", result)  # Error indicated by 0 length

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_invalid_pipeline_format(self):
        """Test WebSocket with invalid pipeline format."""

        # Start FastAPI server
        fastapi_server = self._start_fastapi_server(14061, "ws_invalid")
        client = TestClient(fastapi_server.app)

        # Test invalid pipeline format
        with client.websocket_connect("/ws") as websocket:
            test_data = b"invalid_pipeline_data"
            pipeline = ",,invalid,,"  # Invalid format with empty entries

            websocket.send_json(data={ETL_WS_PIPELINE: pipeline}, mode="binary")
            websocket.send_bytes(test_data)
            result = websocket.receive_text()

            # Should receive error message about invalid pipeline
            self.assertIn("Invalid pipeline", result)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    @pytest.mark.etl
    def test_websocket_no_pipeline_fallback(self):
        """Test WebSocket behavior without pipeline (fallback to direct response)."""

        # Start FastAPI server
        fastapi_server = self._start_fastapi_server(14071, "ws_direct")
        client = TestClient(fastapi_server.app)

        # Test without pipeline
        with client.websocket_connect("/ws") as websocket:
            test_data = b"direct_response_data"

            websocket.send_json(data={}, mode="binary")  # No pipeline
            websocket.send_bytes(test_data)
            result = websocket.receive_bytes()

            # Should receive directly transformed data
            expected = fastapi_server.transform(test_data, "", "")
            self.assertEqual(result, expected)
