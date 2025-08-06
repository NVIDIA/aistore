import os
import sys
import unittest
import time
import threading
import concurrent.futures
from http.server import HTTPServer
from unittest.mock import AsyncMock, Mock

import requests

from aistore.sdk.const import (
    HEADER_NODE_URL,
    HEADER_DIRECT_PUT_LENGTH,
    HEADER_CONTENT_LENGTH,
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

    def test_parse_etl_pipeline_single_url(self):
        """Test parsing a single URL."""
        result = parse_etl_pipeline("http://node1:8080/transform")
        self.assertEqual(result, ("http://node1:8080/transform", ""))

    def test_parse_etl_pipeline_multiple_urls(self):
        """Test parsing multiple URLs."""

        pipeline = "http://node1:8080/transform1,http://node2:8080/transform2,http://node3:8080/final"
        first_url, remaining = parse_etl_pipeline(pipeline)
        self.assertEqual(first_url, "http://node1:8080/transform1")
        self.assertEqual(
            remaining, "http://node2:8080/transform2,http://node3:8080/final"
        )

    def test_parse_etl_pipeline_empty(self):
        """Test parsing empty pipeline."""

        result = parse_etl_pipeline("")
        self.assertEqual(result, ("", ""))

        result = parse_etl_pipeline(None)
        self.assertEqual(result, ("", ""))

    def test_parse_etl_pipeline_with_spaces(self):
        """Test parsing pipeline with spaces around URLs."""

        pipeline = "  http://node1:8080/transform1  ,  http://node2:8080/transform2  "
        first_url, remaining = parse_etl_pipeline(pipeline)
        self.assertEqual(first_url, "http://node1:8080/transform1")
        self.assertEqual(remaining, "http://node2:8080/transform2")

    def test_parse_etl_pipeline_invalid_leading_comma(self):
        """Test pipeline validation with leading comma."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(",http://node1:8080/transform")
        self.assertIn("empty entry", str(cm.exception))

    def test_parse_etl_pipeline_invalid_trailing_comma(self):
        """Test pipeline validation with trailing comma."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline("http://node1:8080/transform,")
        self.assertIn("empty entry", str(cm.exception))

    def test_parse_etl_pipeline_invalid_double_comma(self):
        """Test pipeline validation with double comma."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(
                "http://node1:8080/transform,,http://node2:8080/transform"
            )
        self.assertIn("empty entry", str(cm.exception))

    def test_parse_etl_pipeline_invalid_multiple_empty_entries(self):
        """Test pipeline validation with multiple empty entries."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(",,,")
        self.assertIn("empty entry", str(cm.exception))

    def test_parse_etl_pipeline_invalid_mixed_empty_entries(self):
        """Test pipeline validation with mixed valid and empty entries."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline(
                "http://node1:8080/transform,,   ,http://node2:8080/transform"
            )
        self.assertIn("empty entry", str(cm.exception))

    def test_parse_etl_pipeline_invalid_only_spaces(self):
        """Test pipeline validation with only spaces between commas."""
        with self.assertRaises(InvalidPipelineError) as cm:
            parse_etl_pipeline("   ,   ")
        self.assertIn("empty entry", str(cm.exception))

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

    def test_http_to_http_pipeline_chain(self):
        """Test pipeline forwarding between multiple HTTP servers."""

        # Start three HTTP servers on different ports
        server1 = self._start_http_server(9001, "step1")
        server2 = self._start_http_server(9002, "step2")
        server3 = self._start_http_server(9003, "step3")

        # Create pipeline header: server1 -> server2 -> server3
        pipeline = "http://localhost:9002/transform,http://localhost:9003/transform"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server3.transform(server2.transform(server1.transform(content)))

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:9001/test", data=content, headers=headers, timeout=5
        )

        # Should get 200 (pipeline processed to the final ETL server)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(HEADER_DIRECT_PUT_LENGTH, response.headers)
        self.assertEqual(response.content, result)
        self.assertEqual(response.headers.get(HEADER_CONTENT_LENGTH), str(len(result)))

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

    def test_flask_to_flask_pipeline_chain(self):
        """Test pipeline forwarding between multiple Flask servers."""

        # Start three Flask servers
        server1 = self._start_flask_server(9011, "flask1")
        server2 = self._start_flask_server(9012, "flask2")
        server3 = self._start_flask_server(9013, "flask3")

        # Create pipeline header: server1 -> server2 -> server3
        pipeline = "http://localhost:9012/transform,http://localhost:9013/transform"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server3.transform(
            server2.transform(server1.transform(content, "", ""), "", ""), "", ""
        )

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:9011/test", data=content, headers=headers, timeout=5
        )

        # Should get 200 (pipeline processed to the final ETL server)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(HEADER_DIRECT_PUT_LENGTH, response.headers)
        self.assertEqual(response.content, result)
        self.assertEqual(response.headers.get(HEADER_CONTENT_LENGTH), str(len(result)))

    def test_flask_to_target_pipeline_chain(self):
        """Test pipeline forwarding between Flask servers ending with a target server."""

        # Start two ETL servers
        server1 = self._start_flask_server(9021, "step1")
        server2 = self._start_flask_server(9022, "step2")

        # Create mock response for target server call
        target_response = Mock()
        target_response.status_code = 200  # Will be converted to 204 by server
        target_response.content = b""
        target_response.headers = {}

        server2.client_put = Mock()
        server2.client_put.return_value = target_response

        # Create pipeline header: server1 -> server2 -> target
        pipeline = "http://localhost:9022/transform,http://localhost:9023/target"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server2.transform(server1.transform(content, "", ""), "", "")

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:9021/test", data=content, headers=headers, timeout=5
        )

        # Should get 204 (server converts target's 200 with empty content to 204)
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        self.assertEqual(
            response.headers.get(HEADER_DIRECT_PUT_LENGTH), str(len(result))
        )

        server2.client_put.assert_called_once_with(
            "http://localhost:9023/target", result, headers={}
        )

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
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
    def test_fastapi_to_target_pipeline_chain(self):
        """Test pipeline forwarding between FastAPI servers ending with a target server."""

        # Start two ETL servers
        server1 = self._start_fastapi_server(9041, "step1")
        server2 = self._start_fastapi_server(9042, "step2")

        # Create mock response for target server call
        target_response = AsyncMock()
        target_response.status_code = 200  # Will be converted to 204 by server
        target_response.content = b""
        target_response.headers = {}

        # Mock the direct delivery response (simulate 200 OK)
        server2.client.put = AsyncMock()
        server2.client.put.return_value = target_response

        # Create pipeline header: server1 -> server2 -> target
        pipeline = "http://localhost:9042/transform,http://localhost:9043/target"
        headers = {HEADER_NODE_URL: pipeline}
        content = b"original"
        result = server2.transform(server1.transform(content, "", ""), "", "")

        # Send request to first server with pipeline
        response = requests.put(
            "http://localhost:9041/test", data=content, headers=headers, timeout=5
        )

        # Should get 204 (server converts target's 200 with empty content to 204)
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        self.assertEqual(
            response.headers.get(HEADER_DIRECT_PUT_LENGTH), str(len(result))
        )

        server2.client.put.assert_awaited_once_with(
            "http://localhost:9043/target", content=result, headers={}
        )

    def test_mixed_server_type_pipeline(self):
        """Test pipeline forwarding across different server types."""

        # Start one server of each type
        http_server = self._start_http_server(9031, "http")
        flask_server = self._start_flask_server(9032, "flask")

        # Pipeline: HTTP -> Flask
        pipeline = "http://localhost:9032/transform"
        headers = {HEADER_NODE_URL: pipeline}
        result = flask_server.transform(
            http_server.transform(b"mixed_data", "", ""), "", ""
        )

        response = requests.put(
            "http://localhost:9031/mixed",
            data=b"mixed_data",
            headers=headers,
            timeout=5,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, result)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
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

    def test_pipeline_error_handling(self):
        """Test error handling when pipeline target is unreachable."""

        self._start_http_server(9061, "error_test")

        # Pipeline to non-existent server
        pipeline = "http://localhost:9999/nonexistent"
        headers = {HEADER_NODE_URL: pipeline}

        response = requests.put(
            "http://localhost:9061/error",
            data=b"error_data",
            headers=headers,
            timeout=5,
        )

        self.assertEqual(response.status_code, 500)
        self.assertIn(b"/nonexistent", response.content)
        self.assertIn(b"ConnectionError", response.content)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_long_pipeline_chain(self):
        """Test a longer pipeline with 5 servers."""

        # Start 5 HTTP servers
        content = b"original"
        result = content
        servers = []
        for i in range(5):
            port = 9070 + i
            server = self._start_fastapi_server(port, f"stage{i+1}")
            servers.append(server)
            result = server.transform(result, "", "")

        # Create 5-stage pipeline
        pipeline_stages = [f"http://localhost:{9071 + i}/stage{i+2}" for i in range(4)]
        pipeline = ",".join(pipeline_stages)
        headers = {HEADER_NODE_URL: pipeline}

        response = requests.put(
            "http://localhost:9070/long", data=content, headers=headers, timeout=10
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, result)

    def test_empty_pipeline_header(self):
        """Test behavior with empty pipeline header."""

        server = self._start_http_server(9081, "empty")
        content = b"original"
        result = server.transform(content, "", "")

        # Empty pipeline header should return transformed data normally
        response = requests.put(
            "http://localhost:9081/empty", data=content, headers={}, timeout=5
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, result)

    def test_malformed_pipeline_header(self):
        """Test behavior with malformed pipeline header."""

        self._start_http_server(9091, "malformed")

        # Malformed pipeline (just commas)
        headers = {HEADER_NODE_URL: ",,,,"}

        response = requests.put(
            "http://localhost:9091/malformed",
            data=b"malformed_test",
            headers=headers,
            timeout=5,
        )

        # Should return 400 error due to malformed pipeline
        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid pipeline", response.text)

    def test_concurrent_pipeline_requests(self):
        """Test multiple concurrent requests through pipeline."""

        self._start_http_server(9101, "concurrent1")
        self._start_http_server(9102, "concurrent2")

        pipeline = "http://localhost:9102/concurrent"
        headers = {HEADER_NODE_URL: pipeline}

        def make_request(i):
            return requests.put(
                "http://localhost:9101/concurrent",
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
