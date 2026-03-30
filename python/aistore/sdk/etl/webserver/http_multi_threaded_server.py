#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from socketserver import ThreadingMixIn
from typing import Iterator, Type, Tuple
import signal
import threading
from urllib.parse import urlparse, parse_qs

import requests

from aistore.sdk.etl.webserver.base_etl_server import (
    ETLServer,
    CountingIterator,
    SYNC_DIRECT_PUT_TRANSIENT_ERRORS,
    RETRY_BACKOFF_BASE,
    RETRY_BACKOFF_MAX,
    _is_connection_refused,
)
from aistore.sdk.etl.webserver.utils import (
    compose_etl_direct_put_url,
    parse_etl_pipeline,
)
from aistore.sdk.errors import InvalidPipelineError, ETLDirectPutTransientError
from aistore.sdk.const import (
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    HEADER_NODE_URL,
    HEADER_DIRECT_PUT_LENGTH,
    STATUS_OK,
    STATUS_BAD_GATEWAY,
    QPARAM_ETL_ARGS,
    QPARAM_ETL_FQN,
    STATUS_INTERNAL_SERVER_ERROR,
)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """
    Multi-threaded HTTP server that delegates ETL logic to a provided ETLServer instance.
    """

    def __init__(
        self,
        server_address: Tuple[str, int],
        handler_class: Type[BaseHTTPRequestHandler],
        etl_server: ETLServer,
    ):
        super().__init__(server_address, handler_class)
        self.etl_server = etl_server


class HTTPMultiThreadedServer(ETLServer):
    """
    Multi-threaded HTTP server implementation for ETL payload transformation.
    Handles GET and PUT requests via a request handler class.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        super().__init__()
        self.host = host
        self.port = port

    # pylint: disable=invalid-name
    class RequestHandler(BaseHTTPRequestHandler):
        """
        Request handler for the ETL HTTP server.
        Forwards GET/PUT requests to the AIS target and applies transformation logic.
        """

        def _set_headers(
            self,
            status_code: int = STATUS_OK,
            length: int = 0,
            direct_put_length: int = 0,
        ):
            self.send_response(status_code)
            mime_type = self.server.etl_server.get_mime_type()
            self.send_header(HEADER_CONTENT_TYPE, mime_type)
            self.send_header(HEADER_CONTENT_LENGTH, str(length))
            if direct_put_length != 0:
                self.send_header(HEADER_DIRECT_PUT_LENGTH, str(direct_put_length))
            self.end_headers()

        def log_request(self, code="-", size="-"):
            # Suppress default request logging (or override as needed)
            pass

        def _direct_put(
            self,
            direct_put_url: str,
            data: bytes,
            remaining_pipeline: str = "",
            path: str = "",
        ) -> Tuple[int, bytes, int]:
            """
            Sends the transformed object directly to the specified AIS node (`direct_put_url`),
            eliminating the additional network hop through the original target.
            Used only in bucket-to-bucket offline transforms.

            Args:
                direct_put_url: The first URL in the ETL pipeline
                data: The transformed data to send
                remaining_pipeline: Comma-separated remaining pipeline stages to pass as header
                path: The path of the object.
            Returns:
                status code of the direct put request, transformed data, length of the transformed data (if any)
            """
            try:
                url = compose_etl_direct_put_url(
                    direct_put_url, self.server.etl_server.host_target, path
                )
                headers = {}
                if remaining_pipeline:
                    headers[HEADER_NODE_URL] = remaining_pipeline

                resp = self.server.etl_server.client_put(url, data, headers=headers)
                return self.server.etl_server.handle_direct_put_response(resp, data)

            except SYNC_DIRECT_PUT_TRANSIENT_ERRORS as exc:
                if isinstance(exc, requests.ConnectionError) and _is_connection_refused(exc):
                    error = f"direct_put to {direct_put_url!r} failed: {type(exc).__name__}: {exc}".encode()
                    self.server.etl_server.logger.error(
                        "Permanent connection error to %s: %s", direct_put_url, exc
                    )
                    return STATUS_BAD_GATEWAY, error, 0
                raise ETLDirectPutTransientError(direct_put_url, exc) from exc
            except Exception as e:
                error = str(e).encode()
                self.server.etl_server.logger.error(
                    "Exception during direct put to %s: %s", direct_put_url, e
                )
                return STATUS_INTERNAL_SERVER_ERROR, error, 0

        def _get_fqn_content(self, path: str) -> bytes:
            """
            Parses and safely reads a file when using FQN (fully qualified name) input.
            """
            safe_path = self.server.etl_server.sanitize_fqn(
                path
            )  # pylint: disable=protected-access
            self.server.etl_server.logger.debug("Reading local file: %s", safe_path)
            with open(safe_path, "rb") as f:
                return f.read()

        def _get_stream_reader(self, fqn, raw_path, is_get):
            """Get a BinaryIO reader for the request source data."""
            etl_server = self.server.etl_server
            if fqn:
                return open(etl_server.sanitize_fqn(fqn), "rb")
            if is_get:
                target_url = f"{etl_server.host_target}{raw_path}"
                etl_server.logger.debug(
                    "Forwarding GET (stream) to AIS target: %s", target_url
                )
                resp = etl_server.session.get(target_url, stream=True, timeout=None)
                if resp.status_code != STATUS_OK:
                    resp.close()
                    return None
                return resp.raw
            content_length = int(self.headers.get(HEADER_CONTENT_LENGTH, 0))
            return BytesIO(self.rfile.read(content_length))

        def _direct_put_with_retry(
            self,
            direct_put_url: str,
            data: bytes,
            remaining_pipeline: str = "",
            path: str = "",
        ) -> Tuple[int, bytes, int]:
            """Buffered direct-put with exponential-backoff retry on transient errors."""
            etl = self.server.etl_server
            for attempt in range(etl.direct_put_retries + 1):
                try:
                    return self._direct_put(
                        direct_put_url, data, remaining_pipeline, path
                    )
                except ETLDirectPutTransientError as exc:
                    if attempt >= etl.direct_put_retries:
                        raise
                    delay = min(RETRY_BACKOFF_BASE**attempt, RETRY_BACKOFF_MAX)
                    etl.logger.warning(
                        "direct_put attempt %d/%d failed, retrying in %.1fs: %s",
                        attempt + 1,
                        etl.direct_put_retries + 1,
                        delay,
                        exc,
                        exc_info=True,
                    )
                    time.sleep(delay)
            raise AssertionError("unreachable: loop always returns or raises")

        def _direct_put_stream_with_retry(  # pylint: disable=too-many-arguments,too-many-positional-arguments
            self,
            direct_put_url: str,
            fqn: str,
            raw_path: str,
            etl_args: str,
            is_get: bool,
            remaining_pipeline: str = "",
        ) -> Tuple[int, bytes, int]:
            """
            Streaming direct-put with exponential-backoff retry on transient errors.

            For FQN and GET sources, the reader is closed and reopened on each retry.
            For PUT requests without FQN, the body is buffered in a BytesIO by
            _get_stream_reader and seeked back to 0 on retry so the same bytes are
            replayed (mirrors FastAPIServer._direct_put_stream_with_retry).
            """
            etl = self.server.etl_server
            reader = self._get_stream_reader(fqn, raw_path, is_get)
            if reader is None:
                return (
                    STATUS_BAD_GATEWAY,
                    b"Failed to retrieve data from target",
                    0,
                )
            try:
                for attempt in range(etl.direct_put_retries + 1):
                    try:
                        return self._direct_put_stream(
                            direct_put_url,
                            etl.transform_stream(reader, raw_path, etl_args),
                            remaining_pipeline,
                            raw_path,
                        )
                    except ETLDirectPutTransientError as exc:
                        if attempt >= etl.direct_put_retries:
                            raise
                        delay = min(RETRY_BACKOFF_BASE**attempt, RETRY_BACKOFF_MAX)
                        etl.logger.warning(
                            "direct_put_stream attempt %d/%d failed, retrying in %.1fs: %s",
                            attempt + 1,
                            etl.direct_put_retries + 1,
                            delay,
                            exc,
                            exc_info=True,
                        )
                        if isinstance(reader, BytesIO):
                            reader.seek(0)
                        else:
                            etl.close_reader(reader)
                            reader = self._get_stream_reader(fqn, raw_path, is_get)
                            if reader is None:
                                raise
                        time.sleep(delay)
            finally:
                etl.close_reader(reader)
            raise AssertionError("unreachable: loop always returns or raises")

        def _send_streaming_response(self, output_iter):
            """Stream transformed output directly to the client (no pipeline)."""
            self.send_response(STATUS_OK)
            mime_type = self.server.etl_server.get_mime_type()
            self.send_header(HEADER_CONTENT_TYPE, mime_type)
            self.end_headers()
            for chunk in output_iter:
                if chunk:
                    self.wfile.write(chunk)
            self.wfile.flush()

        def _direct_put_stream(
            self,
            direct_put_url: str,
            data_iter: Iterator[bytes],
            remaining_pipeline: str = "",
            path: str = "",
        ) -> Tuple[int, bytes, int]:
            """Stream transformed output directly to the next pipeline stage."""
            try:
                url = compose_etl_direct_put_url(
                    direct_put_url, self.server.etl_server.host_target, path
                )
                headers = {}
                if remaining_pipeline:
                    headers[HEADER_NODE_URL] = remaining_pipeline

                counted = CountingIterator(data_iter)
                resp = self.server.etl_server.session.put(
                    url, data=counted, timeout=None, headers=headers
                )
                return self.server.etl_server.handle_direct_put_response(
                    resp, b"", data_length=counted.bytes_sent
                )

            except SYNC_DIRECT_PUT_TRANSIENT_ERRORS as exc:
                if isinstance(exc, requests.ConnectionError) and _is_connection_refused(exc):
                    error = f"direct_put to {direct_put_url!r} failed: {type(exc).__name__}: {exc}".encode()
                    self.server.etl_server.logger.error(
                        "Permanent connection error to %s: %s", direct_put_url, exc
                    )
                    return STATUS_BAD_GATEWAY, error, 0
                raise ETLDirectPutTransientError(direct_put_url, exc) from exc
            except Exception as e:
                root = e.__cause__ or e
                self.server.etl_server.logger.error(
                    "streaming direct put to %s failed after %d bytes: %s: %s",
                    direct_put_url,
                    counted.bytes_sent,
                    type(root).__name__,
                    root,
                    exc_info=True,
                )
                return STATUS_INTERNAL_SERVER_ERROR, str(e).encode(), 0

        def _send_with_pipeline(self, transformed: bytes, path: str):
            """
            Forward transformed data to the next ETL stage if a pipeline header exists;
            otherwise, respond directly with the transformed data.

            Args:
                transformed (bytes): The transformed data to be sent.
                path (str): The path of the object.
            """
            pipeline_header = self.headers.get(HEADER_NODE_URL)
            if pipeline_header:
                first_url, remaining_pipeline = parse_etl_pipeline(pipeline_header)
                if first_url:
                    status_code, transformed, direct_put_length = (
                        self._direct_put_with_retry(
                            first_url, transformed, remaining_pipeline, path
                        )
                    )
                    self._set_headers(
                        status_code=status_code,
                        length=len(transformed),
                        direct_put_length=direct_put_length,
                    )
                    if transformed:
                        self.wfile.write(transformed)
                    return

            self._set_headers(
                status_code=STATUS_OK,
                length=len(transformed),
                direct_put_length=0,
            )
            self.wfile.write(transformed)

        def _handle_streaming(self, fqn, raw_path, etl_args, is_get):
            """Handle a streaming GET or PUT request."""
            pipeline_header = self.headers.get(HEADER_NODE_URL)
            if pipeline_header:
                first_url, remaining_pipeline = parse_etl_pipeline(pipeline_header)
                if first_url:
                    result = self._direct_put_stream_with_retry(
                        first_url, fqn, raw_path, etl_args, is_get, remaining_pipeline
                    )
                    self._set_headers(
                        status_code=result[0],
                        length=len(result[1]),
                        direct_put_length=result[2],
                    )
                    if result[1]:
                        self.wfile.write(result[1])
                    return

            # No pipeline: original reader-based streaming path
            reader = self._get_stream_reader(fqn, raw_path, is_get=is_get)
            if reader is None:
                self.send_error(
                    STATUS_BAD_GATEWAY, "Failed to retrieve data from target"
                )
                return
            try:
                output_iter = self.server.etl_server.transform_stream(
                    reader, raw_path, etl_args
                )
                self._send_streaming_response(output_iter)
            finally:
                self.server.etl_server.close_reader(reader)

        # pylint: disable=too-many-locals
        def do_GET(self):
            """
            Handle GET requests by forwarding them to the AIS target or reading from FQN,
            applying the ETL transformation and returning the result.
            """

            logger = self.server.etl_server.logger

            parsed = urlparse(self.path)
            raw_path = parsed.path
            params = parse_qs(parsed.query)
            etl_args = params.get(QPARAM_ETL_ARGS, [""])[0].strip()
            fqn = params.get(QPARAM_ETL_FQN, [""])[0].strip()

            logger.debug(
                "Received GET request for path: %s, etl_args: %s, fqn: %s",
                raw_path,
                etl_args,
                fqn,
            )

            # Health check
            if raw_path == "/health":
                resp = b"Running"
                self._set_headers(length=len(resp))
                self.wfile.write(resp)
                return

            try:
                if self.server.etl_server.use_streaming:
                    self._handle_streaming(fqn, raw_path, etl_args, is_get=True)
                    return

                if fqn and self.server.etl_server.direct_fqn:
                    source = self.server.etl_server.sanitize_fqn(
                        fqn
                    )  # pylint: disable=protected-access
                elif fqn:
                    source = self._get_fqn_content(fqn)
                else:
                    target_url = f"{self.server.etl_server.host_target}{raw_path}"
                    logger.debug("Forwarding GET to AIS target: %s", target_url)

                    resp = self.server.etl_server.session.get(target_url, timeout=None)

                    if resp.status_code != STATUS_OK:
                        logger.warning(
                            "Failed to fetch from target %s (status %d)",
                            target_url,
                            resp.status_code,
                        )
                        self.send_error(
                            resp.status_code, "Failed to retrieve data from target"
                        )
                        return

                    source = resp.content

                transformed = self.server.etl_server.transform(
                    source, raw_path, etl_args
                )

                self._send_with_pipeline(transformed, raw_path)

            except InvalidPipelineError as e:
                self.server.etl_server.logger.error(
                    "Invalid pipeline header: %s", str(e)
                )
                self.send_error(400, f"Invalid pipeline header: {str(e)}")
            except FileNotFoundError as exc:
                fs_path = exc.filename or raw_path
                logger.error(
                    "Error processing request for %r: file not found at %r",
                    raw_path,
                    fs_path,
                )
                self.send_error(
                    404,
                    (
                        f"Error processing object {raw_path!r}: file not found at {fs_path!r}. "
                    ),
                )
            except ETLDirectPutTransientError as e:
                self.server.etl_server.logger.error(
                    "Direct put failed after retries: %s", e
                )
                self.send_error(
                    STATUS_BAD_GATEWAY, f"Direct put failed after retries: {e}"
                )
            except requests.RequestException as e:
                logger.error("Request to AIS target failed: %s", str(e))
                self.send_error(500, f"Error contacting AIS target: {e}")

            except Exception as e:
                logger.exception("Unexpected error processing GET request")
                self.send_error(500, f"Unhandled error: {e}")

        def do_PUT(self):
            """
            Handle PUT requests by transforming the incoming data and responding with the transformed data.
            """
            logger = self.server.etl_server.logger
            parsed = urlparse(self.path)
            raw_path = parsed.path
            params = parse_qs(parsed.query)
            etl_args = params.get(QPARAM_ETL_ARGS, [""])[0].strip()
            fqn = params.get(QPARAM_ETL_FQN, [""])[0].strip()

            logger.debug(
                "Received PUT request for path: %s, etl_args: %s, fqn: %s",
                raw_path,
                etl_args,
                fqn,
            )

            try:
                if self.server.etl_server.use_streaming:
                    self._handle_streaming(fqn, raw_path, etl_args, is_get=False)
                    return

                if fqn and self.server.etl_server.direct_fqn:
                    source = self.server.etl_server.sanitize_fqn(
                        fqn
                    )  # pylint: disable=protected-access
                elif fqn:
                    source = self._get_fqn_content(fqn)
                else:
                    content_length = int(self.headers.get(HEADER_CONTENT_LENGTH, 0))
                    source = self.rfile.read(content_length)
                transformed = self.server.etl_server.transform(
                    source, raw_path, etl_args
                )

                self._send_with_pipeline(transformed, raw_path)

            except InvalidPipelineError as e:
                self.server.etl_server.logger.error(
                    "Invalid pipeline header: %s", str(e)
                )
                self.send_error(400, f"Invalid pipeline header: {str(e)}")
            except FileNotFoundError:
                logger.error("File not found: %s", raw_path)
                self.send_error(404, f"Local file not found: {raw_path}")
            except ETLDirectPutTransientError as e:
                self.server.etl_server.logger.error(
                    "Direct put failed after retries: %s", e
                )
                self.send_error(
                    STATUS_BAD_GATEWAY, f"Direct put failed after retries: {e}"
                )
            except Exception as e:
                logger.error("Error processing PUT request: %s", str(e))
                self.send_error(500, "Internal error during transformation")

    def start(self):
        """
        Starts the HTTP server and gracefully handles SIGTERM/SIGINT for Kubernetes.
        """
        server = ThreadedHTTPServer(
            (self.host, self.port), self.RequestHandler, etl_server=self
        )
        self.logger.info(
            "HTTP Multi-threaded server running at %s:%s", self.host, self.port
        )

        # Register shutdown handler
        def shutdown_handler(signum, _frame):
            self.logger.info("Received signal %s: shutting down.", signum)
            threading.Thread(target=server.shutdown).start()

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)

        try:
            server.serve_forever()
        finally:
            server.server_close()
            self.logger.info("Server shutdown complete.")
