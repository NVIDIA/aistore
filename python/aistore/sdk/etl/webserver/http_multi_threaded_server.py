#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import io
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import BinaryIO, Iterator, Type, Tuple
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
    DRAIN_CHUNK_BYTES,
    _handle_direct_put_transient_error,
    _compute_replayable_retries,
)
from aistore.sdk.etl.webserver.utils import (
    compose_etl_direct_put_url,
    parse_etl_pipeline,
    _ResponseRawReader,
)
from aistore.sdk.errors import InvalidPipelineError, ETLDirectPutTransientError
from aistore.sdk.const import (
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    HEADER_NODE_URL,
    HEADER_DIRECT_PUT_LENGTH,
    HEADER_ETL_RETRY_REASON,
    ETL_RETRY_REASON_DIRECT_PUT_TRANSIENT,
    STATUS_OK,
    STATUS_BAD_GATEWAY,
    STATUS_SERVICE_UNAVAILABLE,
    QPARAM_ETL_ARGS,
    QPARAM_ETL_FQN,
    STATUS_INTERNAL_SERVER_ERROR,
)


class _RFileLimitedReader(io.RawIOBase):
    """Bound `BaseHTTPRequestHandler.rfile` to the current PUT body length.

    `self.rfile` is the raw connection stream; it has no intrinsic EOF at the
    end of this request body. Passing it directly to `transform_stream` would
    cause any transform that calls `reader.read()` with no size argument to
    block indefinitely waiting for the client to close the connection.

    This wrapper tracks `Content-Length` remaining bytes and clamps every
    `read()` call accordingly, giving transforms the same EOF semantics they
    get from a `BytesIO` — without buffering the full body upfront.

    The request body is one-shot; `_direct_put_stream_with_retry` sets
    `effective_retries=0` on this path. `close()` drains any unread bytes
    from the request body so a transform that exits early does not leave
    residual data on a keep-alive connection.
    """

    def __init__(self, rfile: BinaryIO, content_length: int) -> None:
        self._rfile = rfile
        self._remaining = content_length

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._remaining == 0:
            return b""
        if size is None or size < 0:
            data = self._rfile.read(self._remaining)
            self._remaining = 0
            return data
        to_read = min(size, self._remaining)
        data = self._rfile.read(to_read)
        self._remaining -= len(data)
        return data

    def close(self) -> None:
        try:
            while self._remaining > 0:
                try:
                    chunk = self._rfile.read(min(self._remaining, DRAIN_CHUNK_BYTES))
                except (OSError, ValueError):
                    break
                if not chunk:
                    break
                self._remaining -= len(chunk)
        finally:
            super().close()


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

        def _send_direct_put_bail_503(self, exc: ETLDirectPutTransientError) -> None:
            """Emit 503 + `Ais-Etl-Retry-Reason: direct-put-transient`.

            `BaseHTTPRequestHandler.send_error()` cannot attach custom headers,
            so the response is built explicitly here. AIS uses the
            (status, header) pair as the signal to retry the whole PUT
            against the replayable LOM-backed source.
            """
            body = f"Direct put bailed (one-shot body): {exc}".encode()
            self.send_response(STATUS_SERVICE_UNAVAILABLE)
            self.send_header(
                HEADER_ETL_RETRY_REASON, ETL_RETRY_REASON_DIRECT_PUT_TRANSIENT
            )
            self.send_header(HEADER_CONTENT_TYPE, "text/plain; charset=utf-8")
            self.send_header(HEADER_CONTENT_LENGTH, str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _direct_put(  # pylint: disable=too-many-arguments,too-many-positional-arguments
            self,
            direct_put_url: str,
            data: bytes,
            remaining_pipeline: str = "",
            path: str = "",
            etl_args: str = "",
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
                etl_args: Per-request transform arguments to forward to the next stage.
            Returns:
                status code of the direct put request, transformed data, length of the transformed data (if any)
            """
            try:
                url = compose_etl_direct_put_url(
                    direct_put_url, self.server.etl_server.host_target, path, etl_args
                )
                headers = {}
                if remaining_pipeline:
                    headers[HEADER_NODE_URL] = remaining_pipeline

                resp = self.server.etl_server.client_put(url, data, headers=headers)
                return self.server.etl_server.handle_direct_put_response(resp, data)

            except SYNC_DIRECT_PUT_TRANSIENT_ERRORS as exc:
                return _handle_direct_put_transient_error(
                    direct_put_url, exc, self.server.etl_server.logger
                )
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
                try:
                    resp.raise_for_status()
                except requests.HTTPError:
                    resp.close()
                    raise
                return _ResponseRawReader(resp)
            # Request body is one-shot; local retries are skipped for this path
            # (see _direct_put_stream_with_retry).
            content_length = int(self.headers.get(HEADER_CONTENT_LENGTH, 0))
            return _RFileLimitedReader(self.rfile, content_length)

        def _direct_put_with_retry(  # pylint: disable=too-many-arguments,too-many-positional-arguments
            self,
            direct_put_url: str,
            data: bytes,
            remaining_pipeline: str = "",
            path: str = "",
            etl_args: str = "",
        ) -> Tuple[int, bytes, int]:
            """Buffered direct-put with exponential-backoff retry on transient errors."""
            etl = self.server.etl_server
            for attempt in range(etl.direct_put_retries + 1):
                try:
                    return self._direct_put(
                        direct_put_url, data, remaining_pipeline, path, etl_args
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

            Replayable sources (FQN-backed or GET) close and reopen the reader on
            each retry. No-FQN PUT bodies are one-shot (request body is consumed
            from the socket); effective_retries is forced to 0 and a transient
            direct-put error surfaces to AIS as a transform failure.
            """
            etl = self.server.etl_server
            replayable, effective_retries = _compute_replayable_retries(
                fqn, is_get, etl.direct_put_retries
            )
            reader = self._get_stream_reader(fqn, raw_path, is_get)
            try:
                for attempt in range(effective_retries + 1):
                    try:
                        return self._direct_put_stream(
                            direct_put_url,
                            etl.transform_stream(reader, raw_path, etl_args),
                            remaining_pipeline,
                            raw_path,
                            etl_args,
                        )
                    except ETLDirectPutTransientError as exc:
                        if attempt >= effective_retries:
                            if not replayable:
                                # Tag the bail-without-local-retry case so the
                                # outer handler can ask AIS to retry (see
                                # contract on ETLDirectPutTransientError).
                                exc.bail_without_local_retry = True
                                if etl.direct_put_retries:
                                    etl.logger.debug(
                                        "no-FQN PUT: source not replayable; "
                                        "local retries skipped; transient direct-put error "
                                        "will surface as transform failure"
                                    )
                            raise
                        delay = min(RETRY_BACKOFF_BASE**attempt, RETRY_BACKOFF_MAX)
                        etl.logger.warning(
                            "direct_put_stream attempt %d/%d failed, retrying in %.1fs: %s",
                            attempt + 1,
                            effective_retries + 1,
                            delay,
                            exc,
                            exc_info=True,
                        )
                        etl.close_reader(reader)
                        reader = self._get_stream_reader(fqn, raw_path, is_get)
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

        def _direct_put_stream(  # pylint: disable=too-many-arguments,too-many-positional-arguments
            self,
            direct_put_url: str,
            data_iter: Iterator[bytes],
            remaining_pipeline: str = "",
            path: str = "",
            etl_args: str = "",
        ) -> Tuple[int, bytes, int]:
            """Stream transformed output directly to the next pipeline stage."""
            try:
                url = compose_etl_direct_put_url(
                    direct_put_url, self.server.etl_server.host_target, path, etl_args
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
                return _handle_direct_put_transient_error(
                    direct_put_url, exc, self.server.etl_server.logger
                )
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

        def _send_with_pipeline(
            self, transformed: bytes, path: str, etl_args: str = ""
        ):
            """
            Forward transformed data to the next ETL stage if a pipeline header exists;
            otherwise, respond directly with the transformed data.

            Args:
                transformed (bytes): The transformed data to be sent.
                path (str): The path of the object.
                etl_args (str): Per-request transform arguments to forward to the next stage.
            """
            pipeline_header = self.headers.get(HEADER_NODE_URL)
            if pipeline_header:
                first_url, remaining_pipeline = parse_etl_pipeline(pipeline_header)
                if first_url:
                    status_code, transformed, direct_put_length = (
                        self._direct_put_with_retry(
                            first_url, transformed, remaining_pipeline, path, etl_args
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
            try:
                output_iter = self.server.etl_server.transform_stream(
                    reader, raw_path, etl_args
                )
                self._send_streaming_response(output_iter)
            finally:
                self.server.etl_server.close_reader(reader)

        # pylint: disable=too-many-locals,too-many-statements
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

                self._send_with_pipeline(transformed, raw_path, etl_args)

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
            except requests.HTTPError as e:
                status = (
                    e.response.status_code
                    if e.response is not None
                    else STATUS_BAD_GATEWAY
                )
                logger.debug("Upstream GET returned %s", status)
                self.send_error(status, "Target request failed")
            except requests.RequestException as e:
                logger.error("Request to AIS target failed: %s", str(e))
                self.send_error(STATUS_BAD_GATEWAY, f"Error contacting AIS target: {e}")

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

                self._send_with_pipeline(transformed, raw_path, etl_args)

            except InvalidPipelineError as e:
                self.server.etl_server.logger.error(
                    "Invalid pipeline header: %s", str(e)
                )
                self.send_error(400, f"Invalid pipeline header: {str(e)}")
            except FileNotFoundError:
                logger.error("File not found: %s", raw_path)
                self.send_error(404, f"Local file not found: {raw_path}")
            except ETLDirectPutTransientError as e:
                # Contract with AIS: 503 + `Ais-Etl-Retry-Reason: direct-put-transient`
                # fires only when the ETL bailed without trying locally (one-shot
                # body — currently the streaming no-FQN PUT path). Exhausted-retry
                # cases keep emitting 502: the ETL already tried and AIS retrying
                # on top is just amplification.
                if e.bail_without_local_retry:
                    self.server.etl_server.logger.error(
                        "Direct put bailed (one-shot body); AIS will retry: %s", e
                    )
                    self._send_direct_put_bail_503(e)
                else:
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
