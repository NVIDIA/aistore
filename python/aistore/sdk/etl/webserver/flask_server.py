#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import time
from io import BytesIO
from urllib.parse import quote
from typing import Iterator, Tuple

import requests
from flask import Flask, request, Response, jsonify

from aistore.sdk.etl.webserver.base_etl_server import (
    ETLServer,
    CountingIterator,
    SYNC_DIRECT_PUT_TRANSIENT_ERRORS,
    RETRY_BACKOFF_BASE,
    RETRY_BACKOFF_MAX,
    _handle_direct_put_transient_error,
)
from aistore.sdk.etl.webserver.utils import (
    compose_etl_direct_put_url,
    parse_etl_pipeline,
)
from aistore.sdk.errors import InvalidPipelineError, ETLDirectPutTransientError
from aistore.sdk.const import (
    HEADER_NODE_URL,
    STATUS_OK,
    STATUS_BAD_GATEWAY,
    QPARAM_ETL_ARGS,
    QPARAM_ETL_FQN,
    HEADER_DIRECT_PUT_LENGTH,
    STATUS_INTERNAL_SERVER_ERROR,
)


class FlaskServer(ETLServer):
    """
    Flask server implementation for ETL transformations.
    Compatible with environments where Flask is preferred over FastAPI.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        super().__init__()
        self.host = host
        self.port = port
        self.app = Flask(__name__)
        self._register_routes()

    def _register_routes(self):
        self.app.add_url_rule("/health", view_func=self._health, methods=["GET"])
        self.app.add_url_rule(
            "/<path:path>", view_func=self._handle_request, methods=["GET", "PUT"]
        )

    def _health(self):
        return Response(response=b"Running", status=200)

    def _handle_request(self, path):  # pylint: disable=too-many-return-statements
        try:
            if self.use_streaming:
                return self._handle_request_streaming(path)
            return self._handle_request_buffered(path)
        except InvalidPipelineError as e:
            self.logger.error("Invalid pipeline header: %s", str(e))
            return Response(f"Invalid pipeline header: {str(e)}", status=400)
        except FileNotFoundError as exc:
            fs_path = exc.filename or path
            self.logger.error(
                "Error processing object %r: file not found at %r",
                path,
                fs_path,
            )
            return (
                jsonify(
                    {
                        "error": (
                            f"Error processing object {path!r}: file not found at {fs_path!r}. "
                        )
                    }
                ),
                404,
            )
        except ETLDirectPutTransientError as e:
            self.logger.error(
                "Direct put failed after %d retries: %s", self.direct_put_retries, e
            )
            return jsonify({"error": str(e)}), STATUS_BAD_GATEWAY
        except requests.RequestException as e:
            self.logger.error("Request to AIS target failed: %s", str(e))
            return jsonify({"error": str(e)}), STATUS_BAD_GATEWAY
        except Exception as e:
            self.logger.exception("Unhandled error")
            return jsonify({"error": str(e)}), 500

    def _handle_request_buffered(self, path):
        transformed = None
        if request.method == "GET":
            transformed = self._handle_get(path)
        else:
            transformed = self._handle_put(path)

        pipeline_header = request.headers.get(HEADER_NODE_URL)
        self.logger.debug("pipeline_header: %r", pipeline_header)
        if pipeline_header:
            first_url, remaining_pipeline = parse_etl_pipeline(pipeline_header)
            if first_url:
                status_code, transformed, direct_put_length = (
                    self._direct_put_with_retry(
                        first_url, transformed, remaining_pipeline, path
                    )
                )
                return Response(
                    response=transformed,
                    status=status_code,
                    headers=(
                        {HEADER_DIRECT_PUT_LENGTH: str(direct_put_length)}
                        if direct_put_length != 0
                        else {}
                    ),
                )

        return Response(
            response=transformed,
            status=STATUS_OK,
            content_type=self.get_mime_type(),
        )

    def _handle_request_streaming(self, path):
        etl_args = request.args.get(QPARAM_ETL_ARGS, "").strip()

        pipeline_header = request.headers.get(HEADER_NODE_URL)
        self.logger.debug("pipeline_header: %r", pipeline_header)
        if pipeline_header:
            first_url, remaining = parse_etl_pipeline(pipeline_header)
            if first_url:
                result = self._direct_put_stream_with_retry(
                    first_url, path, remaining, etl_args
                )
                return Response(
                    response=result[1],
                    status=result[0],
                    headers=self.make_direct_put_headers(result[2]),
                )

        # No pipeline: stream directly to client
        reader = self._get_stream_reader(path)
        try:
            output_iter = self.transform_stream(reader, path, etl_args)
            return Response(
                response=self.iter_and_close(output_iter, reader),
                status=STATUS_OK,
                content_type=self.get_mime_type(),
            )
        except Exception:
            self.close_reader(reader)
            raise

    def _get_stream_reader(self, path, buffered=False):
        """Get a BinaryIO reader for the request source data.

        Args:
            path: The object path being processed.
            buffered: If True, buffer the PUT body into a BytesIO so it can be
                replayed on retry. If False (default), return request.stream for
                true constant-memory streaming (no-pipeline path only).
        """
        fqn = request.args.get(QPARAM_ETL_FQN, "").strip()
        if fqn:
            # S2083: FQN is an internal AIS target path, not external user input.
            # sanitize_fqn() normalizes via os.path.normpath to prevent traversal.
            return open(self.sanitize_fqn(fqn), "rb")  # noqa: S2083
        if request.method == "GET":
            obj_path = quote(path, safe="@")
            target_url = f"{self.host_target}/{obj_path}"
            self.logger.debug("Forwarding GET (stream) to: %s", target_url)
            resp = self.session.get(target_url, stream=True, timeout=None)
            resp.raise_for_status()
            return resp.raw
        if buffered:
            return BytesIO(request.get_data())
        return request.stream

    def _direct_put_with_retry(
        self,
        direct_put_url: str,
        data: bytes,
        remaining_pipeline: str = "",
        path: str = "",
    ) -> Tuple[int, bytes, int]:
        """Buffered direct-put with exponential-backoff retry on transient errors."""
        for attempt in range(self.direct_put_retries + 1):
            try:
                return self._direct_put(direct_put_url, data, remaining_pipeline, path)
            except ETLDirectPutTransientError as exc:
                if attempt >= self.direct_put_retries:
                    raise
                delay = min(RETRY_BACKOFF_BASE**attempt, RETRY_BACKOFF_MAX)
                self.logger.warning(
                    "direct_put attempt %d/%d failed, retrying in %.1fs: %s",
                    attempt + 1,
                    self.direct_put_retries + 1,
                    delay,
                    exc,
                    exc_info=True,
                )
                time.sleep(delay)
        raise AssertionError("unreachable: loop always returns or raises")

    def _direct_put_stream_with_retry(
        self,
        direct_put_url: str,
        path: str,
        remaining_pipeline: str = "",
        etl_args: str = "",
    ) -> Tuple[int, bytes, int]:
        """
        Streaming direct-put with exponential-backoff retry on transient errors.

        Manages reader lifecycle internally, reopening the source on each retry
        (mirrors FastAPIServer._direct_put_stream_with_retry). For PUT requests
        without FQN, the body is buffered into a BytesIO via get_data() so retries
        replay the same bytes.
        """
        reader = self._get_stream_reader(path, buffered=True)
        try:
            for attempt in range(self.direct_put_retries + 1):
                try:
                    return self._direct_put_stream(
                        direct_put_url,
                        self.transform_stream(reader, path, etl_args),
                        remaining_pipeline,
                        path,
                    )
                except ETLDirectPutTransientError as exc:
                    if attempt >= self.direct_put_retries:
                        raise
                    delay = min(RETRY_BACKOFF_BASE**attempt, RETRY_BACKOFF_MAX)
                    self.logger.warning(
                        "direct_put_stream attempt %d/%d failed, retrying in %.1fs: %s",
                        attempt + 1,
                        self.direct_put_retries + 1,
                        delay,
                        exc,
                        exc_info=True,
                    )
                    self.close_reader(reader)
                    reader = self._get_stream_reader(path, buffered=True)
                    time.sleep(delay)
        finally:
            self.close_reader(reader)
        raise AssertionError("unreachable: loop always returns or raises")

    def _direct_put_stream(
        self,
        direct_put_url: str,
        data_iter: Iterator[bytes],
        remaining_pipeline: str = "",
        path: str = "",
    ) -> Tuple[int, bytes, int]:
        """Stream transformed output directly to the next pipeline stage."""
        try:
            url = compose_etl_direct_put_url(direct_put_url, self.host_target, path)
            headers = {}
            if remaining_pipeline:
                headers[HEADER_NODE_URL] = remaining_pipeline

            counted = CountingIterator(data_iter)
            resp = self.session.put(url, data=counted, timeout=None, headers=headers)
            return self.handle_direct_put_response(
                resp, b"", data_length=counted.bytes_sent
            )

        except SYNC_DIRECT_PUT_TRANSIENT_ERRORS as exc:
            return _handle_direct_put_transient_error(direct_put_url, exc, self.logger)
        except Exception as e:
            root = e.__cause__ or e
            self.logger.error(
                "streaming direct put to %s failed after %d bytes: %s: %s",
                direct_put_url,
                counted.bytes_sent,
                type(root).__name__,
                root,
                exc_info=True,
            )
            return STATUS_INTERNAL_SERVER_ERROR, str(e).encode(), 0

    def _handle_get(self, path):
        etl_args = request.args.get(QPARAM_ETL_ARGS, "").strip()
        fqn = request.args.get(QPARAM_ETL_FQN, "").strip()
        if fqn and self.direct_fqn:
            source = self.sanitize_fqn(fqn)
        elif fqn:
            source = self._get_fqn_content(fqn)
        else:
            obj_path = quote(path, safe="@")
            target_url = f"{self.host_target}/{obj_path}"
            self.logger.debug("Forwarding GET to: %s", target_url)
            resp = self.session.get(target_url, timeout=None)
            resp.raise_for_status()
            source = resp.content
        return self.transform(source, path, etl_args)

    def _handle_put(self, path):
        etl_args = request.args.get(QPARAM_ETL_ARGS, "").strip()
        fqn = request.args.get(QPARAM_ETL_FQN, "").strip()
        if fqn and self.direct_fqn:
            source = self.sanitize_fqn(fqn)
        elif fqn:
            source = self._get_fqn_content(fqn)
        else:
            source = request.get_data()
        return self.transform(source, path, etl_args)

    def _get_fqn_content(self, path: str) -> bytes:
        safe_path = self.sanitize_fqn(path)
        self.logger.debug("Reading local file: %s", safe_path)
        with open(safe_path, "rb") as f:
            return f.read()

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
            status code, transformed data, length of the transformed data (if any)
        """
        try:
            url = compose_etl_direct_put_url(direct_put_url, self.host_target, path)
            headers = {}
            if remaining_pipeline:
                headers[HEADER_NODE_URL] = remaining_pipeline

            resp = self.client_put(url, data, headers=headers)
            return self.handle_direct_put_response(resp, data)

        except SYNC_DIRECT_PUT_TRANSIENT_ERRORS as exc:
            return _handle_direct_put_transient_error(direct_put_url, exc, self.logger)
        except Exception as e:
            error = str(e).encode()
            self.logger.error("Exception in direct put to %s: %s", direct_put_url, e)
            return STATUS_INTERNAL_SERVER_ERROR, error, 0

    # Example Gunicorn command to run this server:
    # command: ["gunicorn", "your_module:flask_app", "--bind", "0.0.0.0:8000", "--workers", "4"]
    def start(self):
        self.logger.info("Starting Flask ETL server at %s:%d", self.host, self.port)
        self.app.run(
            host=self.host, port=self.port, threaded=True
        )  # threaded=True for multi-threaded support
