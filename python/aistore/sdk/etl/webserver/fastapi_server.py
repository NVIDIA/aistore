#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

import os
import io
import asyncio
from collections.abc import AsyncIterable
from urllib.parse import quote
from typing import BinaryIO, Iterator, Optional, List, Tuple

from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    Response,
    WebSocket,
)
from starlette.concurrency import iterate_in_threadpool
from anyio import from_thread
import httpx
import requests
import aiofiles
import uvicorn

from aistore.sdk.etl.webserver.base_etl_server import (
    ETLServer,
    CountingIterator,
    RETRY_BACKOFF_BASE,
    RETRY_BACKOFF_MAX,
)
from aistore.sdk.session_manager import resolve_ssl_config
from aistore.sdk.etl.webserver.utils import (
    compose_etl_direct_put_url,
    parse_etl_pipeline,
    _ResponseRawReader,
)
from aistore.sdk.errors import InvalidPipelineError, ETLDirectPutTransientError
from aistore.sdk.const import (
    MIB,
    AIS_DIRECT_PUT_CHUNK_SIZE,
    AIS_DIRECT_PUT_RETRIES,
    HEADER_NODE_URL,
    HEADER_CONTENT_LENGTH,
    STATUS_OK,
    STATUS_BAD_GATEWAY,
    ETL_WS_FQN,
    ETL_WS_PATH,
    ETL_WS_PIPELINE,
    HEADER_DIRECT_PUT_LENGTH,
    QPARAM_ETL_ARGS,
    QPARAM_ETL_FQN,
    STATUS_INTERNAL_SERVER_ERROR,
    HEADER_AUTHORIZATION,
)


class _RequestStreamReader(io.RawIOBase):
    """Make async `request.stream()` look like a sync file so the existing
    `transform_stream(reader: BinaryIO, ...)` API can consume the request
    body without buffering or an async refactor.

    `transform_stream` runs in an `anyio` worker thread (Starlette dispatches
    sync iteration there via `iterate_in_threadpool`; `httpx.AsyncClient`
    does the same when iterating a sync content iterator). Each `read(n)`
    blocks the worker thread while `anyio.from_thread.run` awaits the next
    chunk on the request's event loop. Same loop, same threadpool, no extra
    deps (`anyio` is already a transitive dep of FastAPI).

    `request.stream()` cannot be replayed; `_direct_put_stream_with_retry`
    skips local retries on this path so AIS retries the whole PUT instead.
    Errors raised by `request.stream()` (client disconnect, framing) surface
    from `read()` to the transform.
    """

    def __init__(self, request: Request) -> None:
        """
        Args:
            request (Request): Starlette/FastAPI `Request` whose async body
                stream is bridged into a sync `BinaryIO`.
        """
        self._iter = request.stream().__aiter__()
        self._buf = bytearray()
        self._eof = False

    def _next_chunk(self) -> Optional[bytes]:
        """Pull the next request body chunk from the event loop.

        Returns:
            Optional[bytes]: Next chunk from `request.stream()`, or `None` on EOF.
        """
        if self._eof:
            return None
        try:
            return from_thread.run(self._iter.__anext__)
        except StopAsyncIteration:
            self._eof = True
            return None

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        """Read up to `size` bytes from the request body stream.

        Args:
            size (int, optional): Maximum number of bytes to read. `-1` (or
                `None`) reads to EOF. Defaults to `-1`.
        Returns:
            bytes: Bytes read from the stream. May be shorter than `size` on EOF.
        """
        if size is None or size < 0:
            while (chunk := self._next_chunk()) is not None:
                self._buf.extend(chunk)
            data, self._buf = bytes(self._buf), bytearray()
            return data
        while len(self._buf) < size and (chunk := self._next_chunk()) is not None:
            self._buf.extend(chunk)
        data = bytes(self._buf[:size])
        del self._buf[:size]
        return data


class _DeferredStartStreamingResponse(Response):
    """Streaming response that defers `http.response.start` until the body
    iterator yields its first chunk, so iterators consuming
    `request.stream()` can still pull request body chunks via `receive()`.

    ASGI request/response phasing: an ASGI server delivers request body
    chunks via `receive()` only while the application is in the
    request-reading phase. Sending `http.response.start` transitions the
    application to the response-sending phase; subsequent `receive()` calls
    return `http.disconnect` (`uvicorn`) or block indefinitely
    (`TestClient`). In short: the request body tap closes the moment the
    response is announced.

    Starlette's built-in `StreamingResponse` sends `response.start` *before*
    iterating the body, so any body iterator that reads `request.stream()`
    (e.g. via `_RequestStreamReader`) hits `ClientDisconnect` on the first
    chunk. This class reorders the two `send()` calls: the first body chunk
    is pulled first (still in the request-reading phase, so `receive()`
    succeeds), then `response.start` is sent, then subsequent chunks.
    Empirically, once delivery has begun the server keeps returning body
    chunks via `receive()` for the lifetime of the request on both real
    `uvicorn` and `TestClient`.

    See starlette discussion #1830 for context. Used only in the inline
    streaming path; the direct-put path consumes the body inside the handler
    before constructing any response, so it can use a regular `Response`.
    """

    # pylint: disable=super-init-not-called
    def __init__(
        self,
        content,
        status_code: int = 200,
        headers: Optional[dict] = None,
        media_type: Optional[str] = None,
    ) -> None:
        """
        Args:
            content (Iterable[bytes] | AsyncIterable[bytes]): Body iterator.
                Async iterables are used as-is; sync iterables are wrapped
                via `iterate_in_threadpool` so they run in a worker thread
                (matches `StreamingResponse` semantics).
            status_code (int, optional): HTTP status to send on
                `http.response.start`. Defaults to `200`.
            headers (Optional[dict], optional): Response headers.
                Defaults to `None`.
            media_type (Optional[str], optional): MIME type for the
                `Content-Type` header. Defaults to `None`.
        """
        if isinstance(content, AsyncIterable):
            self.body_iterator = content
        else:
            self.body_iterator = iterate_in_threadpool(content)
        self.status_code = status_code
        self.media_type = media_type
        self.background = None
        self.init_headers(headers)

    async def __call__(self, scope, receive, send) -> None:
        """ASGI entry point.

        Args:
            scope (Scope): ASGI scope (unused; required by ASGI signature).
            receive (Receive): ASGI `receive` callable (unused; the body
                iterator may drive `receive` itself via `request.stream()`).
            send (Send): ASGI `send` callable used to emit
                `http.response.start` and `http.response.body` messages.
        """
        initial = True
        async for chunk in self.body_iterator:
            if initial:
                await send(
                    {
                        "type": "http.response.start",
                        "status": self.status_code,
                        "headers": self.raw_headers,
                    }
                )
                initial = False
            if not isinstance(chunk, (bytes, memoryview)):
                chunk = chunk.encode(self.charset)
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        if initial:
            await send(
                {
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                }
            )
        await send({"type": "http.response.body", "body": b"", "more_body": False})


HTTP_LIMITS = httpx.Limits(
    max_connections=int(os.getenv("MAX_CONN", "256")),
    max_keepalive_connections=int(os.getenv("MAX_KEEPALIVE_CONN", "128")),
    keepalive_expiry=int(os.getenv("KEEPALIVE_EXPIRY", "30")),
)

# Transient httpx errors that are safe to retry: the connection was lost before
# a response arrived, but the server state is unknown so the caller can resend.
_DIRECT_PUT_TRANSIENT_ERRORS = (
    httpx.ReadError,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
)


class FastAPIServer(ETLServer):
    """
    FastAPI server implementation for ETL transformations.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        super().__init__()
        self.host = host
        self.port = port
        self.app = FastAPI()
        self.client: Optional[httpx.AsyncClient] = None
        self.active_connections: List[WebSocket] = []
        self.chunk_size: int = int(os.getenv(AIS_DIRECT_PUT_CHUNK_SIZE, str(MIB)))
        self._setup_app()

    def _setup_app(self):
        """Configure FastAPI routes and event handlers."""
        self.app.state.etl_server = self
        self.app.router.on_startup.append(self.startup_event)
        self.app.router.on_shutdown.append(self.shutdown_event)

        @self.app.get("/health")
        async def health_check():
            return Response(content=b"Running")

        @self.app.get("/{path:path}")
        async def handle_get(path: str, request: Request):
            return await self._handle_request(path, request, is_get=True)

        @self.app.put("/{path:path}")
        async def handle_put(path: str, request: Request):
            return await self._handle_request(path, request, is_get=False)

        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            try:
                await websocket.accept()
                self.logger.debug(
                    "WebSocket connection established: %s", websocket.client
                )
                self.active_connections.append(websocket)

                while True:
                    await self._handle_ws_message(websocket)

            except Exception as e:
                self.logger.error(
                    "Unexpected WebSocket error from %s: %s", websocket.client, e
                )
            finally:
                if websocket in self.active_connections:
                    self.active_connections.remove(websocket)
                try:
                    await websocket.close()  # might have already been closed by peer
                except RuntimeError as e:
                    self.logger.debug("Skip close: %s", e)

    async def startup_event(self):
        """Initialize resources on server startup."""
        verify, cert = resolve_ssl_config()
        headers = {HEADER_AUTHORIZATION: f"Bearer {self.token}"} if self.token else {}

        transport = httpx.AsyncHTTPTransport(
            retries=int(os.getenv(AIS_DIRECT_PUT_RETRIES, "3")),
            verify=verify,
            cert=cert,
        )
        self.client = httpx.AsyncClient(
            timeout=None, limits=HTTP_LIMITS, transport=transport, headers=headers
        )
        self.logger.info("Server starting up")

    async def shutdown_event(self):
        """Cleanup resources on server shutdown."""
        await self.client.aclose()
        self.logger.info("Server shutting down")

    async def _handle_request(self, path: str, request: Request, is_get: bool):
        """Unified request handler for GET/PUT operations."""
        self.logger.debug(
            "Processing %s request for path: %s", "GET" if is_get else "PUT", path
        )

        try:
            if self.use_streaming:
                return await self._handle_request_streaming(path, request, is_get)
            return await self._handle_request_buffered(path, request, is_get)

        except InvalidPipelineError as e:
            self.logger.error("Invalid pipeline header: %s", str(e))
            raise HTTPException(
                status_code=400, detail=f"Invalid pipeline header: {str(e)}"
            ) from e
        except FileNotFoundError as exc:
            fs_path = exc.filename or path
            self.logger.error(
                "Error processing object %r: file not found at %r",
                path,
                fs_path,
            )
            raise HTTPException(
                404,
                detail=(
                    f"Error processing object {path!r}: file not found at {fs_path!r}."
                ),
            ) from exc
        except ETLDirectPutTransientError as e:
            self.logger.error(
                "Direct put failed after %d retries: %s", self.direct_put_retries, e
            )
            raise HTTPException(
                status_code=STATUS_BAD_GATEWAY,
                detail=f"Direct put failed after retries: {str(e)}",
            ) from e
        except httpx.HTTPStatusError as e:
            self.logger.warning(
                "Target responded with error: %s", e.response.status_code
            )
            raise HTTPException(
                e.response.status_code, detail="Target request failed"
            ) from e
        except httpx.RequestError as e:
            self.logger.error("Network error: %s", str(e))
            raise HTTPException(
                STATUS_BAD_GATEWAY, detail=f"Network error: {str(e)}"
            ) from e
        except requests.HTTPError as e:
            status = (
                e.response.status_code if e.response is not None else STATUS_BAD_GATEWAY
            )
            self.logger.debug("Upstream GET returned %s", status)
            raise HTTPException(status, detail="Target request failed") from e
        except requests.RequestException as e:
            self.logger.error("Network error on upstream GET: %s", str(e))
            raise HTTPException(
                STATUS_BAD_GATEWAY, detail=f"Network error: {str(e)}"
            ) from e
        except Exception as e:
            self.logger.exception("Critical error during processing")
            raise HTTPException(500, detail=f"Processing error: {str(e)}") from e

    async def _handle_request_buffered(self, path: str, request: Request, is_get: bool):
        """Buffered request handler — loads entire object, transforms, returns."""
        etl_args = request.query_params.get(QPARAM_ETL_ARGS, "").strip()
        fqn = request.query_params.get(QPARAM_ETL_FQN, "").strip()
        self.logger.debug("etl_args = %r, fqn = %r", etl_args, fqn)

        if fqn and self.direct_fqn:
            source = self.sanitize_fqn(fqn)
        elif fqn:
            source = await self._get_fqn_content(fqn)
        elif is_get:
            source = await self._get_network_content(path)
        else:
            source = await request.body()

        transformed = self.transform(source, path, etl_args)

        # TODO: extract common pipeline handling into a shared helper
        # (currently duplicated between buffered and streaming paths;
        # deferred because each framework constructs responses differently)
        pipeline_header = request.headers.get(HEADER_NODE_URL)
        self.logger.debug("pipeline_header: %r", pipeline_header)
        if pipeline_header:
            first_url, remaining_pipeline = parse_etl_pipeline(pipeline_header)
            if first_url:
                status_code, transformed, direct_put_length = (
                    await self._direct_put_with_retry(
                        first_url, transformed, remaining_pipeline, path
                    )
                )
                self.logger.debug("status_code: %r", status_code)

                return Response(
                    content=transformed,
                    status_code=status_code,
                    headers=(
                        {HEADER_DIRECT_PUT_LENGTH: str(direct_put_length)}
                        if direct_put_length != 0
                        else {}
                    ),
                )

        self.logger.debug(
            "no pipeline, returning transformed content directly, length: %r",
            len(transformed),
        )
        return Response(
            content=transformed,
            status_code=STATUS_OK,
            media_type=self.get_mime_type(),
        )

    async def _handle_request_streaming(
        self, path: str, request: Request, is_get: bool
    ):
        """Streaming request handler — yields output chunks without buffering."""
        etl_args = request.query_params.get(QPARAM_ETL_ARGS, "").strip()
        fqn = request.query_params.get(QPARAM_ETL_FQN, "").strip()
        self.logger.debug("streaming: etl_args = %r, fqn = %r", etl_args, fqn)

        pipeline_header = request.headers.get(HEADER_NODE_URL)
        self.logger.debug("pipeline_header: %r", pipeline_header)
        if pipeline_header:
            first_url, remaining = parse_etl_pipeline(pipeline_header)
            if first_url:
                result = await self._direct_put_stream_with_retry(
                    fqn, path, request, is_get, etl_args, first_url, remaining
                )
                return Response(
                    content=result[1],
                    status_code=result[0],
                    headers=self.make_direct_put_headers(result[2]),
                )

        reader = await self._get_stream_reader(fqn, path, request, is_get)
        try:
            output_iter = self.transform_stream(reader, path, etl_args)
            # _DeferredStartStreamingResponse (not StreamingResponse) — see its
            # docstring: defers http.response.start until the first body chunk
            # is pulled, so request.stream() iteration in transform_stream's
            # reader still sees body chunks via receive().
            return _DeferredStartStreamingResponse(
                self.iter_and_close(output_iter, reader),
                status_code=STATUS_OK,
                media_type=self.get_mime_type(),
            )
        except Exception:
            self.close_reader(reader)
            raise

    async def _get_stream_reader(self, fqn, path, request, is_get) -> BinaryIO:
        """Get a BinaryIO reader for the request source data."""
        if fqn:
            # Both direct_fqn and non-direct_fqn open the file — transform_stream
            # always receives a BinaryIO reader, never a path string.
            return open(self.sanitize_fqn(fqn), "rb")
        if is_get:
            obj_path = quote(path, safe="@")
            target_url = f"{self.host_target}/{obj_path}"
            self.logger.debug("Forwarding GET (stream) to: %s", target_url)
            return await asyncio.to_thread(self._open_sync_get_stream, target_url)
        # Streaming no-FQN PUT: bridge Starlette's async request.stream() into
        # a sync BinaryIO so transform_stream() can consume it from a worker
        # thread. request.stream() is one-shot; _direct_put_stream_with_retry
        # forces effective_retries=0 here (AIS retries the whole PUT instead).
        return _RequestStreamReader(request)

    def _open_sync_get_stream(self, target_url: str) -> BinaryIO:
        """Open a streaming GET against the AIS target using the shared sync session.

        Blocking — must be called via asyncio.to_thread from an async context.
        Returns a _ResponseRawReader wrapping the response so close() releases
        the connection back to the urllib3 keep-alive pool.
        """
        resp = self.session.get(target_url, stream=True, timeout=None)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            resp.close()
            raise
        return _ResponseRawReader(resp)

    async def _direct_put_stream_with_retry(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        fqn: str,
        path: str,
        request: Request,
        is_get: bool,
        etl_args: str,
        first_url: str,
        remaining: str,
    ) -> Tuple[int, bytes, int]:
        """
        Stream-put with exponential-backoff retry on transient network errors.

        Each retry reopens the source and rebuilds the transform generator
        from scratch. Sources that can be reopened: FQN-backed (re-open the
        file) or GET (re-issue the upstream stream). Streaming no-FQN PUT is
        not replayable — `request.stream()` is one-shot — so retries are
        skipped for that case and AIS retries the whole PUT instead.

        Args:
            fqn (str): Local FQN of the source object on the target's
                filesystem. Empty string when the body comes from
                `request.stream()`.
            path (str): Object path (e.g. `"bucket/object-name"`) forwarded
                to the next pipeline stage and passed to `transform_stream`.
            request (Request): Incoming `Request`; its body is the source
                when `fqn` is empty and `is_get` is `False`.
            is_get (bool): `True` for hpull GET, `False` for hpush PUT.
            etl_args (str): Per-request transformation arguments (may be
                empty).
            first_url (str): First URL in the direct-put pipeline (next
                stage).
            remaining (str): Comma-separated remaining pipeline stages,
                forwarded to the next stage via the `AIS-Node-Url` header.
        Returns:
            Tuple[int, bytes, int]: `(status_code, body, length)` — see
                `_direct_put_stream` for semantics.
        Raises:
            ETLDirectPutTransientError: if all retry attempts are exhausted.
        """
        replayable = bool(fqn) or is_get
        effective_retries = self.direct_put_retries if replayable else 0
        if not replayable and self.direct_put_retries:
            self.logger.debug(
                "no-FQN PUT: source not replayable; "
                "local retries skipped, AIS will retry"
            )

        reader = await self._get_stream_reader(fqn, path, request, is_get)
        try:
            for attempt in range(effective_retries + 1):
                try:
                    return await self._direct_put_stream(
                        first_url,
                        self.transform_stream(reader, path, etl_args),
                        remaining,
                        path,
                    )
                except ETLDirectPutTransientError as exc:
                    if attempt >= effective_retries:
                        raise
                    delay = min(RETRY_BACKOFF_BASE**attempt, RETRY_BACKOFF_MAX)
                    self.logger.warning(
                        "direct_put attempt %d/%d failed, retrying in %.1fs: %s",
                        attempt + 1,
                        effective_retries + 1,
                        delay,
                        exc,
                        exc_info=True,
                    )
                    self.close_reader(reader)
                    reader = await self._get_stream_reader(fqn, path, request, is_get)
                    await asyncio.sleep(delay)
        finally:
            self.close_reader(reader)

    async def _direct_put_stream(
        self,
        direct_put_url: str,
        data_iter: Iterator[bytes],
        remaining_pipeline: str = "",
        path: str = "",
    ) -> Tuple[int, bytes, int]:
        """
        Stream transformed output directly to the next pipeline stage.

        Returns:
            (status_code, body, length) where:
              - status_code: HTTP status of the PUT (200/204 on success, 500 on error).
              - body: response bytes forwarded back to the AIS target (empty on success).
              - length: bytes sent to the destination, from CountingIterator.
        """
        try:
            url = compose_etl_direct_put_url(direct_put_url, self.host_target, path)
            headers = {}
            if remaining_pipeline:
                headers[HEADER_NODE_URL] = remaining_pipeline

            counted = CountingIterator(data_iter)
            resp = await self.client.put(
                url, content=iterate_in_threadpool(counted), headers=headers
            )
            return self.handle_direct_put_response(
                resp, b"", data_length=counted.bytes_sent
            )

        except _DIRECT_PUT_TRANSIENT_ERRORS as exc:
            raise ETLDirectPutTransientError(direct_put_url, exc) from exc
        except Exception as exc:
            self.logger.error(
                "streaming direct_put to %s failed: %s",
                direct_put_url,
                exc,
                exc_info=True,
            )
            return STATUS_INTERNAL_SERVER_ERROR, repr(exc).encode(), 0

    async def _get_fqn_content(self, path: str) -> bytes:
        """Safely read local file content with path normalization."""
        safe_path = self.sanitize_fqn(path)
        self.logger.debug("Reading local file: %s", safe_path)

        async with aiofiles.open(safe_path, "rb") as f:
            return await f.read()

    async def _get_network_content(self, path: str) -> bytes:
        """Retrieve content from AIS target with async HTTP client."""
        obj_path = quote(path, safe="@")
        target_url = f"{self.host_target}/{obj_path}"
        self.logger.debug("Forwarding to target: %s", target_url)

        response = await self.client.get(target_url)
        response.raise_for_status()
        return response.content

    @staticmethod
    async def _iter_chunks(data: bytes, chunk_size: int = MIB):
        """
        Yield `data` in `chunk_size` pieces as an async bytes generator.

        Recommended approach for large PUT bodies with `httpx.AsyncClient`.
        See https://www.python-httpx.org/async/#streaming-requests.
        """
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    async def _direct_put_with_retry(
        self,
        direct_put_url: str,
        data: bytes,
        remaining_pipeline: str = "",
        path: str = "",
    ) -> Tuple[int, bytes, int]:
        """
        Buffered direct-put with exponential-backoff retry on transient network errors.

        Returns:
            (status_code, body, length) — see _direct_put for semantics.
        Raises:
            ETLDirectPutTransientError: if all retry attempts are exhausted.
        """
        for attempt in range(self.direct_put_retries + 1):
            try:
                return await self._direct_put(
                    direct_put_url, data, remaining_pipeline, path
                )
            except ETLDirectPutTransientError as exc:
                if attempt >= self.direct_put_retries:
                    raise
                delay = min(RETRY_BACKOFF_BASE**attempt, RETRY_BACKOFF_MAX)
                self.logger.warning(
                    "direct_put attempt %d/%d failed, retrying in %.1fs",
                    attempt + 1,
                    self.direct_put_retries + 1,
                    delay,
                    exc_info=exc,
                )
                await asyncio.sleep(delay)

    async def _direct_put(
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
        Raises:
            ETLDirectPutTransientError: on ReadError/ConnectError/RemoteProtocolError
                so the caller can retry without re-fetching data.
        """
        try:
            url = compose_etl_direct_put_url(direct_put_url, self.host_target, path)
            headers = {HEADER_CONTENT_LENGTH: str(len(data))}
            if remaining_pipeline:
                headers[HEADER_NODE_URL] = remaining_pipeline
            # TODO: add etl_args to qparams if present

            resp = await self.client.put(
                url, content=self._iter_chunks(data, self.chunk_size), headers=headers
            )
            return self.handle_direct_put_response(resp, data)

        except _DIRECT_PUT_TRANSIENT_ERRORS as exc:
            raise ETLDirectPutTransientError(direct_put_url, exc) from exc
        except Exception as exc:
            self.logger.error(
                "direct_put to %s failed (data_len=%d): %s",
                url,
                len(data),
                exc,
                exc_info=True,
            )
            return STATUS_INTERNAL_SERVER_ERROR, repr(exc).encode(), 0

    def _build_response(self, content: bytes, mime_type: str) -> Response:
        """Construct standardized response with appropriate headers."""
        return Response(
            content=content,
            media_type=mime_type,
            headers={HEADER_CONTENT_LENGTH: str(len(content))},
        )

    async def _handle_ws_message(self, websocket: WebSocket):
        """Handle a single WebSocket message. Always uses buffered transform()."""
        if self.use_streaming:
            raise NotImplementedError(
                "WebSocket transport requires transform(), not transform_stream(). "
                "Override transform() to use WebSocket, or use HTTP transport for streaming."
            )

        ctrl_msg = await websocket.receive_json(mode="binary")
        self.logger.debug("Received control message: %s", ctrl_msg)

        fqn = ctrl_msg.get(ETL_WS_FQN)
        path = ctrl_msg.get(ETL_WS_PATH)
        etl_args = ctrl_msg.get(QPARAM_ETL_ARGS)

        if fqn and self.direct_fqn:
            source = self.sanitize_fqn(fqn)
        elif fqn:
            source = await self._get_fqn_content(fqn)
        else:
            source = await websocket.receive_bytes()

        try:
            transformed = await asyncio.to_thread(
                self.transform, source, path, etl_args
            )

            pipeline_header = ctrl_msg.get(ETL_WS_PIPELINE)
            if pipeline_header:
                self.logger.debug("pipeline_header: %r", pipeline_header)
                first_url, remaining_pipeline = parse_etl_pipeline(pipeline_header)
                if first_url:
                    status_code, transformed, direct_put_length = (
                        await self._direct_put_with_retry(
                            first_url, transformed, remaining_pipeline, path
                        )
                    )
                    if status_code == STATUS_OK:
                        await websocket.send_bytes(transformed)
                    else:
                        await websocket.send_text(str(direct_put_length))
                    return

            # No pipeline, send transformed data
            await websocket.send_bytes(transformed)

        except InvalidPipelineError as e:
            self.logger.error("Invalid pipeline header: %s", str(e))
            await websocket.send_text(f"Invalid pipeline header: {str(e)}")
        except ETLDirectPutTransientError as e:
            self.logger.error("Direct put failed after retries: %s", str(e))
            await websocket.send_text("0")
        except Exception as e:
            self.logger.error("Transform error: %s", str(e))
            await websocket.send_text(f"Transform error: {str(e)}")

    def start(self):
        """Start the server with production-optimized settings."""
        uvicorn.run(self.app, host=self.host, port=self.port)
