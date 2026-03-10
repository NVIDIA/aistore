#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

import os
import asyncio
from io import BytesIO
from urllib.parse import quote
from typing import BinaryIO, Iterator, Optional, List, Tuple

from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    Response,
    WebSocket,
)
from fastapi.responses import StreamingResponse
import httpx
import aiofiles
import uvicorn

from aistore.sdk.etl.webserver.base_etl_server import ETLServer, CountingIterator
from aistore.sdk.session_manager import resolve_ssl_config
from aistore.sdk.etl.webserver.utils import (
    compose_etl_direct_put_url,
    parse_etl_pipeline,
)
from aistore.sdk.errors import InvalidPipelineError
from aistore.sdk.const import (
    MIB,
    AIS_DIRECT_PUT_CHUNK_SIZE,
    AIS_DIRECT_PUT_RETRIES,
    HEADER_NODE_URL,
    HEADER_CONTENT_LENGTH,
    STATUS_OK,
    ETL_WS_FQN,
    ETL_WS_PATH,
    ETL_WS_PIPELINE,
    HEADER_DIRECT_PUT_LENGTH,
    QPARAM_ETL_ARGS,
    QPARAM_ETL_FQN,
    STATUS_INTERNAL_SERVER_ERROR,
    HEADER_AUTHORIZATION,
)

HTTP_LIMITS = httpx.Limits(
    max_connections=int(os.getenv("MAX_CONN", "256")),
    max_keepalive_connections=int(os.getenv("MAX_KEEPALIVE_CONN", "128")),
    keepalive_expiry=int(os.getenv("KEEPALIVE_EXPIRY", "30")),
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
        self.app.add_event_handler("startup", self.startup_event)
        self.app.add_event_handler("shutdown", self.shutdown_event)

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
        except httpx.HTTPStatusError as e:
            self.logger.warning(
                "Target responded with error: %s", e.response.status_code
            )
            raise HTTPException(
                e.response.status_code, detail="Target request failed"
            ) from e
        except httpx.RequestError as e:
            self.logger.error("Network error: %s", str(e))
            raise HTTPException(502, detail=f"Network error: {str(e)}") from e
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
                status_code, transformed, direct_put_length = await self._direct_put(
                    first_url, transformed, remaining_pipeline, path
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

        reader = await self._get_stream_reader(fqn, path, request, is_get)
        try:
            # transform_stream is a sync generator — creating it is instant (no I/O).
            # StreamingResponse iterates sync iterators in a threadpool automatically,
            # so the blocking generator body runs off the event loop.
            output_iter = self.transform_stream(reader, path, etl_args)

            pipeline_header = request.headers.get(HEADER_NODE_URL)
            self.logger.debug("pipeline_header: %r", pipeline_header)
            if pipeline_header:
                first_url, remaining = parse_etl_pipeline(pipeline_header)
                if first_url:
                    try:
                        result = await self._direct_put_stream(
                            first_url, output_iter, remaining, path
                        )
                    finally:
                        self.close_reader(reader)
                    return Response(
                        content=result[1],
                        status_code=result[0],
                        headers=self.make_direct_put_headers(result[2]),
                    )

            return StreamingResponse(
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
            content = await self._get_network_content(path)
            return BytesIO(content)
        body = await request.body()
        return BytesIO(body)

    async def _direct_put_stream(
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
            resp = await self.client.put(
                url, content=self._to_async_iter(counted), headers=headers
            )
            return self.handle_direct_put_response(
                resp, b"", data_length=counted.bytes_sent
            )

        except Exception as exc:
            self.logger.error(
                "streaming direct_put to %s failed: %s",
                direct_put_url,
                exc,
                exc_info=True,
            )
            return STATUS_INTERNAL_SERVER_ERROR, str(exc).encode(), 0

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
    async def _to_async_iter(sync_iter):
        """Wrap a sync iterator as an async generator for httpx.AsyncClient."""
        for chunk in sync_iter:
            yield chunk

    @staticmethod
    async def _iter_chunks(data: bytes, chunk_size: int = MIB):
        """
        Yield `data` in `chunk_size` pieces as an async bytes generator.

        Recommended approach for large PUT bodies with `httpx.AsyncClient`.
        See https://www.python-httpx.org/async/#streaming-requests.
        """
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

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

        except Exception as exc:
            self.logger.error(
                "direct_put to %s failed (data_len=%d): %s",
                url,
                len(data),
                exc,
                exc_info=True,
            )
            return STATUS_INTERNAL_SERVER_ERROR, str(exc).encode(), 0

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
                        await self._direct_put(
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
        except Exception as e:
            self.logger.error("Transform error: %s", str(e))
            await websocket.send_text(f"Transform error: {str(e)}")

    def start(self):
        """Start the server with production-optimized settings."""
        uvicorn.run(self.app, host=self.host, port=self.port)
