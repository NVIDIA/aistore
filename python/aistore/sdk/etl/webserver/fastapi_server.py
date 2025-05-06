#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import os
import asyncio
from urllib.parse import unquote, quote
from typing import Optional, List

from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    Response,
    WebSocket,
)
import httpx
import aiofiles
import uvicorn

from aistore.sdk.etl.webserver.base_etl_server import ETLServer
from aistore.sdk.utils import compose_etl_direct_put_url
from aistore.sdk.const import (
    HEADER_NODE_URL,
    HEADER_CONTENT_LENGTH,
    STATUS_NO_CONTENT,
    ETL_WS_FQN,
    ETL_WS_PATH,
    ETL_WS_DESTINATION_ADDR,
    QPARAM_ETL_ARGS,
)


class FastAPIServer(ETLServer):
    """
    FastAPI server implementation for ETL transformations.
    Utilizes async/await and threading for optimal request handling.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 80):
        super().__init__()
        self.host = host
        self.port = port
        self.app = FastAPI()
        self.client: Optional[httpx.AsyncClient] = None
        self.active_connections: List[WebSocket] = []
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

        # pylint: disable=too-many-branches
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            try:
                await websocket.accept()
                self.logger.debug(
                    "WebSocket connection established: %s", websocket.client
                )
                self.active_connections.append(websocket)

                while True:
                    ctrl_msg = await websocket.receive_json(mode="binary")
                    self.logger.debug("Received control message: %s", ctrl_msg)

                    fqn = ctrl_msg.get(ETL_WS_FQN)
                    path = ctrl_msg.get(ETL_WS_PATH)
                    content = (
                        await self._get_fqn_content(fqn)
                        if fqn
                        else await websocket.receive_bytes()
                    )
                    etl_args = ctrl_msg.get(QPARAM_ETL_ARGS)

                    self.logger.debug("Received content length: %d", len(content))
                    transformed = await asyncio.to_thread(
                        self.transform, content, path, etl_args
                    )

                    direct_put_url = ctrl_msg.get(ETL_WS_DESTINATION_ADDR)
                    if direct_put_url:
                        try:
                            response = await self._direct_put(
                                direct_put_url, transformed
                            )
                            if response:
                                await websocket.send_text("direct put success")
                                continue
                        except Exception as e:
                            self.logger.warning("Direct put failed: %s", e)

                    await websocket.send_bytes(transformed)

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
        self.client = httpx.AsyncClient(timeout=None)
        self.logger.info("Server starting up")

    async def shutdown_event(self):
        """Cleanup resources on server shutdown."""
        await self.client.aclose()
        self.logger.info("Server shutting down")

    async def _handle_request(self, path: str, request: Request, is_get: bool):
        """Unified request handler for GET/PUT operations."""
        self.logger.info(
            "Processing %s request for path: %s", "GET" if is_get else "PUT", path
        )
        etl_args: str = request.query_params.get(QPARAM_ETL_ARGS, "")

        self.logger.debug("etl_args = %r", etl_args)

        try:
            if self.arg_type == "fqn":
                content = await self._get_fqn_content(path)
            else:
                content = (
                    await self._get_network_content(path)
                    if is_get
                    else await request.body()
                )

            transformed = await asyncio.to_thread(
                self.transform, content, path, etl_args
            )

            delivery_target_url = request.headers.get(HEADER_NODE_URL)
            if delivery_target_url:
                response = await self._direct_put(delivery_target_url, transformed)
                if response:
                    return Response(
                        status_code=STATUS_NO_CONTENT,
                        headers={HEADER_CONTENT_LENGTH: "0"},
                    )

            return self._build_response(transformed, self.get_mime_type())

        except FileNotFoundError as exc:
            self.logger.error("File not found: %s", path)
            raise HTTPException(
                404,
                detail=(
                    f"Local file not found: {path}. "
                    "This typically indicates the ETL container was not started with the correct volume mounts."
                    "Please verify your ETL specification includes the necessary mount paths."
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

    async def _get_fqn_content(self, path: str) -> bytes:
        """Safely read local file content with path normalization."""
        decoded_path = unquote(path)
        safe_path = os.path.normpath(os.path.join("/", decoded_path.lstrip("/")))
        self.logger.info("Reading local file: %s", safe_path)

        async with aiofiles.open(safe_path, "rb") as f:
            return await f.read()

    async def _get_network_content(self, path: str) -> bytes:
        """Retrieve content from AIS target with async HTTP client."""
        obj_path = quote(path, safe="@")
        target_url = f"{self.host_target}/{obj_path}"
        self.logger.info("Forwarding to target: %s", target_url)

        response = await self.client.get(target_url)
        response.raise_for_status()
        return response.content

    async def _direct_put(self, delivery_target_url: str, data: bytes) -> bool:
        """
        Sends the transformed object directly to the specified AIS node (`delivery_target_url`),
        eliminating the additional network hop through the original target.
        Used only in bucket-to-bucket offline transforms.

        Returns:
            True if the direct put succeeds, False otherwise.
        """
        try:
            url = compose_etl_direct_put_url(delivery_target_url, self.host_target)
            resp = await self.client.put(url, data=data)
            if resp.status_code == 200:
                return True

            error = await resp.text()
            self.logger.error(
                "Failed to deliver object to %s: HTTP %s, %s",
                delivery_target_url,
                resp.status_code,
                error,
            )
        except Exception as e:
            self.logger.error(
                "Exception during delivery to %s: %s", delivery_target_url, e
            )

        return False

    def _build_response(self, content: bytes, mime_type: str) -> Response:
        """Construct standardized response with appropriate headers."""
        return Response(
            content=content,
            media_type=mime_type,
            headers={HEADER_CONTENT_LENGTH: str(len(content))},
        )

    def start(self):
        """Start the server with production-optimized settings."""
        uvicorn.run(self.app, host=self.host, port=self.port)
