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
    WebSocketDisconnect,
)
import httpx
import aiofiles
import uvicorn

from aistore.sdk.etl.webserver.base_etl_server import ETLServer


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

        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            self.logger.debug(
                "New WebSocket connection attempt from: %s", websocket.client
            )

            try:
                await websocket.accept()
                self.logger.debug(
                    "WebSocket connection established: %s", websocket.client
                )
                self.active_connections.append(websocket)

                while True:
                    data = await websocket.receive_bytes()
                    self.logger.info("Received message of length: %d", len(data))
                    transformed = await asyncio.to_thread(self.transform, data, "")
                    await websocket.send_bytes(transformed)

            except WebSocketDisconnect:
                self.logger.warning("WebSocket disconnected: %s", websocket.client)
                self.active_connections.remove(websocket)

            except Exception as e:
                self.logger.error(
                    "Unexpected WebSocket error from %s: %s", websocket.client, e
                )
                self.active_connections.remove(websocket)
                await websocket.close()

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

        try:
            if self.arg_type == "fqn":
                content = await self._get_fqn_content(path)
            else:
                content = (
                    await self._get_network_content(path)
                    if is_get
                    else await request.body()
                )

            transformed = await asyncio.to_thread(self.transform, content, path)
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

    def _build_response(self, content: bytes, mime_type: str) -> Response:
        """Construct standardized response with appropriate headers."""
        return Response(
            content=content,
            media_type=mime_type,
            headers={"Content-Length": str(len(content))},
        )

    def start(self):
        """Start the server with production-optimized settings."""
        uvicorn.run(self.app, host=self.host, port=self.port)
