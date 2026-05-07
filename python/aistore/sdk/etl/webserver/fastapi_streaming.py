#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#
"""FastAPI streaming helpers for ETL servers.

Bridges Starlette's async request stream into a sync `BinaryIO` and
implements an ASGI streaming response that defers `http.response.start`
until the first body chunk is produced. Both classes are FastAPI-internal
adapters used by `fastapi_server.FastAPIServer`; they live here to keep
that module focused on routing and request handling.
"""

import io
from typing import Optional

from fastapi import Request
from fastapi.responses import StreamingResponse
from anyio import from_thread

# ASGI message-type identifiers (https://asgi.readthedocs.io/en/latest/specs/www.html).
# Starlette and uvicorn emit these as bare string literals; we centralize them
# here so the send() calls below don't repeat the strings.
ASGI_HTTP_RESPONSE_START = "http.response.start"
ASGI_HTTP_RESPONSE_BODY = "http.response.body"


class _RequestStreamReader(io.RawIOBase):
    """
    Make async `request.stream()` look like a sync file so the existing
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
        """
        Pull the next request body chunk from the event loop.

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
        """
        Read up to `size` bytes from the request body stream. Bytes pulled
        but not returned (when a chunk straddles `size`) stay in the
        internal buffer for the next `read()` call.

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


class _DeferredStartStreamingResponse(StreamingResponse):
    """
    Streaming response that defers `http.response.start` until the body
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

    Inherits `__init__` from `StreamingResponse` (same signature, same
    `body_iterator` setup) and only overrides `__call__` to swap the
    ordering. We override `__call__` rather than `stream_response` because
    `StreamingResponse.__call__` on ASGI spec_version < 2.4 spawns a
    `listen_for_disconnect` task that competes with our body iterator for
    `receive()` messages — the very conflict we are avoiding.

    See starlette discussion #1830 for context. Used only in the inline
    streaming path; the direct-put path consumes the body inside the handler
    before constructing any response, so it can use a regular `Response`.
    """

    async def __call__(self, scope, receive, send) -> None:
        """
        ASGI entry point.

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
                        "type": ASGI_HTTP_RESPONSE_START,
                        "status": self.status_code,
                        "headers": self.raw_headers,
                    }
                )
                initial = False
            if not isinstance(chunk, (bytes, memoryview)):
                chunk = chunk.encode(self.charset)
            await send(
                {"type": ASGI_HTTP_RESPONSE_BODY, "body": chunk, "more_body": True}
            )
        if initial:
            await send(
                {
                    "type": ASGI_HTTP_RESPONSE_START,
                    "status": self.status_code,
                    "headers": self.raw_headers,
                }
            )
        await send({"type": ASGI_HTTP_RESPONSE_BODY, "body": b"", "more_body": False})
