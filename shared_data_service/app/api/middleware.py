"""Correlation-ID middleware: honor X-Correlation-ID or mint one, expose it
on the response, and log one structured access line per request.

Pure ASGI (not BaseHTTPMiddleware): no per-request task group or memory
streams, no interference with streaming responses.
"""
from __future__ import annotations

import logging
import time

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.logging.correlation import set_correlation_id

logger = logging.getLogger("app.api.access")

CORRELATION_HEADER = "X-Correlation-ID"


class CorrelationIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        cid = set_correlation_id(Headers(scope=scope).get(CORRELATION_HEADER))
        started = time.perf_counter()
        status_code = 0

        async def send_with_header(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                MutableHeaders(scope=message)[CORRELATION_HEADER] = cid
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            logger.info(
                "request handled",
                extra={
                    "method": scope["method"],
                    "path": scope["path"],
                    "status": status_code,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                },
            )
