"""Liveness and readiness endpoints.

/health — process is alive, always 200.
/ready  — dependencies are usable: PostgreSQL answers SELECT 1 and the
          RabbitMQ connections are open. 503 otherwise.
"""
from __future__ import annotations

from typing import Awaitable, Callable

from fastapi import APIRouter, Response, status

ReadinessProbe = Callable[[], Awaitable[dict[str, bool]]]


def build_health_router(readiness_probe: ReadinessProbe) -> APIRouter:
    router = APIRouter(tags=["health"])

    @router.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/ready")
    async def ready(response: Response) -> dict[str, bool]:
        checks = await readiness_probe()
        if not all(checks.values()):
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return checks

    return router
