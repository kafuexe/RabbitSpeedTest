"""Domain-exception → HTTP-status mapping. Kept at the API edge so the
business layer stays HTTP-free."""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.logging.correlation import get_correlation_id
from app.modules.shared.errors import (
    ConflictError,
    DomainError,
    InvalidInputError,
    NotFoundError,
)

logger = logging.getLogger(__name__)

_STATUS_BY_TYPE: list[tuple[type[DomainError], int]] = [
    (NotFoundError, status.HTTP_404_NOT_FOUND),
    (ConflictError, status.HTTP_409_CONFLICT),
    (InvalidInputError, status.HTTP_400_BAD_REQUEST),
]


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def handle_domain_error(request: Request, exc: DomainError) -> JSONResponse:
        for error_type, code in _STATUS_BY_TYPE:
            if isinstance(exc, error_type):
                return _response(code, exc.message)
        logger.error("unmapped domain error", extra={"error": exc.message})
        return _response(status.HTTP_500_INTERNAL_SERVER_ERROR, "internal error")


def _response(code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=code,
        content={"detail": message, "correlation_id": get_correlation_id()},
    )
