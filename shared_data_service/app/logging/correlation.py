"""Correlation-ID propagation via contextvars.

One correlation id per unit of work (HTTP request or consumed message); every
log line and outgoing CloudEvent carries it, so a request can be traced across
the API, the business layer, and the broker.
"""
from __future__ import annotations

import contextvars
import uuid

_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


def get_correlation_id() -> str:
    return _correlation_id.get()


def set_correlation_id(value: str | None = None) -> str:
    """Set (or generate) the correlation id for the current context."""
    cid = value or uuid.uuid4().hex
    _correlation_id.set(cid)
    return cid
