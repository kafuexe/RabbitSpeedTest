"""What PostgreSQL can physically store — the storability floor.

PostgreSQL rejects NUL (\\x00) in text and JSONB, and non-finite floats
(NaN/Infinity) in JSONB, deterministically at execute time. Anything that
passes Pydantic but cannot be stored becomes a poison message on the
consumer path and a 500 on the API path — so every boundary (API schemas,
event payloads, CloudEvent envelopes) rejects it up front by calling these.
This lives in the database layer because it is a fact about the database;
`modules/shared/validation.py` re-exports it for business-layer callers.
"""
from __future__ import annotations

import math
from typing import Any

_NUL = "\x00"


def storable_text(value: str) -> str:
    if _NUL in value:
        raise ValueError("must not contain NUL (\\x00) characters")
    return value


def storable_json(value: dict[str, Any]) -> dict[str, Any]:
    _scan(value)
    return value


def _scan(node: Any) -> None:
    if isinstance(node, str):
        storable_text(node)
    elif isinstance(node, float) and not math.isfinite(node):
        # json.loads/pydantic accept NaN and Infinity; JSONB does not.
        raise ValueError("must not contain non-finite numbers (NaN/Infinity)")
    elif isinstance(node, dict):
        for key, child in node.items():
            storable_text(key)
            _scan(child)
    elif isinstance(node, (list, tuple)):
        for child in node:
            _scan(child)
