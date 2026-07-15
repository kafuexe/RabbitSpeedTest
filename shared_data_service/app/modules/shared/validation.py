"""Storability validators shared by API schemas and event payloads.

PostgreSQL rejects NUL (\\x00) in text and JSONB at execute time; anything
that passes Pydantic but cannot be stored would otherwise become a
deterministic transaction failure — a poison message on the consumer path,
a 500 on the API path. Reject it at the boundary instead.
"""
from __future__ import annotations

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
    elif isinstance(node, dict):
        for key, child in node.items():
            storable_text(key)
            _scan(child)
    elif isinstance(node, (list, tuple)):
        for child in node:
            _scan(child)
