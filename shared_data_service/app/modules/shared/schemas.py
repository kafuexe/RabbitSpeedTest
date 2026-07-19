"""Shared API response models."""
from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel

ItemT = TypeVar("ItemT")


class Page(BaseModel, Generic[ItemT]):
    """Generic page envelope. Each module subclasses it explicitly
    (`class UserPageOut(Page[UserOut])`) so the OpenAPI schema keeps a
    stable, entity-named title."""

    items: list[ItemT]
    total: int
    limit: int
    offset: int
