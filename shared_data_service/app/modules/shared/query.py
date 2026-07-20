"""Pagination, sorting and filtering primitives shared by all modules.

Each module declares its own whitelists; anything outside them is rejected
with InvalidQueryError (HTTP 400), never passed to SQL.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, Sequence, TypeVar

from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.filters import FilterClause

T = TypeVar("T")


@dataclass(frozen=True)
class PageRequest:
    limit: int
    offset: int


@dataclass(frozen=True)
class SortSpec:
    field: str
    descending: bool = False


@dataclass(frozen=True)
class ListQuery:
    page: PageRequest
    sort: SortSpec
    filters: Sequence[FilterClause] = field(default_factory=lambda: [])


@dataclass(frozen=True)
class PageResult(Generic[T]):
    """Service-layer page of ORM rows. (The API-facing pydantic page model
    is modules/shared/schemas.Page.)"""

    items: Sequence[T]
    total: int
    limit: int
    offset: int


def make_page_request(limit: int, offset: int, *, max_limit: int) -> PageRequest:
    if limit < 1:
        raise InvalidQueryError("limit must be >= 1")
    if limit > max_limit:
        raise InvalidQueryError(f"limit must be <= {max_limit}")
    if offset < 0:
        raise InvalidQueryError("offset must be >= 0")
    return PageRequest(limit=limit, offset=offset)


def parse_sort(raw: str | None, *, allowed: frozenset[str], default: SortSpec) -> SortSpec:
    """Parse "field" / "-field" (descending) against a whitelist."""
    if not raw:
        return default
    descending = raw.startswith("-")
    fieldname = raw[1:] if descending else raw
    if fieldname not in allowed:
        raise InvalidQueryError(
            f"cannot sort by {fieldname!r}; allowed: {', '.join(sorted(allowed))}"
        )
    return SortSpec(field=fieldname, descending=descending)


