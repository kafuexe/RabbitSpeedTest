"""Shared API request/response models."""
from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

ItemT = TypeVar("ItemT")


class VersionedUpdate(BaseModel):
    """Base for every entity's Update schema: the optimistic-concurrency
    guard plus the sent-field contract (`model_fields_set`) the generic
    service reads. Subclasses add their mutable fields, each
    `<Type> | None = None` — None (explicit or omitted) means "leave
    unchanged".

    validate_assignment: the service applies these values with no further
    validation ("valid by construction"), so mutating an instance after
    construction must re-run the same rules — exactly like the Data models.
    """

    model_config = ConfigDict(validate_assignment=True)

    expected_version: int | None = Field(default=None, ge=1)


class Pagination(BaseModel):
    """The entity-agnostic list-query surface — declared ONCE and shared by
    every entity's list endpoint. Each module composes it into its own
    flattened query model (`class UserListParams(UserFilters, Pagination)`),
    because FastAPI flattens exactly one query-param model per endpoint.

    Plain (unconstrained) `int` fields on purpose: bound-checking lives in
    the service (`make_page_request` → 400 InvalidQueryError), so the 400 vs
    422 behavior is unchanged. `limit`/`offset` navigate the page; `sort` is
    the shared "field" / "-field" ordering param, whitelisted per entity.
    """

    limit: int = 50
    offset: int = 0
    sort: str | None = Field(default=None, description="field or -field")


class Page(BaseModel, Generic[ItemT]):
    """Generic page envelope. Each module subclasses it explicitly
    (`class UserPageOut(Page[UserOut])`) so the OpenAPI schema keeps a
    stable, entity-named title."""

    items: list[ItemT]
    total: int
    limit: int
    offset: int
