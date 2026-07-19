"""Shared endpoint BODIES for the standard CRUD routes.

The route *signatures* stay hand-written in each entity file, deliberately:
FastAPI resolves parameter annotations at decoration time, so payload and
filter parameters must be concrete classes to be visible to pyright and to
the OpenAPI schema — a TypeVar or variable annotation there is exactly the
dynamic-signature trick this codebase bans. Each hand-written endpoint is
therefore one declaration plus one call into the helpers below.
"""
from __future__ import annotations

import uuid
from typing import Any, Callable, TypeVar

from fastapi import APIRouter, Response, status
from pydantic import BaseModel, Field

from app.modules.shared.schemas import Page
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import D, EntitySpec, M

OutT = TypeVar("OutT", bound=BaseModel)
PageT = TypeVar("PageT", bound=Page[Any])
S = TypeVar("S", bound="VersionedEntityService[Any, Any, Any]")

RouteHook = Callable[[S, APIRouter], None]
"""Extension point: `build_*_router(service, extra_routes=...)` calls this
after installing the CRUD routes so a module can add endpoints beyond CRUD
onto the same router."""


class VersionedUpdate(BaseModel):
    """Base for every entity's Update schema: optimistic-concurrency guard
    plus the sent-field contract (`model_fields_set`) the generic service
    reads. Subclasses add their mutable fields, each `<Type> | None = None`
    — None (explicit or omitted) means "leave unchanged"."""

    expected_version: int | None = Field(default=None, ge=1)


UpdateT = TypeVar("UpdateT", bound=VersionedUpdate)


async def create_and_respond(
    spec: EntitySpec[M, D, UpdateT],
    service: VersionedEntityService[M, D, UpdateT],
    payload: BaseModel,
    response: Response,
    *,
    out: type[OutT],
) -> OutT:
    """Idempotent create: 201 on first write, 200 on identical replay.
    Generic because every Create schema's field names are a subset of its
    Data model's field names by design."""
    values = payload.model_dump(mode="python")
    entity_id = values.pop("id", None) or uuid.uuid4()
    data = spec.data.model_validate({**values, "id": entity_id})
    entity, created = await service.create(data)
    if not created:
        response.status_code = status.HTTP_200_OK
    return out.model_validate(entity)


async def update_and_respond(
    service: VersionedEntityService[M, D, UpdateT],
    entity_id: uuid.UUID,
    payload: UpdateT,
    *,
    out: type[OutT],
) -> OutT:
    entity = await service.update(
        entity_id, payload, expected_version=payload.expected_version
    )
    return out.model_validate(entity)


async def list_and_respond(
    service: VersionedEntityService[M, D, UpdateT],
    *,
    limit: int,
    offset: int,
    sort: str | None,
    filters: BaseModel,
    out: type[OutT],
    page_out: Callable[..., PageT],
) -> PageT:
    page = await service.list_page(
        limit=limit, offset=offset, sort=sort, filters=filters.model_dump()
    )
    return page_out(
        items=[out.model_validate(item) for item in page.items],
        total=page.total,
        limit=page.limit,
        offset=page.offset,
    )
