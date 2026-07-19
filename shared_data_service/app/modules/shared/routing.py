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

from fastapi import Response, status
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.modules.shared.errors import InvalidInputError
from app.modules.shared.schemas import Page
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import D, M

OutT = TypeVar("OutT", bound=BaseModel)
PageT = TypeVar("PageT", bound=Page[Any])


class VersionedUpdate(BaseModel):
    """Base for every entity's Update schema: optimistic-concurrency guard
    plus the sent-field contract (`model_fields_set`) the generic service
    reads. Subclasses add their mutable fields, each `<Type> | None = None`
    — None (explicit or omitted) means "leave unchanged".

    validate_assignment: the service applies these values with no further
    validation ("valid by construction"), so mutating an instance after
    construction must re-run the same rules — exactly like the Data models.
    """

    model_config = ConfigDict(validate_assignment=True)

    expected_version: int | None = Field(default=None, ge=1)


UpdateT = TypeVar("UpdateT", bound=VersionedUpdate)


async def create_and_respond(
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
    try:
        data = service.spec.data.model_validate({**values, "id": entity_id})
    except ValidationError as exc:
        # Only reachable if a Create schema is looser than its Data model —
        # a module bug, but the client-visible invariant stays "floor
        # violation → 4xx", never a 500.
        raise InvalidInputError(
            f"create payload violates the {service.spec.name} data floor "
            f"({exc.error_count()} error(s))"
        ) from exc
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
