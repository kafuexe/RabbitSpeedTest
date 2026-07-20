"""PHASE-2 TEMP: shared endpoint bodies for the project module only.

Amendment 2 moved routing into the overridable `EntityRoutes`
(shared/routes.py); the user module already uses it. Project stays on
these helpers until its phase-3 migration, after which this module is
deleted. `VersionedUpdate` now lives in shared/schemas.py and is
re-exported here so project's import keeps working during the bridge.
"""
from __future__ import annotations

import uuid
from typing import Any, Callable, TypeVar

from fastapi import Response, status
from pydantic import BaseModel, ValidationError

from app.modules.shared.errors import InvalidInputError
from app.modules.shared.schemas import Page, Pagination, VersionedUpdate
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import D, M

__all__ = [
    "VersionedUpdate",
    "create_and_respond",
    "update_and_respond",
    "list_and_respond",
]

OutT = TypeVar("OutT", bound=BaseModel)
PageT = TypeVar("PageT", bound=Page[Any])
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
    params: Pagination,
    *,
    out: type[OutT],
    page_out: Callable[..., PageT],
) -> PageT:
    """`params` is an entity's `<Entity>ListParams` — its filter model
    composed with the shared Pagination surface. The filter values are
    exactly the spec's filter fields; the rest (limit/offset/sort) is
    Pagination, so this stays fully generic."""
    filters = {
        name: getattr(params, name)
        for name in service.spec.filters.model_fields
    }
    page = await service.list_page(
        limit=params.limit, offset=params.offset, sort=params.sort, filters=filters
    )
    return page_out(
        items=[out.model_validate(item) for item in page.items],
        total=page.total,
        limit=page.limit,
        offset=page.offset,
    )
