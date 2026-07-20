"""Organization module — the whole entity in one file, same shape as
app/modules/user.py: ORM (storage), OrganizationData (business state AND
event payload — the permissive floor), OrganizationCreate/OrganizationUpdate
(strict API ingress), OrganizationOut/OrganizationPageOut (responses),
OrganizationFilters (statically declared list-filter params), the thin route
declarations, and ORGANIZATION_SPEC.

Strict-at-API / permissive-at-events: OrganizationCreate/OrganizationUpdate
carry StrictEmail (a bad address gets a 422 the client can fix);
OrganizationData carries FloorEmail (a consumed full-state event must never
freeze the replica over email syntax — see modules/shared/validation.py).
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, Integer, String, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base
from app.modules.shared.routing import (
    VersionedUpdate,
    create_and_respond,
    list_and_respond,
    update_and_respond,
)
from app.modules.shared.schemas import Page, Pagination
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import EntitySpec, q
from app.modules.shared.validation import (
    FloorEmail,
    StorableAttributes,
    StorableText,
    StrictEmail,
    ValidName,
)

# The plan-slug rule+shape in ONE place: storable text, max 50. Every schema
# below composes this, so the limit cannot drift.
OrganizationPlan = Annotated[StorableText, Field(max_length=50)]

# ------------------------------------------------------------------ storage


class Organization(Base):
    """`version` is the optimistic-concurrency / event-ordering anchor:
    every successful update increments it, and inbound events carrying a
    version <= the stored one are stale."""

    __tablename__ = "organizations"
    # Fetch server-generated columns (created_at/updated_at) via RETURNING at
    # flush time, so instances stay complete after the session closes.
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(
        String(200), nullable=False, info=q(filter=True, sort=True)
    )
    billing_email: Mapped[str] = mapped_column(
        String(320), nullable=False, index=True, info=q(filter=True, sort=True)
    )
    # Python-side default (NOT server_default): a business column the write
    # path owns must be carried in every INSERT/upsert. A server_default here
    # would be silently dropped by the repository's _row_values.
    plan: Mapped[str] = mapped_column(String(50), nullable=False, default="free")
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


# ------------------------------- full state: business model + event payload


class OrganizationData(BaseModel):
    """Full desired state of an organization; ALSO the CloudEvent payload.
    Carries the PERMISSIVE floor (FloorEmail, verbatim) — strictness is the
    API schemas' job. `extra="ignore"` is also what keeps
    created_at/updated_at out of events built from ORM rows. Valid by
    construction; assignment re-validates."""

    model_config = ConfigDict(extra="ignore", validate_assignment=True)

    id: uuid.UUID
    name: ValidName
    billing_email: FloorEmail
    plan: OrganizationPlan = "free"
    attributes: StorableAttributes = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)


# ------------------------------------------------ strict API-ingress schemas


class OrganizationCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: ValidName
    billing_email: StrictEmail
    plan: OrganizationPlan = "free"
    attributes: StorableAttributes = Field(default_factory=dict)


class OrganizationUpdate(VersionedUpdate):
    # Sent-field semantics via model_fields_set; None means unchanged.
    name: ValidName | None = None
    billing_email: StrictEmail | None = None
    plan: OrganizationPlan | None = None
    attributes: StorableAttributes | None = None


class OrganizationOut(BaseModel):
    # API response. Plain field types on PURPOSE — NOT OrganizationData's
    # floor types: a response model re-validates the stored row, and
    # inheriting ValidName/FloorEmail/OrganizationPlan would turn any
    # out-of-band row that violates the floor into a 500 on read. Plain types
    # also keep the response schema byte-identical to the pre-refactor OpenAPI.
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    billing_email: str
    plan: str
    attributes: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime


class OrganizationPageOut(Page[OrganizationOut]):
    # Explicit subclass so the OpenAPI schema keeps a stable, entity-named
    # title.
    pass


class OrganizationFilters(BaseModel):
    """The entity's filter params — mirrors the model's q(filter=True) tags
    (the filter-sync contract test enforces the match). Kept PURE so it is
    `ORGANIZATION_SPEC.filters`; the shared pagination/sort surface composes
    in via OrganizationListParams below."""

    name: str | None = None
    billing_email: str | None = None


class OrganizationListParams(OrganizationFilters, Pagination):
    """The list endpoint's flattened query model: filters + the shared
    `Pagination` surface (limit/offset/sort). One query-param model per
    endpoint (FastAPI's flattening rule); base order reproduces the
    query-param order limit, offset, sort, name, billing_email."""


OrganizationService = VersionedEntityService[
    Organization, OrganizationData, OrganizationUpdate
]

# ------------------------------------------------------------------- routes
# Hand-written signatures on purpose: FastAPI resolves annotations at
# decoration time, so payload/filter params must be concrete classes to be
# visible to pyright and OpenAPI. Bodies are the shared helpers. A module
# needing endpoints beyond CRUD simply adds them here, in its own factory.


def build_organization_router(service: OrganizationService) -> APIRouter:
    router = APIRouter(prefix="/organizations", tags=["organizations"])

    @router.post(
        "", response_model=OrganizationOut, status_code=status.HTTP_201_CREATED
    )
    async def create_organization(
        payload: OrganizationCreate, response: Response
    ) -> OrganizationOut:
        return await create_and_respond(
            service, payload, response, out=OrganizationOut
        )

    @router.get("/{organization_id}", response_model=OrganizationOut)
    async def get_organization(organization_id: uuid.UUID) -> OrganizationOut:
        return OrganizationOut.model_validate(await service.get(organization_id))

    @router.patch("/{organization_id}", response_model=OrganizationOut)
    async def update_organization(
        organization_id: uuid.UUID, payload: OrganizationUpdate
    ) -> OrganizationOut:
        return await update_and_respond(
            service, organization_id, payload, out=OrganizationOut
        )

    @router.get("", response_model=OrganizationPageOut)
    async def list_organizations(
        params: Annotated[OrganizationListParams, Query()],
    ) -> OrganizationPageOut:
        return await list_and_respond(
            service,
            params,
            out=OrganizationOut,
            page_out=OrganizationPageOut,
        )

    return router


# ---------------------------------------------------------------------- spec
# Last on purpose: it references the route builder above. Registered in
# app/modules/__init__.py (ALL_SPECS) — the only other file a new entity
# touches.

ORGANIZATION_SPEC = EntitySpec(
    name="organization",
    model=Organization,
    data=OrganizationData,
    create=OrganizationCreate,
    update=OrganizationUpdate,
    out=OrganizationOut,
    filters=OrganizationFilters,
    mutable_fields=("name", "billing_email", "plan", "attributes"),
    router_factory=build_organization_router,
)
