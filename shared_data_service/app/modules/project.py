"""Project module — the whole entity in one file, same shape as
app/modules/user.py: ORM (storage), ProjectData (business state AND event
payload — the permissive floor), ProjectCreate/ProjectUpdate (strict API
ingress), ProjectOut/ProjectPageOut (responses), ProjectFilters
(statically declared list-filter params), the thin route declarations,
and PROJECT_SPEC.

Strict-at-API / permissive-at-events: ProjectCreate/ProjectUpdate carry
StrictEmail (a bad address gets a 422 the client can fix); ProjectData
carries FloorEmail (a consumed full-state event must never freeze the
replica over email syntax — see modules/shared/validation.py).
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

# The project description rule+shape in ONE place: storable text, max 2000.
# Every schema below composes this, so the limit cannot drift.
ProjectDescription = Annotated[StorableText, Field(max_length=2000)]

# ------------------------------------------------------------------ storage


class Project(Base):
    """`version` is the optimistic-concurrency / event-ordering anchor:
    every successful update increments it, and inbound events carrying a
    version <= the stored one are stale."""

    __tablename__ = "projects"
    # Fetch server-generated columns (created_at/updated_at) via RETURNING at
    # flush time, so instances stay complete after the session closes.
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(
        String(200), nullable=False, info=q(filter=True, sort=True)
    )
    description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    owner_email: Mapped[str] = mapped_column(
        String(320), nullable=False, index=True, info=q(filter=True, sort=True)
    )
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


class ProjectData(BaseModel):
    """Full desired state of a project; ALSO the CloudEvent payload.
    Carries the PERMISSIVE floor (FloorEmail, verbatim) — strictness is the
    API schemas' job. `extra="ignore"` is also what keeps
    created_at/updated_at out of events built from ORM rows. Valid by
    construction; assignment re-validates."""

    model_config = ConfigDict(extra="ignore", validate_assignment=True)

    id: uuid.UUID
    name: ValidName
    description: ProjectDescription = ""
    owner_email: FloorEmail
    attributes: StorableAttributes = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)


# ------------------------------------------------ strict API-ingress schemas


class ProjectCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: ValidName
    description: ProjectDescription = ""
    owner_email: StrictEmail
    attributes: StorableAttributes = Field(default_factory=dict)


class ProjectUpdate(VersionedUpdate):
    # Sent-field semantics via model_fields_set; None means unchanged.
    name: ValidName | None = None
    description: ProjectDescription | None = None
    owner_email: StrictEmail | None = None
    attributes: StorableAttributes | None = None


class ProjectOut(BaseModel):
    # API response. Plain field types on PURPOSE — NOT ProjectData's floor
    # types: a response model re-validates the stored row, and inheriting
    # ValidName/FloorEmail/ProjectDescription would turn any out-of-band row
    # that violates the floor into a 500 on read. Plain types also keep the
    # response schema byte-identical to the pre-refactor OpenAPI.
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: str
    owner_email: str
    attributes: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime


class ProjectPageOut(Page[ProjectOut]):
    # Explicit subclass so the OpenAPI schema keeps its current name.
    pass


class ProjectFilters(BaseModel):
    """The entity's filter params — mirrors the model's q(filter=True) tags
    (the filter-sync contract test enforces the match). Kept PURE so it is
    `PROJECT_SPEC.filters`; the shared pagination/sort surface composes in
    via ProjectListParams below."""

    name: str | None = None
    owner_email: str | None = None


class ProjectListParams(ProjectFilters, Pagination):
    """The list endpoint's flattened query model: filters + the shared
    `Pagination` surface (limit/offset/sort). One query-param model per
    endpoint (FastAPI's flattening rule); base order reproduces the
    query-param order limit, offset, sort, name, owner_email."""


ProjectService = VersionedEntityService[Project, ProjectData, ProjectUpdate]

# ------------------------------------------------------------------- routes
# Hand-written signatures on purpose: FastAPI resolves annotations at
# decoration time, so payload/filter params must be concrete classes to be
# visible to pyright and OpenAPI. Bodies are the shared helpers. A module
# needing endpoints beyond CRUD simply adds them here, in its own factory.


def build_project_router(service: ProjectService) -> APIRouter:
    router = APIRouter(prefix="/projects", tags=["projects"])

    @router.post("", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
    async def create_project(payload: ProjectCreate, response: Response) -> ProjectOut:
        return await create_and_respond(service, payload, response, out=ProjectOut)

    @router.get("/{project_id}", response_model=ProjectOut)
    async def get_project(project_id: uuid.UUID) -> ProjectOut:
        return ProjectOut.model_validate(await service.get(project_id))

    @router.patch("/{project_id}", response_model=ProjectOut)
    async def update_project(
        project_id: uuid.UUID, payload: ProjectUpdate
    ) -> ProjectOut:
        return await update_and_respond(service, project_id, payload, out=ProjectOut)

    @router.get("", response_model=ProjectPageOut)
    async def list_projects(
        params: Annotated[ProjectListParams, Query()],
    ) -> ProjectPageOut:
        return await list_and_respond(
            service,
            params,
            out=ProjectOut,
            page_out=ProjectPageOut,
        )

    return router


# ---------------------------------------------------------------------- spec
# Last on purpose: it references the route builder above. Registered in
# app/modules/__init__.py (ALL_SPECS) — the only other file a new entity
# touches.

PROJECT_SPEC = EntitySpec(
    name="project",
    model=Project,
    data=ProjectData,
    create=ProjectCreate,
    update=ProjectUpdate,
    out=ProjectOut,
    filters=ProjectFilters,
    mutable_fields=("name", "description", "owner_email", "attributes"),
    router_factory=build_project_router,
)
