"""User module — the whole entity in one file. Layers are separated by
class, not by file: ORM (storage), UserData (business state AND event
payload — the permissive floor), UserCreate/UserUpdate (strict API
ingress), UserOut/UserPageOut (responses), UserFilters (statically
declared list-filter params), USER_SPEC (what the generic machinery
consumes), and the thin route declarations.

Strict-at-API / permissive-at-events: UserCreate/UserUpdate carry
StrictEmail (a bad address gets a 422 the client can fix); UserData
carries FloorEmail (a consumed full-state event must never freeze the
replica over email syntax — see modules/shared/validation.py).
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

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
from app.modules.shared.schemas import Page
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import EntitySpec, q
from app.modules.shared.validation import (
    FloorEmail,
    StorableAttributes,
    StrictEmail,
    ValidName,
)

# ------------------------------------------------------------------ storage


class User(Base):
    """`version` is the optimistic-concurrency / event-ordering anchor:
    every successful update increments it, and inbound events carrying a
    version <= the stored one are stale."""

    __tablename__ = "users"
    # Fetch server-generated columns (created_at/updated_at) via RETURNING at
    # flush time, so instances stay complete after the session closes.
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(
        String(200), nullable=False, info=q(filter=True, sort=True)
    )
    email: Mapped[str] = mapped_column(
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


class UserData(BaseModel):
    """Full desired state of a user; ALSO the CloudEvent payload. Carries
    the PERMISSIVE floor (FloorEmail, verbatim) — strictness is the API
    schemas' job. `extra="ignore"` is also what keeps created_at/updated_at
    out of events built from ORM rows. Valid by construction; assignment
    re-validates."""

    model_config = ConfigDict(extra="ignore", validate_assignment=True)

    id: uuid.UUID
    name: ValidName
    email: FloorEmail
    attributes: StorableAttributes = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)


# ------------------------------------------------ strict API-ingress schemas


class UserCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: ValidName
    email: StrictEmail
    attributes: StorableAttributes = Field(default_factory=dict)


class UserUpdate(VersionedUpdate):
    # Sent-field semantics via model_fields_set; None means unchanged.
    # (Comment, not docstring: docstrings leak into the OpenAPI schema as
    # `description` and the response contract must stay byte-stable.)
    name: ValidName | None = None
    email: StrictEmail | None = None
    attributes: StorableAttributes | None = None


class UserOut(BaseModel):
    # API response. Plain field types on PURPOSE — NOT UserData's floor
    # types: a response model re-validates the stored row (FastAPI validates
    # against response_model), and inheriting ValidName/FloorEmail would turn
    # any out-of-band row that violates the floor into a 500 on read. Plain
    # types also keep the response schema byte-identical to the pre-refactor
    # OpenAPI (no minLength/maxLength on name/email).
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    email: str
    attributes: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime


class UserPageOut(Page[UserOut]):
    # Explicit subclass so the OpenAPI schema keeps its current name.
    pass


class UserFilters(BaseModel):
    """Statically declared filter params — mirrors the model's
    q(filter=True) tags; the filter-sync unit test enforces the match.
    Not a query-model param: on this FastAPI build `Annotated[Model,
    Query()]` does NOT flatten into per-field query params (verified — it
    demands a literal `?filters=` object), so the endpoint declares the
    fields explicitly and builds this model, keeping the wire contract and
    OpenAPI byte-identical."""

    name: str | None = None
    email: str | None = None


UserService = VersionedEntityService[User, UserData, UserUpdate]

# ------------------------------------------------------------------- routes
# Hand-written signatures on purpose: FastAPI resolves annotations at
# decoration time, so payload/filter params must be concrete classes to be
# visible to pyright and OpenAPI. Bodies are the shared helpers. A module
# needing endpoints beyond CRUD simply adds them here, in its own factory.


def build_user_router(service: UserService) -> APIRouter:
    router = APIRouter(prefix="/users", tags=["users"])

    @router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
    async def create_user(payload: UserCreate, response: Response) -> UserOut:
        return await create_and_respond(service, payload, response, out=UserOut)

    @router.get("/{user_id}", response_model=UserOut)
    async def get_user(user_id: uuid.UUID) -> UserOut:
        return UserOut.model_validate(await service.get(user_id))

    @router.patch("/{user_id}", response_model=UserOut)
    async def update_user(user_id: uuid.UUID, payload: UserUpdate) -> UserOut:
        return await update_and_respond(service, user_id, payload, out=UserOut)

    @router.get("", response_model=UserPageOut)
    async def list_users(
        limit: int = Query(default=50),
        offset: int = Query(default=0),
        sort: str | None = Query(default=None, description="field or -field"),
        name: str | None = Query(default=None),
        email: str | None = Query(default=None),
    ) -> UserPageOut:
        return await list_and_respond(
            service,
            limit=limit,
            offset=offset,
            sort=sort,
            filters=UserFilters(name=name, email=email),
            out=UserOut,
            page_out=UserPageOut,
        )

    return router


# ---------------------------------------------------------------------- spec
# Last on purpose: it references the route builder above. Registered in
# app/modules/__init__.py (ALL_SPECS) — the only other file a new entity
# touches.

USER_SPEC = EntitySpec(
    name="user",
    model=User,
    data=UserData,
    create=UserCreate,
    update=UserUpdate,
    out=UserOut,
    filters=UserFilters,
    mutable_fields=("name", "email", "attributes"),
    router_factory=build_user_router,
)
