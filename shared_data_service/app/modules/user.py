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

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, Integer, String, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base
from app.modules.shared.schemas import Page, Pagination, VersionedUpdate
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
    """The entity's filter params — mirrors the model's q(filter=True) tags
    (the filter-sync contract test enforces the match). Kept PURE (filters
    only) so it is `USER_SPEC.filters` and the sync test sees exactly the
    filterable fields; the shared pagination/sort surface is composed in via
    UserListParams below."""

    name: str | None = None
    email: str | None = None


class UserListParams(UserFilters, Pagination):
    """The list endpoint's flattened query model: the entity's filters plus
    the shared `Pagination` surface (limit/offset/sort). FastAPI flattens
    exactly ONE query-param model per endpoint, so the two compose here.
    Base order (filters, Pagination) reproduces the query-param order
    limit, offset, sort, name, email."""


UserService = VersionedEntityService[User, UserData, UserUpdate]

# ---------------------------------------------------------------------- spec
# No route code lives here: the four CRUD routes are generated by the shared
# EntityRoutes (app/modules/shared/routes.py) from USER_SPEC. A module that
# needs behavior/endpoints beyond CRUD subclasses EntityRoutes, overrides a
# logic method (calling super()) and/or extra_routes, and passes routes_cls=.
# Registered in app/modules/__init__.py (ALL_SPECS) — the only other file a
# new entity touches.

USER_SPEC = EntitySpec(
    name="user",
    model=User,
    data=UserData,
    create=UserCreate,
    update=UserUpdate,
    out=UserOut,
    filters=UserFilters,
    page_out=UserPageOut,
    list_params=UserListParams,
    mutable_fields=("name", "email", "attributes"),
)
