"""API DTOs for the project module (Pydantic v2). Fields are declared with
the shared Annotated types from modules/shared/validation.py (plus the
module's own ProjectDescription) — rule + shape constraints in one
declaration, whole-schema validation by default."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.modules.project.business import ProjectDescription
from app.modules.shared.validation import (
    StorableAttributes,
    StrictEmail,
    ValidName,
)


class ProjectCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: ValidName
    description: ProjectDescription = ""
    owner_email: StrictEmail
    attributes: StorableAttributes = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    name: ValidName | None = None
    description: ProjectDescription | None = None
    owner_email: StrictEmail | None = None
    attributes: StorableAttributes | None = None
    expected_version: int | None = Field(default=None, ge=1)


class ProjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: str
    owner_email: str
    attributes: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime


class ProjectPageOut(BaseModel):
    items: list[ProjectOut]
    total: int
    limit: int
    offset: int
