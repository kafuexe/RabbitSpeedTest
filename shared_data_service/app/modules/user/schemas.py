"""API DTOs for the user module (Pydantic v2). Fields are declared with the
shared Annotated types from modules/shared/validation.py — rule + shape
constraints in one declaration, whole-schema validation by default."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.modules.shared.validation import (
    StorableAttributes,
    StrictEmail,
    ValidName,
)


class UserCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: ValidName
    email: StrictEmail
    attributes: StorableAttributes = Field(default_factory=dict)


class UserUpdate(BaseModel):
    name: ValidName | None = None
    email: StrictEmail | None = None
    attributes: StorableAttributes | None = None
    expected_version: int | None = Field(default=None, ge=1)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    email: str
    attributes: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime


class UserPageOut(BaseModel):
    items: list[UserOut]
    total: int
    limit: int
    offset: int
