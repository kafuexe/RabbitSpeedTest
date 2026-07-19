"""API DTOs for the user module (Pydantic v2)."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from app.modules.shared.validation import storable_json, valid_name


class UserCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: str = Field(min_length=1, max_length=200)
    email: EmailStr = Field(max_length=320)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_valid(cls, value: str) -> str:
        return valid_name(value)

    @field_validator("attributes")
    @classmethod
    def _attributes_storable(cls, value: dict[str, Any]) -> dict[str, Any]:
        return storable_json(value)


class UserUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    email: EmailStr | None = Field(default=None, max_length=320)
    attributes: dict[str, Any] | None = None
    expected_version: int | None = Field(default=None, ge=1)

    @field_validator("name")
    @classmethod
    def _name_valid(cls, value: str | None) -> str | None:
        return None if value is None else valid_name(value)

    @field_validator("attributes")
    @classmethod
    def _attributes_storable(
        cls, value: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        return None if value is None else storable_json(value)


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
