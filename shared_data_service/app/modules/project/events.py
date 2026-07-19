"""Project event contract: types, payload schema, builder, registration.

Both project.created and project.updated carry the project's FULL state plus
its version, which is what makes out-of-order handling possible: the consumer
can upsert from any event and drop anything stale. Envelope building and
handler registration are the shared plumbing in modules/shared/events.py.
"""
from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.events import build_state_event, register_state_event_handlers
from app.modules.shared.validation import (
    email_floor,
    storable_json,
    storable_text,
    valid_name,
)
from app.modules.project.business import ProjectData, ProjectEventItem
from app.modules.project.model import Project

PROJECT_CREATED = "project.created"
PROJECT_UPDATED = "project.updated"


class ProjectEventData(BaseModel):
    """Payload carried in the CloudEvent `data` attribute.

    DELIBERATELY more permissive than the API schemas: events are FULL-STATE
    announcements from an authoritative producer, and rejecting one (the
    dispatch layer acks rejected payloads away) would freeze the replica at
    the previous version forever. So only what can never be stored (NUL/NaN)
    or is not minimally shaped is rejected, and values are stored VERBATIM.
    Strict validation belongs at the API ingress, where the client can
    correct a 422. See modules/shared/validation.py.
    """

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)
    owner_email: str = Field(min_length=3, max_length=320)
    attributes: dict[str, Any] = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)

    @field_validator("name")
    @classmethod
    def _name_floor(cls, value: str) -> str:
        return valid_name(value)

    @field_validator("description")
    @classmethod
    def _description_floor(cls, value: str) -> str:
        return storable_text(value)

    @field_validator("owner_email")
    @classmethod
    def _email_floor(cls, value: str) -> str:
        return email_floor(value)

    @field_validator("attributes")
    @classmethod
    def _attributes_storable(cls, value: dict[str, Any]) -> dict[str, Any]:
        return storable_json(value)


def build_project_event(
    event_type: str, project: Project, *, source: str
) -> CloudEvent:
    payload = ProjectEventData(
        id=project.id,
        name=project.name,
        description=project.description,
        owner_email=project.owner_email,
        attributes=project.attributes,
        version=project.version,
    )
    return build_state_event(event_type, payload, source=source)


def register_project_event_handlers(
    registry: EventHandlerRegistry, batcher: Batcher[ProjectEventItem]
) -> None:
    register_state_event_handlers(
        registry,
        batcher,
        event_types=(PROJECT_CREATED, PROJECT_UPDATED),
        payload_model=ProjectEventData,
        data_type=ProjectData,
    )
