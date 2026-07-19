"""Project event contract: types, payload schema, builder, registration.

Both project.created and project.updated carry the project's FULL state plus
its version, which is what makes out-of-order handling possible: the consumer
can upsert from any event and drop anything stale. Envelope building and
handler registration are the shared plumbing in modules/shared/events.py.
"""
from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field

from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.events import build_state_event
from app.modules.shared.spec import StateEventItem
from app.modules.shared.validation import FloorEmail, StorableAttributes, ValidName
from app.modules.project.business import (
    ProjectData,
    ProjectDescription,
    ProjectEventItem,
)
from app.modules.project.model import Project

PROJECT_CREATED = "project.created"
PROJECT_UPDATED = "project.updated"


class ProjectEventData(BaseModel):
    """Payload carried in the CloudEvent `data` attribute.

    DELIBERATELY more permissive than the API schemas: events are FULL-STATE
    announcements from an authoritative producer, and rejecting one (the
    dispatch layer acks rejected payloads away) would freeze the replica at
    the previous version forever. So the email field is FloorEmail, not
    StrictEmail: only what can never be stored (NUL/NaN) or is not minimally
    shaped is rejected, and values are stored VERBATIM. Strict validation
    belongs at the API ingress, where the client can correct a 422. See
    modules/shared/validation.py.
    """

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    name: ValidName
    description: ProjectDescription = ""
    owner_email: FloorEmail
    attributes: StorableAttributes = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)


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
    """PHASE-1 TEMP: validate permissively (ProjectEventData), then carry
    the values into the STRICT ProjectData via model_construct — the floor
    already ran, and re-running the strict rules here would re-adjudicate a
    consumed event and freeze the replica. Phase 2 makes ProjectData itself
    the floor payload and this collapses into the generic
    register_entity_event_handlers."""
    field_names = list(ProjectData.model_fields)

    async def apply_state_event(event: CloudEvent) -> None:
        payload = ProjectEventData.model_validate(event.data)
        await batcher.submit(
            StateEventItem(
                event_id=event.id,
                source=event.source,
                data=ProjectData.model_construct(
                    **{name: getattr(payload, name) for name in field_names}
                ),
            )
        )

    registry.register(PROJECT_CREATED, apply_state_event)
    registry.register(PROJECT_UPDATED, apply_state_event)
