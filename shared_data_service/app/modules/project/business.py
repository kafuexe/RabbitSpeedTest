"""Project business rules. The choreography (idempotent create, optimistic
update, batched event application) lives in the shared
VersionedEntityService; this module supplies only what is project-specific:
the data shapes (which ARE the validation floor — see below), replay
equality, and event building.

The data shapes are pydantic models declared with the shared Annotated
types from modules/shared/validation.py, so constructing one — or assigning
to a field (validate_assignment) — IS the business validation. No manual
validation calls exist here; a ProjectData/ProjectChanges instance is valid
by construction, and invalid input raises pydantic.ValidationError at the
call site that built it.

Framework-free: no FastAPI, no RabbitMQ imports. Both the API and the
consumer call these methods; which EventPublisher the injected UnitOfWork
carries (real vs null) is the composition root's decision.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel, ConfigDict, Field

from app.modules.shared.query import SortSpec
from app.modules.shared.service import StateEventItem, VersionedEntityService
from app.modules.shared.validation import (
    StorableAttributes,
    StorableText,
    StrictEmail,
    ValidName,
)
from app.modules.project.model import Project
from app.modules.project.repository import ProjectRepository

if TYPE_CHECKING:
    from app.messaging.cloudevents import CloudEvent

# The project description rule+shape in ONE place: storable text, max 2000.
# API schemas and the event payload import this, so the limit cannot drift.
ProjectDescription = Annotated[StorableText, Field(max_length=2000)]


class ProjectData(BaseModel):
    """Full desired state of a project (create payload / event payload).
    Valid by construction; assignment re-validates. The email is STRICT
    (StrictEmail, normalized) — the consumer path deliberately bypasses this
    via model_construct after its own permissive floor, see
    modules/shared/events.py."""

    model_config = ConfigDict(validate_assignment=True)

    id: uuid.UUID
    name: ValidName
    description: ProjectDescription
    owner_email: StrictEmail
    attributes: StorableAttributes
    version: int = 1


class ProjectChanges(BaseModel):
    """Partial update; None means "leave unchanged". Non-None fields are
    validated by the same shared types the full state uses."""

    model_config = ConfigDict(validate_assignment=True)

    name: ValidName | None = None
    description: ProjectDescription | None = None
    owner_email: StrictEmail | None = None
    attributes: StorableAttributes | None = None


ProjectEventItem = StateEventItem[ProjectData]


class ProjectService(VersionedEntityService[Project, ProjectData, ProjectChanges]):
    entity_name = "project"
    created_event_type = "project.created"
    updated_event_type = "project.updated"
    default_sort = SortSpec(field="created_at", descending=True)
    sortable_fields = frozenset(ProjectRepository.sortable_columns)
    filterable_fields = frozenset(ProjectRepository.filterable_columns)

    def _new_entity(self, data: ProjectData) -> Project:
        return Project(
            id=data.id,
            name=data.name,
            description=data.description,
            owner_email=data.owner_email,
            attributes=dict(data.attributes),
            version=data.version,
        )

    def _content_matches(self, entity: Project, data: ProjectData) -> bool:
        return (
            entity.name, entity.description, entity.owner_email,
            entity.attributes,
        ) == (data.name, data.description, data.owner_email, data.attributes)

    def _build_event(self, event_type: str, entity: Project) -> "CloudEvent":
        # Local import keeps business.py free of a hard edge on the event
        # module at import time while events.py imports ProjectData from here.
        from app.modules.project.events import build_project_event

        return build_project_event(event_type, entity, source=self._event_source)
