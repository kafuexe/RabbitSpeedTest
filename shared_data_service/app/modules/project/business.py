"""Project business rules. The choreography (idempotent create, optimistic
update, batched event application) lives in the shared
VersionedEntityService; this module supplies only what is project-specific:
the data shapes, the validation floor, replay equality, and event building.

Framework-free: no FastAPI, no RabbitMQ imports. Both the API and the
consumer call these methods; which EventPublisher the injected UnitOfWork
carries (real vs null) is the composition root's decision.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.modules.shared.errors import InvalidInputError
from app.modules.shared.query import SortSpec
from app.modules.shared.service import StateEventItem, VersionedEntityService
from app.modules.shared.validation import storable_text, valid_email, valid_name
from app.modules.project.model import Project
from app.modules.project.repository import ProjectRepository

if TYPE_CHECKING:
    from app.messaging.cloudevents import CloudEvent


@dataclass(frozen=True)
class ProjectData:
    """Full desired state of a project (create payload / event payload)."""

    id: uuid.UUID
    name: str
    description: str
    owner_email: str
    attributes: dict[str, Any]
    version: int = 1


@dataclass(frozen=True)
class ProjectChanges:
    """Partial update; None means "leave unchanged"."""

    name: str | None = None
    description: str | None = None
    owner_email: str | None = None
    attributes: dict[str, Any] | None = None


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

    def _validate_data(self, data: ProjectData) -> None:
        self._validate_name(data.name)
        self._validate_description(data.description)
        self._validate_email(data.owner_email)

    def _validate_changes(self, changes: ProjectChanges) -> None:
        if changes.name is not None:
            self._validate_name(changes.name)
        if changes.description is not None:
            self._validate_description(changes.description)
        if changes.owner_email is not None:
            self._validate_email(changes.owner_email)

    def _build_event(self, event_type: str, entity: Project) -> "CloudEvent":
        # Local import keeps business.py free of a hard edge on the event
        # module at import time while events.py imports ProjectData from here.
        from app.modules.project.events import build_project_event

        return build_project_event(event_type, entity, source=self._event_source)

    # The business floor delegates to the SHARED rules (the same functions
    # the API schemas and ProjectEventData run), so no write path can drift.

    @staticmethod
    def _validate_name(name: str) -> None:
        try:
            valid_name(name)
        except ValueError as exc:
            raise InvalidInputError(f"name {exc}") from None

    @staticmethod
    def _validate_description(description: str) -> None:
        try:
            storable_text(description)
        except ValueError as exc:
            raise InvalidInputError(f"description {exc}") from None

    @staticmethod
    def _validate_email(email: str) -> None:
        try:
            valid_email(email)
        except ValueError as exc:
            raise InvalidInputError(f"owner_email invalid: {exc}") from None
