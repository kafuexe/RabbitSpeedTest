"""User business rules. The choreography (idempotent create, optimistic
update, batched event application) lives in the shared
VersionedEntityService; this module supplies only what is user-specific:
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
from app.modules.shared.validation import valid_email, valid_name
from app.modules.user.model import User
from app.modules.user.repository import UserRepository

if TYPE_CHECKING:
    from app.messaging.cloudevents import CloudEvent


@dataclass(frozen=True)
class UserData:
    """Full desired state of a user (create payload / event payload)."""

    id: uuid.UUID
    name: str
    email: str
    attributes: dict[str, Any]
    version: int = 1


@dataclass(frozen=True)
class UserChanges:
    """Partial update; None means "leave unchanged"."""

    name: str | None = None
    email: str | None = None
    attributes: dict[str, Any] | None = None


UserEventItem = StateEventItem[UserData]


class UserService(VersionedEntityService[User, UserData, UserChanges]):
    entity_name = "user"
    created_event_type = "user.created"
    updated_event_type = "user.updated"
    default_sort = SortSpec(field="created_at", descending=True)
    sortable_fields = frozenset(UserRepository.sortable_columns)
    filterable_fields = frozenset(UserRepository.filterable_columns)

    def _new_entity(self, data: UserData) -> User:
        return User(
            id=data.id,
            name=data.name,
            email=data.email,
            attributes=dict(data.attributes),
            version=data.version,
        )

    def _content_matches(self, entity: User, data: UserData) -> bool:
        return (entity.name, entity.email, entity.attributes) == (
            data.name, data.email, data.attributes,
        )

    def _validate_data(self, data: UserData) -> None:
        self._validate_name(data.name)
        self._validate_email(data.email)

    def _validate_changes(self, changes: UserChanges) -> None:
        if changes.name is not None:
            self._validate_name(changes.name)
        if changes.email is not None:
            self._validate_email(changes.email)

    def _build_event(self, event_type: str, entity: User) -> "CloudEvent":
        # Local import keeps business.py free of a hard edge on the event
        # module at import time while events.py imports UserData from here.
        from app.modules.user.events import build_user_event

        return build_user_event(event_type, entity, source=self._event_source)

    # The business floor delegates to the SHARED rules (the same functions
    # the API schemas and UserEventData run), so no write path can drift:
    # a value valid here is valid everywhere, and vice versa.

    @staticmethod
    def _validate_name(name: str) -> None:
        try:
            valid_name(name)
        except ValueError as exc:
            raise InvalidInputError(f"name {exc}") from None

    @staticmethod
    def _validate_email(email: str) -> None:
        try:
            valid_email(email)
        except ValueError as exc:
            raise InvalidInputError(f"email invalid: {exc}") from None
