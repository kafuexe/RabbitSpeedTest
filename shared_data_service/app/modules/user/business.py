"""User business rules. The choreography (idempotent create, optimistic
update, batched event application) lives in the shared
VersionedEntityService; this module supplies only what is user-specific:
the data shapes (which ARE the validation floor — see below), replay
equality, and event building.

The data shapes are pydantic models declared with the shared Annotated
types from modules/shared/validation.py, so constructing one — or assigning
to a field (validate_assignment) — IS the business validation. No manual
validation calls exist here; a UserData/UserChanges instance is valid by
construction, and invalid input raises pydantic.ValidationError at the
call site that built it.

Framework-free: no FastAPI, no RabbitMQ imports. Both the API and the
consumer call these methods; which EventPublisher the injected UnitOfWork
carries (real vs null) is the composition root's decision.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from app.modules.shared.query import SortSpec
from app.modules.shared.service import StateEventItem, VersionedEntityService
from app.modules.shared.validation import StorableAttributes, StrictEmail, ValidName
from app.modules.user.model import User
from app.modules.user.repository import UserRepository

if TYPE_CHECKING:
    from app.messaging.cloudevents import CloudEvent


class UserData(BaseModel):
    """Full desired state of a user (create payload / event payload).
    Valid by construction; assignment re-validates. The email is STRICT
    (StrictEmail, normalized) — the consumer path deliberately bypasses this
    via model_construct after its own permissive floor, see
    modules/shared/events.py."""

    model_config = ConfigDict(validate_assignment=True)

    id: uuid.UUID
    name: ValidName
    email: StrictEmail
    attributes: StorableAttributes
    version: int = 1


class UserChanges(BaseModel):
    """Partial update; None means "leave unchanged". Non-None fields are
    validated by the same shared types the full state uses."""

    model_config = ConfigDict(validate_assignment=True)

    name: ValidName | None = None
    email: StrictEmail | None = None
    attributes: StorableAttributes | None = None


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

    def _build_event(self, event_type: str, entity: User) -> "CloudEvent":
        # Local import keeps business.py free of a hard edge on the event
        # module at import time while events.py imports UserData from here.
        from app.modules.user.events import build_user_event

        return build_user_event(event_type, entity, source=self._event_source)
