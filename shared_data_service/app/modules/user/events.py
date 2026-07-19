"""User event contract: types, payload schema, builder, handler registration.

Both user.created and user.updated carry the user's FULL state plus its
version, which is what makes out-of-order handling possible: the consumer can
upsert from any event and drop anything stale. Envelope building and handler
registration are the shared plumbing in modules/shared/events.py.
"""
from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field

from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.events import build_state_event, register_state_event_handlers
from app.modules.shared.validation import FloorEmail, StorableAttributes, ValidName
from app.modules.user.business import UserData, UserEventItem
from app.modules.user.model import User

USER_CREATED = "user.created"
USER_UPDATED = "user.updated"


class UserEventData(BaseModel):
    """Payload carried in the CloudEvent `data` attribute.

    DELIBERATELY more permissive than the API schemas: events are FULL-STATE
    announcements from an authoritative producer, and rejecting one (the
    dispatch layer acks rejected payloads away) would freeze the replica at
    the previous version forever — every later event for that user carries
    the same email. So the email field is FloorEmail, not StrictEmail: only
    what can never be stored (NUL/NaN) or is not minimally shaped is
    rejected, and values are stored VERBATIM (no normalization — the
    producer's value is the truth). Strict validation belongs at the API
    ingress, where the client can correct a 422. See
    modules/shared/validation.py.
    """

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    name: ValidName
    email: FloorEmail
    attributes: StorableAttributes = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)


def build_user_event(event_type: str, user: User, *, source: str) -> CloudEvent:
    payload = UserEventData(
        id=user.id,
        name=user.name,
        email=user.email,
        attributes=user.attributes,
        version=user.version,
    )
    return build_state_event(event_type, payload, source=source)


def register_user_event_handlers(
    registry: EventHandlerRegistry, batcher: Batcher[UserEventItem]
) -> None:
    register_state_event_handlers(
        registry,
        batcher,
        event_types=(USER_CREATED, USER_UPDATED),
        payload_model=UserEventData,
        data_type=UserData,
    )
