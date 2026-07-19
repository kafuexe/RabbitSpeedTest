"""User event contract: types, payload schema, builders, handler registration.

Both user.created and user.updated carry the user's FULL state plus its
version, which is what makes out-of-order handling possible: the consumer can
upsert from any event and drop anything stale.
"""
from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.logging.correlation import get_correlation_id
from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent, now_utc
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.validation import email_floor, storable_json, valid_name
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
    the same email. So only what can never be stored (NUL/NaN) or is not
    minimally shaped is rejected, and values are stored VERBATIM (no
    normalization — the producer's value is the truth). Strict validation
    belongs at the API ingress, where the client can correct a 422. See
    modules/shared/validation.py.
    """

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    name: str = Field(min_length=1, max_length=200)
    email: str = Field(min_length=3, max_length=320)
    attributes: dict[str, Any] = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)

    @field_validator("name")
    @classmethod
    def _name_floor(cls, value: str) -> str:
        return valid_name(value)

    @field_validator("email")
    @classmethod
    def _email_floor(cls, value: str) -> str:
        return email_floor(value)

    @field_validator("attributes")
    @classmethod
    def _attributes_storable(cls, value: dict[str, Any]) -> dict[str, Any]:
        return storable_json(value)


def build_user_event(event_type: str, user: User, *, source: str) -> CloudEvent:
    data = UserEventData(
        id=user.id,
        name=user.name,
        email=user.email,
        attributes=user.attributes,
        version=user.version,
    )
    return CloudEvent(
        id=str(uuid.uuid4()),
        source=source,
        type=event_type,
        time=now_utc(),
        data=data.model_dump(mode="json"),
        correlationid=get_correlation_id(),
    )


def register_user_event_handlers(
    registry: EventHandlerRegistry, batcher: Batcher[UserEventItem]
) -> None:
    """Handlers validate (ValidationError propagates — EventConsumer's
    dispatch classifies it permanent and acks) and submit to the greedy
    batcher; submit() returns — and the message is acked — only once the
    item's batch has committed. Storage rejections likewise propagate and
    are classified by dispatch, so this module owns no ack/nack policy."""

    async def apply_state_event(event: CloudEvent) -> None:
        payload = UserEventData.model_validate(event.data)
        await batcher.submit(
            UserEventItem(
                event_id=event.id,
                source=event.source,
                data=UserData(
                    id=payload.id,
                    name=payload.name,
                    email=payload.email,
                    attributes=payload.attributes,
                    version=payload.version,
                ),
            )
        )

    registry.register(USER_CREATED, apply_state_event)
    registry.register(USER_UPDATED, apply_state_event)
