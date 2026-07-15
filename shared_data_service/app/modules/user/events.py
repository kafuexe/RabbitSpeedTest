"""User event contract: types, payload schema, builders, handler registration.

Both user.created and user.updated carry the user's FULL state plus its
version, which is what makes out-of-order handling possible: the consumer can
upsert from any event and drop anything stale.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy.exc import DataError

from app.logging.correlation import get_correlation_id
from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent, now_utc, validation_error_reason
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.validation import storable_json, storable_text
from app.modules.user.business import UserData, UserEventItem
from app.modules.user.model import User

logger = logging.getLogger(__name__)

USER_CREATED = "user.created"
USER_UPDATED = "user.updated"


class UserEventData(BaseModel):
    """Payload carried in the CloudEvent `data` attribute.

    Enforces the same business floor as the API path (non-blank name,
    email with '@') plus storability (no NUL bytes), so nothing the API
    would reject — or PostgreSQL cannot store — reaches the database via
    events.
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
        if not value.strip():
            raise ValueError("must not be blank")
        return storable_text(value)

    @field_validator("email")
    @classmethod
    def _email_floor(cls, value: str) -> str:
        if "@" not in value:
            raise ValueError("must contain '@'")
        return storable_text(value)

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
    """Handlers validate the payload (permanent failures are logged and acked
    away here), then hand the item to the greedy batcher; submit() returns —
    and the message is acked — only once the item's batch has committed."""

    async def apply_state_event(event: CloudEvent) -> None:
        try:
            payload = UserEventData.model_validate(event.data)
        except ValidationError as exc:
            # Permanent failure: log (without payload values — PII) and
            # return (ack) — never poison-loop.
            logger.warning(
                "invalid user event payload rejected",
                extra={"event_id": event.id, "event_type": event.type,
                       "reason": validation_error_reason(exc)},
            )
            return
        try:
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
        except DataError as exc:
            # Deterministic storage rejection that slipped past validation:
            # retrying can never succeed, so ack it away instead of
            # requeue-looping. (Transient DB errors are OperationalError etc.
            # and still propagate → requeue.)
            logger.warning(
                "unstorable user event rejected",
                extra={"event_id": event.id, "event_type": event.type,
                       "reason": type(exc).__name__},
            )
            return

    registry.register(USER_CREATED, apply_state_event)
    registry.register(USER_UPDATED, apply_state_event)
