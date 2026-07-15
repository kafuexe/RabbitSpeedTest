"""Hardening tests for the user event handler: payload floor, storability,
poison classification, and PII-free logging."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import DataError

from app.messaging.cloudevents import CloudEvent, now_utc
from app.messaging.registry import EventHandlerRegistry
from app.modules.user.events import (
    USER_CREATED,
    UserEventData,
    register_user_event_handlers,
)
from app.modules.user.schemas import UserCreate, UserUpdate

UID = str(uuid.uuid4())


def payload(**overrides) -> dict:
    base = {"id": UID, "name": "Ada", "email": "a@ex.com", "version": 1}
    base.update(overrides)
    return base


# ------------------------------------------------------- payload validation


def test_event_payload_enforces_business_floor():
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(name="   "))  # blank name
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(email="abc"))  # no '@'
    assert UserEventData.model_validate(payload()).name == "Ada"


def test_event_payload_rejects_nul_bytes():
    # NUL passes Pydantic strings but PostgreSQL rejects it at execute time —
    # unvalidated it becomes a deterministic requeue loop.
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(name="a\x00b"))
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(attributes={"k": "a\x00b"}))
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(attributes={"k": ["nested", {"x": "\x00"}]}))


def test_api_schemas_reject_nul_bytes():
    with pytest.raises(ValidationError):
        UserCreate.model_validate({"name": "a\x00b", "email": "a@ex.com"})
    with pytest.raises(ValidationError):
        UserCreate.model_validate(
            {"name": "ok", "email": "a@ex.com", "attributes": {"k": "\x00"}})
    with pytest.raises(ValidationError):
        UserUpdate.model_validate({"name": "   "})  # blank-after-strip


# --------------------------------------------------- handler classification


class _StubBatcher:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc
        self.items: list = []

    async def submit(self, item) -> None:
        if self.exc is not None:
            raise self.exc
        self.items.append(item)


def make_handler(batcher):
    registry = EventHandlerRegistry()
    register_user_event_handlers(registry, batcher)
    return registry.get(USER_CREATED)


def event(**data_overrides) -> CloudEvent:
    return CloudEvent(id="evt-1", source="urn:test", type=USER_CREATED,
                      time=now_utc(), data=payload(**data_overrides))


async def test_invalid_payload_is_acked_and_log_has_no_pii(caplog):
    batcher = _StubBatcher()
    handler = make_handler(batcher)
    secret_email = "very.secret@example.com"
    with caplog.at_level("WARNING"):
        await handler(event(email=secret_email.replace("@", "")))  # invalid
    assert batcher.items == []  # rejected before the batcher
    joined = " ".join(
        str(r.msg) + str(getattr(r, "reason", "")) for r in caplog.records
    )
    assert "invalid user event payload" in joined
    assert "secret" not in joined  # the rejected value never reaches the log


async def test_data_error_is_permanent_ack_not_requeue(caplog):
    # A deterministic storage rejection that slipped past validation must be
    # acked away (return), not raised (requeue loop).
    handler = make_handler(_StubBatcher(exc=DataError("stmt", {}, Exception("nul"))))
    with caplog.at_level("WARNING"):
        await handler(event())  # must NOT raise
    assert any("unstorable user event" in r.message for r in caplog.records)


async def test_transient_errors_still_propagate_for_requeue():
    handler = make_handler(_StubBatcher(exc=ConnectionError("db down")))
    with pytest.raises(ConnectionError):
        await handler(event())
