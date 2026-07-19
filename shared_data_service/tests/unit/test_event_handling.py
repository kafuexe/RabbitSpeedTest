"""Hardening tests for event handling: payload floor, storability, poison
classification at the DISPATCH layer, and PII-free logging.

Classification lives in EventConsumer (not per-module handlers), so these
tests drive full event bytes through the consumer's dispatch path.
"""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import DataError, DBAPIError

from app.database.errors import is_permanent_data_error
from app.messaging.cloudevents import CloudEvent, now_utc
from app.messaging.consumer import EventConsumer
from app.messaging.registry import EventHandlerRegistry
from app.modules.user.events import (
    USER_CREATED,
    UserEventData,
    register_user_event_handlers,
)
from app.modules.user.schemas import UserCreate, UserUpdate
from tests.fakes import FakeMessageBus

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


def test_event_email_floor_is_deliberately_permissive_and_verbatim():
    # POLICY: events are full-state announcements from an authoritative
    # producer; rejecting one over strict email syntax would freeze the
    # replica forever (rejected payloads are ACKED away). So the consumer
    # floor accepts anything storable and minimally email-shaped, VERBATIM —
    # while the API stays strict (see test below).
    for producer_email in ("ops@backend", "svc@localhost", "Bob@EXAMPLE.COM"):
        parsed = UserEventData.model_validate(payload(email=producer_email))
        assert parsed.email == producer_email  # verbatim, never normalized
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(email="no-at-sign"))
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(email="nul@\x00.com"))


def test_api_email_stays_strict_where_the_client_can_correct():
    from app.modules.shared.validation import valid_email

    # The API ingress rejects what the consumer floor tolerates — a 422 the
    # client can fix, vs an event that would be silently lost.
    with pytest.raises(ValidationError):
        UserCreate.model_validate({"name": "n", "email": "ops@backend"})
    # And the business floor is EXACTLY pydantic's EmailStr rule, including
    # the 'Name <addr>' pretty-form unwrap raw email-validator rejects.
    assert valid_email("Ada <ada@example.com>") == "ada@example.com"
    assert valid_email("Bob@EXAMPLE.COM") == "Bob@example.com"


def test_event_payload_rejects_nul_bytes():
    # NUL passes Pydantic strings but PostgreSQL rejects it at execute time —
    # unvalidated it becomes a deterministic requeue loop.
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(name="a\x00b"))
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(attributes={"k": "a\x00b"}))
    with pytest.raises(ValidationError):
        UserEventData.model_validate(payload(attributes={"k": ["nested", {"x": "\x00"}]}))


def test_event_payload_rejects_non_finite_numbers():
    # json.loads/pydantic parse NaN and Infinity happily; JSONB rejects them
    # at execute time — another deterministic requeue loop if unvalidated.
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValidationError):
            UserEventData.model_validate(payload(attributes={"x": bad}))
        with pytest.raises(ValidationError):
            UserEventData.model_validate(payload(attributes={"deep": [{"x": bad}]}))
    assert UserEventData.model_validate(payload(attributes={"x": 1.5}))


def test_whole_schema_validation_aggregates_field_errors():
    # Whole-schema validation: a bad name AND bad attributes are both
    # reported in the SAME ValidationError — for the API DTO...
    with pytest.raises(ValidationError) as excinfo:
        UserCreate.model_validate(
            {"name": "   ", "email": "a@ex.com", "attributes": {"k": "\x00"}}
        )
    assert {e["loc"][0] for e in excinfo.value.errors()} >= {"name", "attributes"}
    # ...and for the event payload.
    with pytest.raises(ValidationError) as excinfo:
        UserEventData.model_validate(
            payload(name="   ", attributes={"k": float("nan")})
        )
    assert {e["loc"][0] for e in excinfo.value.errors()} >= {"name", "attributes"}


def test_api_schemas_reject_nul_bytes():
    with pytest.raises(ValidationError):
        UserCreate.model_validate({"name": "a\x00b", "email": "a@ex.com"})
    with pytest.raises(ValidationError):
        UserCreate.model_validate(
            {"name": "ok", "email": "a@ex.com", "attributes": {"k": "\x00"}})
    with pytest.raises(ValidationError):
        UserUpdate.model_validate({"name": "   "})  # blank-after-strip


# ------------------------------------------- permanent-error classification


class _FakePgError(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(f"sqlstate {sqlstate}")
        self.sqlstate = sqlstate


def _dbapi_error(sqlstate: str) -> DBAPIError:
    # Mirrors what the asyncpg dialect actually raises: a GENERIC DBAPIError
    # (never sqlalchemy.exc.DataError) whose orig chain carries the SQLSTATE.
    return DBAPIError("INSERT ...", {}, _FakePgError(sqlstate))


def test_is_permanent_data_error_classifies_by_sqlstate():
    assert is_permanent_data_error(_dbapi_error("22021"))  # NUL in text
    assert is_permanent_data_error(_dbapi_error("22P02"))  # invalid JSONB
    assert not is_permanent_data_error(_dbapi_error("40001"))  # serialization
    assert not is_permanent_data_error(_dbapi_error("08006"))  # connection
    assert not is_permanent_data_error(ConnectionError("db down"))
    # Other drivers that DO translate still classify correctly.
    assert is_permanent_data_error(DataError("stmt", {}, Exception("nul")))


# --------------------------------------------------- dispatch classification


class _StubBatcher:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc
        self.items: list = []

    async def submit(self, item) -> None:
        if self.exc is not None:
            raise self.exc
        self.items.append(item)


def make_dispatch(batcher):
    registry = EventHandlerRegistry()
    register_user_event_handlers(registry, batcher)
    consumer = EventConsumer(FakeMessageBus(), registry, ["q"])
    return consumer._handler_for("q")


def event_bytes(**data_overrides) -> bytes:
    return CloudEvent(id="evt-1", source="urn:test", type=USER_CREATED,
                      time=now_utc(), data=payload(**data_overrides)).to_bytes()


async def test_invalid_payload_is_acked_and_log_has_no_pii(caplog):
    batcher = _StubBatcher()
    handle = make_dispatch(batcher)
    secret_email = "very.secret@example.com"
    with caplog.at_level("WARNING"):
        await handle(event_bytes(email=secret_email.replace("@", "")))  # invalid
    assert batcher.items == []  # rejected before the batcher
    joined = " ".join(
        str(r.msg) + str(getattr(r, "reason", "")) for r in caplog.records
    )
    assert "event payload rejected" in joined
    assert "secret" not in joined  # the rejected value never reaches the log


async def test_consumer_floor_email_survives_strict_business_model():
    # Regression guard for the dispatch mapping: the business UserData model
    # is STRICT (StrictEmail), but the consumer path must keep accepting
    # floor-level emails VERBATIM (the payload model already validated them;
    # dispatch uses model_construct, never re-adjudicating).
    batcher = _StubBatcher()
    handle = make_dispatch(batcher)
    await handle(event_bytes(email="ops@backend"))
    assert len(batcher.items) == 1
    assert batcher.items[0].data.email == "ops@backend"  # verbatim, kept


async def test_real_driver_shaped_data_error_is_acked_not_requeued(caplog):
    # THE regression test for the dead `except DataError`: the exception the
    # asyncpg dialect actually raises (generic DBAPIError, SQLSTATE class 22
    # in the orig chain) must be classified permanent → return → ack.
    handle = make_dispatch(_StubBatcher(exc=_dbapi_error("22021")))
    with caplog.at_level("WARNING"):
        await handle(event_bytes())  # must NOT raise
    assert any("unstorable event" in r.message for r in caplog.records)


async def test_transient_db_errors_still_propagate_for_requeue():
    handle = make_dispatch(_StubBatcher(exc=_dbapi_error("08006")))  # conn lost
    with pytest.raises(DBAPIError):
        await handle(event_bytes())


async def test_batcher_closed_propagates_for_requeue():
    from app.messaging.batcher import BatcherClosedError

    # Shutdown must NACK in-flight messages, never ack them away.
    handle = make_dispatch(_StubBatcher(exc=BatcherClosedError("closing")))
    with pytest.raises(BatcherClosedError):
        await handle(event_bytes())


async def test_transient_errors_still_propagate_for_requeue():
    handle = make_dispatch(_StubBatcher(exc=ConnectionError("db down")))
    with pytest.raises(ConnectionError):
        await handle(event_bytes())
