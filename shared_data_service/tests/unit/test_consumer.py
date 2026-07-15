"""Consumer edge behavior: invalid envelopes and unknown types are logged and
acked (return), valid events dispatch with correlation propagated."""
import pytest

from app.logging.correlation import get_correlation_id
from app.messaging.cloudevents import CloudEvent, now_utc
from app.messaging.consumer import EventConsumer
from app.messaging.registry import EventHandlerRegistry
from tests.fakes import FakeMessageBus


def make_handler(registry: EventHandlerRegistry):
    consumer = EventConsumer(FakeMessageBus(), registry, ["q"])
    return consumer._handler_for("q")


async def test_invalid_envelope_is_swallowed(caplog):
    handle = make_handler(EventHandlerRegistry())
    with caplog.at_level("WARNING"):
        await handle(b"this is not json")  # must NOT raise (raise = requeue loop)
    assert any("invalid CloudEvent" in r.message for r in caplog.records)


async def test_unknown_event_type_is_swallowed(caplog):
    handle = make_handler(EventHandlerRegistry())
    event = CloudEvent(id="1", source="s", type="mystery.event", time=now_utc())
    with caplog.at_level("WARNING"):
        await handle(event.to_bytes())
    assert any("unknown event type" in r.message for r in caplog.records)


async def test_known_event_dispatches_and_sets_correlation():
    registry = EventHandlerRegistry()
    seen: list[tuple[str, str]] = []

    async def on_event(event: CloudEvent) -> None:
        seen.append((event.id, get_correlation_id()))

    registry.register("user.created", on_event)
    handle = make_handler(registry)
    event = CloudEvent(id="e-9", source="s", type="user.created", correlationid="corr-42")
    await handle(event.to_bytes())
    assert seen == [("e-9", "corr-42")]


async def test_handler_exception_propagates_for_requeue():
    registry = EventHandlerRegistry()

    async def failing(event: CloudEvent) -> None:
        raise TimeoutError("db down")  # transient → must propagate

    registry.register("user.created", failing)
    handle = make_handler(registry)
    event = CloudEvent(id="1", source="s", type="user.created")
    with pytest.raises(TimeoutError):
        await handle(event.to_bytes())


def test_registry_rejects_duplicate_registration():
    registry = EventHandlerRegistry()

    async def handler(event: CloudEvent) -> None: ...

    registry.register("t", handler)
    with pytest.raises(ValueError):
        registry.register("t", handler)
