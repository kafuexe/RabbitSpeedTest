"""End-to-end messaging tests: real RabbitMQ (via RabbitClient), real
PostgreSQL, the real consumer wiring."""
import asyncio
import json
import uuid

import pytest
from sqlalchemy import text

from app.messaging.cloudevents import CloudEvent, now_utc
from rabbit_client import RabbitClient
from tests.integration.conftest import requires_pg, requires_rabbit, make_settings

pytestmark = [requires_pg, requires_rabbit]

IN_QUEUE = "sds-test.events.in"
OUT_QUEUE = "sds-test.events.out"


@pytest.fixture
async def aux():
    """Independent client to inject inbound events and read outbound ones."""
    c = RabbitClient("amqp://guest:guest@localhost:5672/")
    await c.connect()
    for q in (IN_QUEUE, OUT_QUEUE):
        await c.delete_queue(q)
    yield c
    for q in (IN_QUEUE, OUT_QUEUE):
        await c.delete_queue(q)
    await c.close()


@pytest.fixture
async def running_consumer(container, aux):
    task = asyncio.create_task(container.event_consumer.run())
    yield container
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def user_event(event_type: str, uid: uuid.UUID, *, name: str, version: int,
               event_id: str | None = None) -> bytes:
    return CloudEvent(
        id=event_id or str(uuid.uuid4()),
        source="urn:test:producer",
        type=event_type,
        time=now_utc(),
        data={"id": str(uid), "name": name, "email": "e@example.com",
              "version": version},
    ).to_bytes()


async def fetch_user(container, uid: uuid.UUID):
    async with container.engine.connect() as conn:
        row = (await conn.execute(
            text("SELECT name, version FROM users WHERE id = :id"), {"id": uid}
        )).first()
    return row


async def wait_for(predicate, timeout: float = 10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met before timeout")


async def test_consume_create_duplicate_and_stale(running_consumer, aux):
    container = running_consumer
    uid = uuid.uuid4()

    await aux.publish(IN_QUEUE, user_event("user.created", uid, name="Eve", version=1,
                                           event_id="evt-create-1"))
    row = await wait_for(lambda: fetch_user(container, uid))
    assert (row.name, row.version) == ("Eve", 1)

    # exact duplicate delivery → inbox dedup, no change
    await aux.publish(IN_QUEUE, user_event("user.created", uid, name="Hacker", version=9,
                                           event_id="evt-create-1"))
    # newer update applies
    await aux.publish(IN_QUEUE, user_event("user.updated", uid, name="Eve II", version=2))
    await wait_for(lambda: _version_is(container, uid, 2))

    # stale event (version 1 again) → dropped
    await aux.publish(IN_QUEUE, user_event("user.updated", uid, name="Old", version=1))
    await asyncio.sleep(0.5)
    row = await fetch_user(container, uid)
    assert (row.name, row.version) == ("Eve II", 2)


async def _version_is(container, uid, version) -> bool:
    row = await fetch_user(container, uid)
    return bool(row and row.version == version)


async def test_invalid_and_unknown_events_do_not_kill_consumer(running_consumer, aux):
    container = running_consumer
    await aux.publish(IN_QUEUE, b"total garbage not json")
    await aux.publish(IN_QUEUE, CloudEvent(
        id="u-1", source="urn:test", type="alien.event").to_bytes())
    await aux.publish(IN_QUEUE, user_event(
        "user.created", uuid.uuid4(), name="", version=1))  # invalid payload

    uid = uuid.uuid4()
    await aux.publish(IN_QUEUE, user_event("user.created", uid, name="Survivor", version=1))
    row = await wait_for(lambda: fetch_user(container, uid))
    assert row.name == "Survivor"  # consumer still processing after the junk


async def test_api_create_publishes_cloudevent_after_commit(container, aux):
    from app.modules.user.business import UserData

    uid = uuid.uuid4()
    got: list[bytes] = []
    received = asyncio.Event()

    async def collect(body: bytes) -> None:
        got.append(body)
        received.set()

    consume_task = asyncio.create_task(aux.consume(OUT_QUEUE, collect))
    await asyncio.sleep(0.2)  # consumer registered

    user, created = await container.user_service.create(
        UserData(id=uid, name="Pub", email="p@example.com", attributes={})
    )
    assert created
    await asyncio.wait_for(received.wait(), timeout=10)
    consume_task.cancel()
    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    event = json.loads(got[0])
    assert event["specversion"] == "1.0"
    assert event["type"] == "user.created"
    assert event["data"]["id"] == str(uid)
    assert event["data"]["version"] == 1
