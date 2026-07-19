"""Integration tests for RabbitClient — need a live broker.

Broker coordinates come from RABBIT_HOST/RABBIT_PORT (default localhost:5672,
see conftest.py). Skipped automatically when no broker is reachable, so
`python -m pytest -q` stays broker-free everywhere else. Every test here
talks to the real broker; fully-mocked coverage lives in test_unit.py and
test_watchdog.py.
"""

import asyncio
import logging

import aio_pika
import pytest
from conftest import AMQP_URL, broker_up

import rabbit_client.client as client_module
from rabbit_client import RabbitClient

QUEUE = "rabbit_client_test"

pytestmark = pytest.mark.skipif(not broker_up(), reason="no reachable RabbitMQ broker")


@pytest.fixture
async def client():
    c = RabbitClient(AMQP_URL)
    await c.connect()
    await c.delete_queue(QUEUE)
    yield c
    await c.close()  # cancels any consumer a failed test left behind
    # Fresh throwaway client for cleanup: c's channels are gone after close().
    sweeper = RabbitClient(AMQP_URL)
    await sweeper.connect()
    await sweeper.delete_queue(QUEUE)
    await sweeper.close()


async def test_publish_and_consume_end_to_end(client):
    await client.publish_many(QUEUE, [b"m%d" % i for i in range(50)])
    got: list[bytes] = []
    done = asyncio.Event()

    async def handler(body: bytes) -> None:
        got.append(body)
        if len(got) == 50:
            done.set()

    consumer = await client.consume(QUEUE, handler)
    assert consumer.queue == QUEUE
    await asyncio.wait_for(done.wait(), timeout=10)
    await consumer.cancel()
    assert await consumer.wait() is None  # wait() after cancel() returns None
    assert sorted(got) == sorted(b"m%d" % i for i in range(50))


async def test_failing_handler_requeues_message(client):
    await client.publish_many(QUEUE, [b"ok-1", b"poison", b"ok-2"])
    seen: list[bytes] = []
    done = asyncio.Event()
    failed_once = False

    async def handler(body: bytes) -> None:
        nonlocal failed_once
        if body == b"poison" and not failed_once:
            failed_once = True
            raise RuntimeError("transient failure")
        seen.append(body)
        if len(seen) == 3:
            done.set()

    consumer = await client.consume(QUEUE, handler)
    # The poison message is requeued after the first failure and eventually
    # processed on redelivery — at-least-once, nothing lost.
    await asyncio.wait_for(done.wait(), timeout=10)
    await consumer.cancel()
    assert sorted(seen) == [b"ok-1", b"ok-2", b"poison"]


async def test_two_connections_isolate_publish_from_consume(client):
    # Broker flow control on a busy publisher must not stall consumers.
    assert client._pub_conn is not client._con_conn


async def test_broker_side_cancel_recovers_and_consumption_resumes(monkeypatch, caplog):
    # Deleting a consumed queue makes the broker send Basic.Cancel, which
    # aio-pika swallows silently (consumers are only restored on RECONNECT).
    # The consumer's internal task must detect it, log a WARNING, re-declare
    # the queue and RESUME consuming — the handle never notices.
    monkeypatch.setattr(client_module, "_RECONSUME_BACKOFF", 0.2)
    c = RabbitClient(AMQP_URL, cancel_check_interval=0.2)
    await c.connect()
    q = QUEUE + "_cancel"
    consumer = None
    try:
        await c.delete_queue(q)
        got: list[bytes] = []
        received = asyncio.Event()

        async def handler(body: bytes) -> None:
            got.append(body)
            received.set()

        with caplog.at_level(logging.WARNING, logger="rabbit_client"):
            consumer = await c.consume(q, handler)
            await c.publish(q, b"before")  # prove the consumer is live
            await asyncio.wait_for(received.wait(), timeout=10)
            received.clear()

            await c.delete_queue(q)  # broker cancels our consumer, silently
            # publish() re-declares the queue (delete purged the cache); the
            # recovered consumer must pick the message up after re-consuming.
            await c.publish(q, b"after")
            await asyncio.wait_for(received.wait(), timeout=15)

        assert got == [b"before", b"after"]
        assert not consumer._task.done(), "recovery must keep the consumer running"
        assert any(
            r.getMessage() == "consumer cancelled by broker; re-declaring and resuming"
            for r in caplog.records
        )
    finally:
        if consumer is not None:
            await consumer.cancel()
        await c.delete_queue(q)  # idempotent: deleting a missing queue is OK
        await c.close()


async def test_consumes_many_queues_concurrently(client):
    q2 = QUEUE + "_2"
    await client.delete_queue(q2)
    consumers = []
    try:
        await client.publish_many(QUEUE, [b"a"] * 3)
        await client.publish_many(q2, [b"b"] * 3)
        got = {QUEUE: [], q2: []}
        done = asyncio.Event()

        def make_handler(name):
            async def handler(body: bytes) -> None:
                got[name].append(body)
                if sum(len(v) for v in got.values()) == 6:
                    done.set()

            return handler

        consumers.append(await client.consume(QUEUE, make_handler(QUEUE)))
        consumers.append(await client.consume(q2, make_handler(q2)))
        await asyncio.wait_for(done.wait(), timeout=10)
        assert got[QUEUE] == [b"a"] * 3
        assert got[q2] == [b"b"] * 3
    finally:
        for consumer in consumers:
            await consumer.cancel()  # cancelled BEFORE q2 is deleted below
        await client.delete_queue(q2)  # cleaned up even when the test fails


async def test_handlers_run_concurrently(client):
    await client.publish_many(QUEUE, [b"x"] * 10)
    in_flight = 0
    peak = 0
    done = asyncio.Event()
    processed = 0

    async def handler(body: bytes) -> None:
        nonlocal in_flight, peak, processed
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.05)  # a stand-in for the user's DB await
        in_flight -= 1
        processed += 1
        if processed == 10:
            done.set()

    consumer = await client.consume(QUEUE, handler)
    await asyncio.wait_for(done.wait(), timeout=10)
    await consumer.cancel()
    # Ten 50ms handlers completing in well under 500ms means they overlapped.
    assert peak > 1, "handlers ran serially; DB awaits would not overlap"


async def test_per_consume_prefetch_override_caps_in_flight(client):
    # Constructor prefetch is 200; the per-consume override must win.
    await client.publish_many(QUEUE, [b"x"] * 20)
    in_flight = 0
    peak = 0
    done = asyncio.Event()
    processed = 0

    async def handler(body: bytes) -> None:
        nonlocal in_flight, peak, processed
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.02)
        in_flight -= 1
        processed += 1
        if processed == 20:
            done.set()

    consumer = await client.consume(QUEUE, handler, prefetch=2)
    await asyncio.wait_for(done.wait(), timeout=10)
    await consumer.cancel()
    assert peak <= 2, f"prefetch=2 must cap concurrent handlers, saw {peak}"
    assert peak == 2, "with 20 queued messages the cap should actually be reached"


async def test_publish_properties_round_trip_via_raw_basic_get(client):
    # The bytes-only handler cannot observe properties, so assert them via a
    # second RAW aio-pika connection and basic.get.
    await client.publish(
        QUEUE,
        b"props-payload",
        persistent=True,
        headers={"x-source": "integration", "x-attempt": 2},
        correlation_id="corr-42",
        message_id="msg-42",
        content_type="application/json",
        priority=5,
    )
    raw = await aio_pika.connect(AMQP_URL)
    try:
        channel = await raw.channel()
        queue = await channel.declare_queue(QUEUE, durable=True)
        message = await queue.get(timeout=5)
        assert message is not None
        await message.ack()
        assert message.body == b"props-payload"
        assert message.headers == {"x-source": "integration", "x-attempt": 2}
        assert message.correlation_id == "corr-42"
        assert message.message_id == "msg-42"
        assert message.content_type == "application/json"
        assert message.priority == 5
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    finally:
        await raw.close()


async def test_close_cancels_live_consumer_and_wait_returns(client):
    async def handler(body: bytes) -> None:  # pragma: no cover - no traffic
        pass

    consumer = await client.consume(QUEUE, handler)
    waiter = asyncio.create_task(consumer.wait())
    await asyncio.sleep(0.1)
    assert not waiter.done()

    await client.close()
    assert await asyncio.wait_for(waiter, timeout=5) is None
