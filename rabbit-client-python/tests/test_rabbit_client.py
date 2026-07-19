"""Integration tests for RabbitClient — need a live broker on localhost:5672.

Skipped automatically when no broker is reachable, so `python -m pytest -q`
stays broker-free everywhere else.
"""
import asyncio
import socket

import aio_pika
import pytest

from rabbit_client import ConsumerCancelledError, RabbitClient

AMQP = "amqp://guest:guest@localhost:5672/"
QUEUE = "rabbit_client_test"


def _broker_up() -> bool:
    try:
        with socket.create_connection(("localhost", 5672), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _broker_up(), reason="no local RabbitMQ broker")


@pytest.fixture
async def client():
    c = RabbitClient(AMQP)
    await c.connect()
    await c.delete_queue(QUEUE)
    yield c
    await c.delete_queue(QUEUE)
    await c.close()


async def test_publish_and_consume_end_to_end(client):
    await client.publish_many(QUEUE, [b"m%d" % i for i in range(50)])
    got: list[bytes] = []
    done = asyncio.Event()

    async def handler(body: bytes) -> None:
        got.append(body)
        if len(got) == 50:
            done.set()

    task = asyncio.create_task(client.consume(QUEUE, handler))
    await asyncio.wait_for(done.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
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

    task = asyncio.create_task(client.consume(QUEUE, handler))
    # The poison message is requeued after the first failure and eventually
    # processed on redelivery — at-least-once, nothing lost.
    await asyncio.wait_for(done.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sorted(seen) == [b"ok-1", b"ok-2", b"poison"]


async def test_two_connections_isolate_publish_from_consume(client):
    # Broker flow control on a busy publisher must not stall consumers.
    assert client._pub_conn is not client._con_conn


async def test_broker_side_consumer_cancel_is_detected():
    # Deleting a consumed queue makes the broker send Basic.Cancel, which
    # aio-pika swallows silently (consumers are only restored on RECONNECT).
    # The watchdog must turn that into a raise so callers can retry.
    c = RabbitClient(AMQP, cancel_check_interval=0.2)
    await c.connect()
    q = QUEUE + "_cancel"
    await c.delete_queue(q)

    async def handler(body: bytes) -> None:  # pragma: no cover - no traffic
        pass

    task = asyncio.create_task(c.consume(q, handler))
    await asyncio.sleep(0.3)  # consumer established
    await c.delete_queue(q)  # broker cancels our consumer, silently
    with pytest.raises(ConsumerCancelledError):
        await asyncio.wait_for(task, timeout=5)
    await c.close()


async def test_connect_partial_failure_closes_the_survivor(monkeypatch):
    # gather() with one failed connect must not leak the successful robust
    # connection (reconnect machinery alive, unreachable by close()).
    closed: list[object] = []

    class FakeConn:
        is_closed = False

        async def close(self) -> None:
            closed.append(self)

    calls = {"n": 0}

    async def flaky_connect(url):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeConn()
        raise ConnectionError("connection limit reached")

    monkeypatch.setattr(aio_pika, "connect_robust", flaky_connect)
    c = RabbitClient(AMQP)
    with pytest.raises(ConnectionError):
        await c.connect()
    assert len(closed) == 1  # the survivor was closed, not leaked
    assert c._pub_conn is None and c._con_conn is None


async def test_is_connected_reflects_lifecycle():
    c = RabbitClient(AMQP)
    assert c.is_connected is False  # never connected
    await c.connect()
    assert c.is_connected is True
    await c.close()
    assert c.is_connected is False  # closed connections are not "connected"


async def test_publish_declares_queue_only_once(client):
    calls = 0
    orig = client._pub_channel.declare_queue

    async def counting(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await orig(*args, **kwargs)

    client._pub_channel.declare_queue = counting
    for _ in range(3):
        await client.publish(QUEUE, b"x")
    assert calls == 1  # declared once, cached afterwards


async def test_consumes_many_queues_concurrently(client):
    q2 = QUEUE + "_2"
    await client.delete_queue(q2)
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

    t1 = asyncio.create_task(client.consume(QUEUE, make_handler(QUEUE)))
    t2 = asyncio.create_task(client.consume(q2, make_handler(q2)))
    await asyncio.wait_for(done.wait(), timeout=10)
    for t in (t1, t2):
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
    assert got[QUEUE] == [b"a"] * 3
    assert got[q2] == [b"b"] * 3
    await client.delete_queue(q2)


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

    task = asyncio.create_task(client.consume(QUEUE, handler))
    await asyncio.wait_for(done.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Ten 50ms handlers completing in well under 500ms means they overlapped.
    assert peak > 1, "handlers ran serially; DB awaits would not overlap"
