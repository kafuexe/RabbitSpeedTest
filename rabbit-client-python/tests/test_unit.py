"""Broker-free unit tests for RabbitClient.

All broker traffic is faked by monkeypatching attributes on the imported
``aio_pika`` module (the library imports ``aio_pika`` wholesale, so patching
module attributes is seen by the code under test). No RabbitMQ required;
run with `python -m pytest -q`.
"""
import asyncio

import aio_pika
import pytest

from rabbit_client import RabbitClient

URL = "amqp://guest:guest@nowhere/"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeUnderlay:
    """Stands in for the aiormq channel the watchdog inspects."""

    def __init__(self) -> None:
        self.consumers: dict[str, object] = {}


class FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[aio_pika.Message, str]] = []

    async def publish(self, message: aio_pika.Message, routing_key: str) -> None:
        self.published.append((message, routing_key))


class FakeQueue:
    def __init__(self, name: str, underlay: FakeUnderlay) -> None:
        self.name = name
        self._underlay = underlay
        self.callback = None
        self.consume_tags: list[str] = []
        self.cancelled: list[str] = []

    async def consume(self, callback) -> str:
        self.callback = callback
        tag = f"ctag-{self.name}-{len(self.consume_tags)}"
        self.consume_tags.append(tag)
        self._underlay.consumers[tag] = callback
        return tag

    async def cancel(self, tag: str) -> None:
        self.cancelled.append(tag)
        self._underlay.consumers.pop(tag, None)


class FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = FakeExchange()
        self.declare_calls: list[tuple[str, bool]] = []
        self.deleted: list[str] = []
        self.qos: int | None = None
        self.queues: dict[str, FakeQueue] = {}
        self.underlay = FakeUnderlay()

    async def declare_queue(self, name: str, durable: bool = False) -> FakeQueue:
        self.declare_calls.append((name, durable))
        q = self.queues.get(name)
        if q is None:
            q = FakeQueue(name, self.underlay)
            self.queues[name] = q
        return q

    async def queue_delete(self, name: str) -> None:
        self.deleted.append(name)

    async def set_qos(self, prefetch_count: int) -> None:
        self.qos = prefetch_count

    async def get_underlay_channel(self) -> FakeUnderlay:
        return self.underlay


class FakeConnection:
    def __init__(self) -> None:
        self.is_closed = False
        self.connected = asyncio.Event()
        self.connected.set()
        self.channels: list[FakeChannel] = []

    async def channel(self, publisher_confirms: bool = False) -> FakeChannel:
        ch = FakeChannel()
        self.channels.append(ch)
        return ch

    async def close(self) -> None:
        self.is_closed = True
        self.connected.clear()


class FakeMessageChannel:
    """The raw channel hanging off an incoming message (ack/nack target)."""

    def __init__(self, events: list) -> None:
        self.events = events

    async def basic_ack(self, delivery_tag, wait=True) -> None:
        self.events.append(("ack", delivery_tag, wait))

    async def basic_nack(self, delivery_tag, requeue=False) -> None:
        self.events.append(("nack", delivery_tag, requeue))


class FakeIncomingMessage:
    def __init__(self, body: bytes, channel: FakeMessageChannel, delivery_tag: int) -> None:
        self.body = body
        self.channel = channel
        self.delivery_tag = delivery_tag


async def connected_client(monkeypatch, **kwargs):
    """RabbitClient wired to fakes via aio_pika.connect_robust.

    Returns (client, pub_channel, con_channel).
    """
    conns: list[FakeConnection] = []

    async def fake_connect(url: str) -> FakeConnection:
        conn = FakeConnection()
        conns.append(conn)
        return conn

    monkeypatch.setattr(aio_pika, "connect_robust", fake_connect)
    client = RabbitClient(URL, **kwargs)
    await client.connect()
    pub_conn, con_conn = conns
    return client, pub_conn.channels[0], con_conn.channels[0]


# ---------------------------------------------------------------------------
# (a) connect() partial-failure cleanup
# ---------------------------------------------------------------------------

async def test_connect_second_failure_closes_survivor_and_reraises(monkeypatch):
    survivor = FakeConnection()
    calls = {"n": 0}

    async def flaky_connect(url: str):
        calls["n"] += 1
        if calls["n"] == 1:
            return survivor
        raise ConnectionError("second connect failed")

    monkeypatch.setattr(aio_pika, "connect_robust", flaky_connect)
    client = RabbitClient(URL)
    with pytest.raises(ConnectionError, match="second connect failed"):
        await client.connect()
    assert survivor.is_closed, "surviving connection must be closed, not leaked"
    assert client._pub_conn is None and client._con_conn is None
    assert client.is_connected is False


async def test_connect_both_failures_reraises_the_first(monkeypatch):
    errors = [ConnectionError("first"), OSError("second")]

    async def failing_connect(url: str):
        raise errors.pop(0)

    monkeypatch.setattr(aio_pika, "connect_robust", failing_connect)
    client = RabbitClient(URL)
    with pytest.raises(ConnectionError, match="first"):
        await client.connect()


async def test_connect_survivor_close_error_does_not_mask_failure(monkeypatch):
    class BadCloseConnection(FakeConnection):
        async def close(self) -> None:
            raise RuntimeError("close blew up")

    calls = {"n": 0}

    async def flaky_connect(url: str):
        calls["n"] += 1
        if calls["n"] == 1:
            return BadCloseConnection()
        raise ConnectionError("the real failure")

    monkeypatch.setattr(aio_pika, "connect_robust", flaky_connect)
    client = RabbitClient(URL)
    with pytest.raises(ConnectionError, match="the real failure"):
        await client.connect()


# ---------------------------------------------------------------------------
# (b) publish: declare caching + delivery mode
# ---------------------------------------------------------------------------

async def test_publish_declares_each_queue_once(monkeypatch):
    client, pub, _ = await connected_client(monkeypatch)
    for _ in range(3):
        await client.publish("jobs", b"x")
    await client.publish("other", b"y")
    assert pub.declare_calls == [("jobs", True), ("other", True)]
    assert len(pub.default_exchange.published) == 4


async def test_publish_routes_to_default_exchange_with_queue_as_routing_key(monkeypatch):
    client, pub, _ = await connected_client(monkeypatch)
    await client.publish("jobs", b"payload")
    [(message, routing_key)] = pub.default_exchange.published
    assert routing_key == "jobs"
    assert message.body == b"payload"


@pytest.mark.parametrize("durable, mode", [
    (True, aio_pika.DeliveryMode.PERSISTENT),
    (False, aio_pika.DeliveryMode.NOT_PERSISTENT),
])
async def test_publish_delivery_mode_follows_durable_flag(monkeypatch, durable, mode):
    client, pub, _ = await connected_client(monkeypatch, durable=durable)
    await client.publish("jobs", b"x")
    [(message, _)] = pub.default_exchange.published
    assert message.delivery_mode == mode


# ---------------------------------------------------------------------------
# (c) publish_many pipeline batching
# ---------------------------------------------------------------------------

async def test_publish_many_batches_confirms_in_1000s(monkeypatch):
    client, pub, _ = await connected_client(monkeypatch)
    real_gather = asyncio.gather
    batch_sizes: list[int] = []

    def spying_gather(*aws, **kwargs):
        batch_sizes.append(len(aws))
        return real_gather(*aws, **kwargs)

    monkeypatch.setattr(asyncio, "gather", spying_gather)
    await client.publish_many("jobs", [b"m"] * 2500)
    assert batch_sizes == [1000, 1000, 500]
    assert len(pub.default_exchange.published) == 2500


async def test_publish_many_single_small_batch(monkeypatch):
    client, pub, _ = await connected_client(monkeypatch)
    real_gather = asyncio.gather
    batch_sizes: list[int] = []

    def spying_gather(*aws, **kwargs):
        batch_sizes.append(len(aws))
        return real_gather(*aws, **kwargs)

    monkeypatch.setattr(asyncio, "gather", spying_gather)
    await client.publish_many("jobs", [b"m"] * 3)
    assert batch_sizes == [3]
    assert len(pub.default_exchange.published) == 3


# ---------------------------------------------------------------------------
# (d) consume: ack after handler / nack on failure
# ---------------------------------------------------------------------------

async def _start_consumer(client, con: FakeChannel, queue: str, handler):
    task = asyncio.create_task(client.consume(queue, handler))
    for _ in range(20):
        await asyncio.sleep(0)
        q = con.queues.get(queue)
        if q is not None and q.callback is not None:
            return task, q
    raise AssertionError("consumer was never registered")


async def test_consume_acks_once_after_handler_completes(monkeypatch):
    client, _, con = await connected_client(monkeypatch, cancel_check_interval=60)
    events: list = []

    async def handler(body: bytes) -> None:
        await asyncio.sleep(0)  # yield, like real async work
        events.append(("handler_done", body))

    task, q = await _start_consumer(client, con, "jobs", handler)
    msg = FakeIncomingMessage(b"payload", FakeMessageChannel(events), delivery_tag=7)
    await q.callback(msg)
    assert events == [("handler_done", b"payload"), ("ack", 7, False)], \
        "exactly one ack, with wait=False, strictly after the handler finished"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_consume_nacks_with_requeue_and_never_acks_on_handler_error(monkeypatch):
    client, _, con = await connected_client(monkeypatch, cancel_check_interval=60)
    events: list = []

    async def handler(body: bytes) -> None:
        raise RuntimeError("handler failure")

    task, q = await _start_consumer(client, con, "jobs", handler)
    msg = FakeIncomingMessage(b"poison", FakeMessageChannel(events), delivery_tag=9)
    await q.callback(msg)
    assert events == [("nack", 9, True)]
    assert not any(e[0] == "ack" for e in events)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# (e) delete_queue clears both caches
# ---------------------------------------------------------------------------

async def test_delete_queue_clears_publish_and_consume_caches(monkeypatch):
    client, pub, con = await connected_client(monkeypatch, cancel_check_interval=60)
    await client.publish("jobs", b"x")  # seeds the publish-declare cache

    async def handler(body: bytes) -> None:  # pragma: no cover - no traffic
        pass

    task, _ = await _start_consumer(client, con, "jobs", handler)  # seeds consume cache
    assert "jobs" in client._declared_pub
    assert "jobs" in client._con_queues

    await client.delete_queue("jobs")
    assert pub.deleted == ["jobs"]
    assert "jobs" not in client._declared_pub
    assert "jobs" not in client._con_queues

    # Next publish must re-declare, not trust the stale cache.
    await client.publish("jobs", b"y")
    assert pub.declare_calls.count(("jobs", True)) == 2
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# (f) is_connected: `connected` event, not just is_closed
# ---------------------------------------------------------------------------

async def test_is_connected_false_while_reconnecting_even_if_not_closed(monkeypatch):
    client, _, _ = await connected_client(monkeypatch)
    assert client.is_connected is True
    # A robust connection mid-reconnect is NOT closed, but its `connected`
    # event is cleared — is_connected must report False.
    client._con_conn.connected.clear()
    assert client._con_conn.is_closed is False
    assert client.is_connected is False
    client._con_conn.connected.set()
    assert client.is_connected is True
