"""Broker-free unit tests for RabbitClient.

The fake aio_pika stack and the connected_client()/start_consumer() helpers
live in conftest.py (shared with test_watchdog.py). No RabbitMQ required;
run with `python -m pytest -q`.
"""

import asyncio

import aio_pika
import pytest
from conftest import (
    FAKE_URL,
    FakeConnection,
    FakeIncomingMessage,
    FakeMessageChannel,
    connected_client,
    start_consumer,
)

from hs_rabbit_client import RabbitClient

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
    client = RabbitClient(FAKE_URL)
    with pytest.raises(ConnectionError, match="second connect failed"):
        await client.connect()
    assert survivor.is_closed, "surviving connection must be closed, not leaked"
    assert client._pub_conn is None
    assert client._con_conn is None
    assert client.is_connected is False


async def test_connect_both_failures_reraises_the_first(monkeypatch):
    errors = [ConnectionError("first"), OSError("second")]

    async def failing_connect(url: str):
        raise errors.pop(0)

    monkeypatch.setattr(aio_pika, "connect_robust", failing_connect)
    client = RabbitClient(FAKE_URL)
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
    client = RabbitClient(FAKE_URL)
    with pytest.raises(ConnectionError, match="the real failure"):
        await client.connect()


# ---------------------------------------------------------------------------
# (b) publish: declare caching + delivery mode
# ---------------------------------------------------------------------------


async def test_publish_declares_each_queue_once(monkeypatch):
    ctx = await connected_client(monkeypatch)
    client, pub = ctx.client, ctx.pub_channel
    for _ in range(3):
        await client.publish("jobs", b"x")
    await client.publish("other", b"y")
    assert pub.declare_calls == [("jobs", True), ("other", True)]
    assert len(pub.default_exchange.published) == 4


async def test_publish_routes_to_default_exchange_with_queue_as_routing_key(monkeypatch):
    ctx = await connected_client(monkeypatch)
    await ctx.client.publish("jobs", b"payload")
    [(message, routing_key)] = ctx.pub_channel.default_exchange.published
    assert routing_key == "jobs"
    assert message.body == b"payload"


@pytest.mark.parametrize(
    ("durable", "mode"),
    [
        (True, aio_pika.DeliveryMode.PERSISTENT),
        (False, aio_pika.DeliveryMode.NOT_PERSISTENT),
    ],
)
async def test_publish_delivery_mode_follows_durable_flag(monkeypatch, durable, mode):
    ctx = await connected_client(monkeypatch, durable=durable)
    await ctx.client.publish("jobs", b"x")
    [(message, _)] = ctx.pub_channel.default_exchange.published
    assert message.delivery_mode == mode


# ---------------------------------------------------------------------------
# (c) publish_many pipeline batching
# ---------------------------------------------------------------------------


async def test_publish_many_batches_confirms_in_1000s(monkeypatch):
    ctx = await connected_client(monkeypatch)
    real_gather = asyncio.gather
    batch_sizes: list[int] = []

    def spying_gather(*aws, **kwargs):
        batch_sizes.append(len(aws))
        return real_gather(*aws, **kwargs)

    monkeypatch.setattr(asyncio, "gather", spying_gather)
    await ctx.client.publish_many("jobs", [b"m"] * 2500)
    assert batch_sizes == [1000, 1000, 500]
    assert len(ctx.pub_channel.default_exchange.published) == 2500


async def test_publish_many_single_small_batch(monkeypatch):
    ctx = await connected_client(monkeypatch)
    real_gather = asyncio.gather
    batch_sizes: list[int] = []

    def spying_gather(*aws, **kwargs):
        batch_sizes.append(len(aws))
        return real_gather(*aws, **kwargs)

    monkeypatch.setattr(asyncio, "gather", spying_gather)
    await ctx.client.publish_many("jobs", [b"m"] * 3)
    assert batch_sizes == [3]
    assert len(ctx.pub_channel.default_exchange.published) == 3


# ---------------------------------------------------------------------------
# (d) consume: ack after handler / nack on failure
# ---------------------------------------------------------------------------


async def test_consume_acks_once_after_handler_completes(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    events: list = []

    async def handler(body: bytes) -> None:
        await asyncio.sleep(0)  # yield, like real async work
        events.append(("handler_done", body))

    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs", handler)
    msg = FakeIncomingMessage(b"payload", FakeMessageChannel(events), delivery_tag=7)
    await q.callback(msg)
    assert events == [("handler_done", b"payload"), ("ack", 7, False)], (
        "exactly one ack, with wait=False, strictly after the handler finished"
    )
    await consumer.cancel()


async def test_consume_nacks_with_requeue_and_never_acks_on_handler_error(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    events: list = []

    async def handler(body: bytes) -> None:
        raise RuntimeError("handler failure")

    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs", handler)
    msg = FakeIncomingMessage(b"poison", FakeMessageChannel(events), delivery_tag=9)
    await q.callback(msg)
    assert events == [("nack", 9, True)]
    assert not any(e[0] == "ack" for e in events)
    await consumer.cancel()


# ---------------------------------------------------------------------------
# (e) delete_queue clears both caches
# ---------------------------------------------------------------------------


async def test_delete_queue_clears_publish_and_consume_caches(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    client, pub = ctx.client, ctx.pub_channel
    await client.publish("jobs", b"x")  # seeds the publish-declare cache

    consumer, _, _ = await start_consumer(client, ctx.con_conn, "jobs")  # seeds consume cache
    assert "jobs" in client._declared_pub
    assert "jobs" in client._con_queues

    await client.delete_queue("jobs")
    assert pub.deleted == ["jobs"]
    assert "jobs" not in client._declared_pub
    assert "jobs" not in client._con_queues

    # Next publish must re-declare, not trust the stale cache.
    await client.publish("jobs", b"y")
    assert pub.declare_calls.count(("jobs", True)) == 2
    await consumer.cancel()


# ---------------------------------------------------------------------------
# (f) is_connected: `connected` event, not just is_closed
# ---------------------------------------------------------------------------


async def test_is_connected_false_while_reconnecting_even_if_not_closed(monkeypatch):
    ctx = await connected_client(monkeypatch)
    assert ctx.client.is_connected is True
    # A robust connection mid-reconnect is NOT closed, but its `connected`
    # event is cleared — is_connected must report False.
    ctx.con_conn.connected.clear()
    assert ctx.con_conn.is_closed is False
    assert ctx.client.is_connected is False
    ctx.con_conn.connected.set()
    assert ctx.client.is_connected is True


# ---------------------------------------------------------------------------
# (g) not-connected misuse guard
# ---------------------------------------------------------------------------

NOT_CONNECTED = "hs-rabbit-client is not connected — call connect\\(\\) first"


async def test_unconnected_client_raises_runtime_error_everywhere():
    client = RabbitClient(FAKE_URL)

    async def handler(body: bytes) -> None:  # pragma: no cover - never called
        pass

    with pytest.raises(RuntimeError, match=NOT_CONNECTED):
        await client.publish("jobs", b"x")
    with pytest.raises(RuntimeError, match=NOT_CONNECTED):
        await client.publish_many("jobs", [b"x"])
    with pytest.raises(RuntimeError, match=NOT_CONNECTED):
        await client.consume("jobs", handler)
    with pytest.raises(RuntimeError, match=NOT_CONNECTED):
        await client.delete_queue("jobs")


# ---------------------------------------------------------------------------
# (h) per-publish overrides + properties passthrough
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("durable", "persistent", "mode"),
    [
        (False, True, aio_pika.DeliveryMode.PERSISTENT),  # override up
        (True, False, aio_pika.DeliveryMode.NOT_PERSISTENT),  # override down
        (True, None, aio_pika.DeliveryMode.PERSISTENT),  # None -> constructor
        (False, None, aio_pika.DeliveryMode.NOT_PERSISTENT),
    ],
)
async def test_publish_persistent_override(monkeypatch, durable, persistent, mode):
    ctx = await connected_client(monkeypatch, durable=durable)
    await ctx.client.publish("jobs", b"x", persistent=persistent)
    [(message, _)] = ctx.pub_channel.default_exchange.published
    assert message.delivery_mode == mode


async def test_publish_properties_map_onto_message_kwargs(monkeypatch):
    ctx = await connected_client(monkeypatch)
    await ctx.client.publish(
        "jobs",
        b"payload",
        headers={"x-retry": 3},
        correlation_id="corr-1",
        message_id="msg-1",
        content_type="application/json",
        expiration=5.0,
        priority=7,
    )
    [(message, routing_key)] = ctx.pub_channel.default_exchange.published
    assert routing_key == "jobs"
    assert message.headers == {"x-retry": 3}
    assert message.correlation_id == "corr-1"
    assert message.message_id == "msg-1"
    assert message.content_type == "application/json"
    assert message.expiration == 5.0  # seconds, passed straight to aio-pika
    assert message.priority == 7


async def test_publish_defaults_leave_properties_unset(monkeypatch):
    ctx = await connected_client(monkeypatch)
    await ctx.client.publish("jobs", b"payload")
    [(message, _)] = ctx.pub_channel.default_exchange.published
    assert message.headers == {}  # aio-pika normalizes None to empty headers
    assert message.correlation_id is None
    assert message.message_id is None
    assert message.content_type is None
    assert message.expiration is None
    assert message.priority == 0  # aio-pika normalizes a None priority to 0


async def test_publish_many_applies_properties_to_every_message(monkeypatch):
    ctx = await connected_client(monkeypatch)
    await ctx.client.publish_many(
        "jobs",
        [b"a", b"b", b"c"],
        persistent=True,
        headers={"batch": "1"},
        correlation_id="corr-batch",
        content_type="text/plain",
        priority=2,
    )
    published = ctx.pub_channel.default_exchange.published
    assert len(published) == 3
    for message, routing_key in published:
        assert routing_key == "jobs"
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert message.headers == {"batch": "1"}
        assert message.correlation_id == "corr-batch"
        assert message.content_type == "text/plain"
        assert message.priority == 2


# ---------------------------------------------------------------------------
# (i) Consumer handle lifecycle
# ---------------------------------------------------------------------------


async def test_consume_returns_handle_with_queue_attr(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs")
    assert consumer.queue == "jobs"
    assert q.callback is not None, "consumer must be established before consume() returns"
    await consumer.cancel()


async def test_consume_setup_error_raises_at_call_site(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    ctx.con_channel.declare_error = ConnectionError("declare refused")

    async def handler(body: bytes) -> None:  # pragma: no cover - never consumes
        pass

    with pytest.raises(ConnectionError, match="declare refused"):
        await ctx.client.consume("jobs", handler)


async def test_wait_parks_until_cancel_then_returns_none(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    consumer, _, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs")

    # wait() parks while the consumer runs; cancelling the WAITER (here via
    # wait_for's timeout) must not stop the consumer itself.
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(consumer.wait(), timeout=0.05)
    assert not consumer._task.done(), "a cancelled waiter must leave the consumer running"

    await consumer.cancel()
    assert await consumer.wait() is None  # returns promptly after cancel()


async def test_cancel_is_idempotent(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs")
    await consumer.cancel()
    await consumer.cancel()  # second call: no error, no second broker cancel
    assert q.cancelled == q.consume_tags[:1]


async def test_concurrent_cancels_await_the_same_cancellation(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs")
    await asyncio.gather(consumer.cancel(), consumer.cancel(), consumer.cancel())
    assert q.cancelled == q.consume_tags[:1], "exactly one broker-side cancel"
    assert await consumer.wait() is None


async def test_close_cancels_outstanding_consumers_so_wait_returns(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs")
    waiter = asyncio.create_task(consumer.wait())
    await asyncio.sleep(0)  # waiter parked

    await ctx.client.close()
    assert await asyncio.wait_for(waiter, timeout=1) is None
    assert q.cancelled == q.consume_tags[:1], "close() cancelled the consumer at the broker"
    assert ctx.con_conn.is_closed
    assert ctx.pub_conn.is_closed


# ---------------------------------------------------------------------------
# (j) per-consume prefetch override
# ---------------------------------------------------------------------------


async def test_consume_prefetch_override_sets_qos_before_consume(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    con = ctx.con_channel
    assert con.qos_calls == [200], "connect() applies the constructor prefetch"

    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs", prefetch=7)
    assert con.qos_calls == [200, 7]
    assert q.qos_at_consume == [7], "qos must be in effect BEFORE basic.consume"
    await consumer.cancel()


async def test_consume_without_override_issues_no_extra_qos(monkeypatch):
    ctx = await connected_client(monkeypatch, cancel_check_interval=60)
    consumer, q, _ = await start_consumer(ctx.client, ctx.con_conn, "jobs")
    assert ctx.con_channel.qos_calls == [200], "constructor prefetch stays the default"
    assert q.qos_at_consume == [200]
    await consumer.cancel()
