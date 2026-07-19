"""Broker-free tests for the consume() broker-cancel watchdog, the
auto-recovery loop behind Consumer handles, and the connect()/close()
lifecycle edges.

The watchdog is the subtlest code in the library: a broker-sent Basic.Cancel
silently removes a consumer (aio-pika raises nothing and only restores
consumers on reconnect), so each consumer's internal task polls the
underlying aiormq channel's consumer table. Since v0.2.0 a detected cancel is
no longer surfaced to the caller: the task logs a WARNING, backs off, then
re-declares the queue and re-consumes (parity with the TypeScript client's
amqp-connection-manager behavior). These tests drive every branch with the
shared fakes from conftest.py:

- silent consumer disappearance  -> WARNING + re-declare + re-consume
- one miss then reappearance     -> no recovery (misses reset)
- connection mid-reconnect       -> no false positive
- underlay channel None (reset)  -> no false positive
- fresh underlay object adopted  -> no false positive while robust restore runs
- recovery machinery failing     -> the error surfaces through Consumer.wait()
- cancel-RPC failure on exit     -> robust bookkeeping purged (no duplicate
  consumer resurrection on the next reconnect)

Timing-sensitive scenarios gate on the fake channel's poll counter
(FakeChannel.wait_for_polls) and the fake queue's consume counter
(FakeQueue.wait_for_consumes) instead of wall-clock sleeps, so an event-loop
stall on a loaded runner cannot change what the watchdog observes.
"""

import asyncio
import logging

import pytest
from conftest import FAKE_URL, FakeQueue, FakeUnderlay, connected_client, start_consumer

import hs_rabbit_client.client as client_module
from hs_rabbit_client import Consumer, RabbitClient

INTERVAL = 0.01  # keep the watchdog (and the re-consume backoff) fast in tests

WARNING_MESSAGE = "consumer cancelled by broker; re-declaring and resuming"


async def consuming_client(monkeypatch, queue: str = "jobs", prefetch: int | None = None):
    """A fake-wired client with a fast watchdog/backoff and one running consumer."""
    monkeypatch.setattr(client_module, "_RECONSUME_BACKOFF", INTERVAL)
    ctx = await connected_client(monkeypatch, cancel_check_interval=INTERVAL)
    consumer, q, channel = await start_consumer(ctx.client, ctx.con_conn, queue, prefetch=prefetch)
    return ctx, consumer, q, channel


async def assert_consumer_stays_quiet(consumer: Consumer, q: FakeQueue, intervals: float = 6):
    """The consumer must still be on its ORIGINAL basic.consume (no recovery
    fired, no internal error) after several watchdog periods, and must still
    cancel cleanly."""
    await asyncio.sleep(INTERVAL * intervals)
    assert not consumer._task.done(), f"consumer task died: {consumer._task}"
    assert len(q.consume_tags) == 1, "watchdog false positive: recovery re-consumed"
    await consumer.cancel()


def recovery_warnings(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.message == WARNING_MESSAGE]


# ---------------------------------------------------------------------------
# Genuine broker-side cancel -> auto-recovery
# ---------------------------------------------------------------------------


async def test_silent_cancel_logs_warning_then_redeclares_and_reconsumes(monkeypatch, caplog):
    ctx, consumer, q, channel = await consuming_client(monkeypatch)
    declares_before = channel.declare_calls.count(("jobs", True))

    # Broker deletes the queue: consumer vanishes from the aiormq channel
    # with NO exception raised anywhere — the exact failure mode aio-pika
    # swallows. Same underlay object, live connection.
    with caplog.at_level(logging.WARNING, logger="hs_rabbit_client"):
        channel.underlay.consumers.clear()
        await q.wait_for_consumes(2)  # recovery re-consumed

    [record] = recovery_warnings(caplog)
    assert record.levelno == logging.WARNING
    assert record.queue == "jobs"  # extra={"queue": ...}

    # Re-declared (cache was purged) and re-consumed; the handle never saw it.
    assert channel.declare_calls.count(("jobs", True)) == declares_before + 1
    assert len(q.consume_tags) == 2
    assert q.consume_tags[1] in channel.underlay.consumers
    assert not consumer._task.done()
    assert "jobs" in ctx.client._con_queues  # cache warm again after re-declare

    await consumer.cancel()
    assert await consumer.wait() is None


async def test_recovery_repeats_on_every_broker_cancel(monkeypatch, caplog):
    """The loop is 'forever until cancel()': a second broker cancel after a
    successful recovery is recovered from again."""
    _ctx, consumer, q, channel = await consuming_client(monkeypatch)

    with caplog.at_level(logging.WARNING, logger="hs_rabbit_client"):
        channel.underlay.consumers.clear()
        await q.wait_for_consumes(2)
        channel.underlay.consumers.clear()  # broker cancels the NEW consumer too
        await q.wait_for_consumes(3)

    assert len(recovery_warnings(caplog)) == 2
    await consumer.cancel()


async def test_recovery_reapplies_per_consume_prefetch(monkeypatch):
    """A per-consume prefetch override must be re-issued before every internal
    re-consume, not just the first basic.consume."""
    _ctx, consumer, q, channel = await consuming_client(monkeypatch, prefetch=7)
    assert channel.qos_calls == [200, 7]  # connect() default, then the override

    channel.underlay.consumers.clear()
    await q.wait_for_consumes(2)

    assert channel.qos_calls == [200, 7, 7]
    assert q.qos_at_consume == [7, 7], "qos in effect before BOTH basic.consume calls"
    await consumer.cancel()


async def test_single_miss_then_reappearance_does_not_recover(monkeypatch, caplog):
    """One polling miss must not trigger recovery — 2 consecutive misses are
    required, and a reappearance in between resets the counter."""
    _ctx, consumer, q, channel = await consuming_client(monkeypatch)
    tag = next(iter(channel.underlay.consumers))

    # Vanish for exactly one observed poll, then come back. Gated on the
    # watchdog's own poll counter, not wall-clock: no await between the
    # snapshot and the clear, so the first poll >= polls_before + 1 is
    # guaranteed to see the gap, and the restore below runs before the
    # watchdog can poll again (it is parked in its sleep).
    saved = dict(channel.underlay.consumers)
    polls_before = channel.underlay_polls
    channel.underlay.consumers.clear()
    with caplog.at_level(logging.WARNING, logger="hs_rabbit_client"):
        await channel.wait_for_polls(polls_before + 1)  # exactly one miss observed
        channel.underlay.consumers.update(saved)  # back before the second poll

        await assert_consumer_stays_quiet(consumer, q)
    assert recovery_warnings(caplog) == []
    assert tag not in channel.underlay.consumers  # cancelled on exit


# ---------------------------------------------------------------------------
# Reconnect scenarios must NOT be mistaken for a cancel
# ---------------------------------------------------------------------------


async def test_no_false_positive_while_connection_is_reconnecting(monkeypatch):
    """During an outage the consumer is gone AND the connection is down; the
    robust machinery will restore it on reconnect. Watchdog must not fire."""
    ctx, consumer, q, channel = await consuming_client(monkeypatch)

    channel.underlay.consumers.clear()  # consumers lost with the connection
    ctx.con_conn.connected.clear()  # robust connection mid-reconnect (not closed!)
    assert ctx.con_conn.is_closed is False

    await assert_consumer_stays_quiet(consumer, q)


async def test_no_false_positive_while_channel_is_resetting(monkeypatch):
    """Connection is back but the channel is still re-initializing (underlay
    unavailable). Watchdog must treat that as restore-in-progress."""
    _ctx, consumer, q, channel = await consuming_client(monkeypatch)

    channel.underlay.consumers.clear()
    channel.underlay_none = True  # get_underlay_channel() fails -> None

    await assert_consumer_stays_quiet(consumer, q)


async def test_fresh_underlay_is_adopted_without_recovery(monkeypatch):
    """After a reconnect a NEW aiormq channel appears whose consumer table the
    robust machinery is still refilling. The watchdog must adopt the new
    object and give the restore a full 2-miss grace, not fire instantly."""
    _ctx, consumer, q, channel = await consuming_client(monkeypatch)
    tag = next(iter(channel.underlay.consumers))

    # Reconnect: brand-new empty underlay object. Wait for the watchdog to
    # actually observe it once (the adoption poll, which resets the miss
    # counter) — poll-gated, so a stalled event loop cannot skew the count.
    polls_before = channel.underlay_polls
    channel.underlay = FakeUnderlay()
    await channel.wait_for_polls(polls_before + 1)  # adoption poll happened

    # Robust restore completes before the 2-miss grace elapses.
    channel.underlay.consumers[tag] = object()

    await assert_consumer_stays_quiet(consumer, q)


async def test_fresh_underlay_that_never_restores_triggers_recovery(monkeypatch, caplog):
    """Adoption is a grace period, not amnesty: if the consumer never comes
    back on the new channel, the recovery loop must still kick in."""
    _ctx, consumer, q, channel = await consuming_client(monkeypatch)

    with caplog.at_level(logging.WARNING, logger="hs_rabbit_client"):
        channel.underlay = FakeUnderlay()  # new channel, consumer never restored
        await q.wait_for_consumes(2)

    assert len(recovery_warnings(caplog)) == 1
    await consumer.cancel()


# ---------------------------------------------------------------------------
# Errors of the recovery machinery itself surface through wait()
# ---------------------------------------------------------------------------


async def test_recovery_failure_is_reraised_by_wait(monkeypatch):
    """ConsumerCancelledError is absorbed by the recovery loop, but an
    UNEXPECTED internal error (here: the re-declare failing hard) must kill
    the task and re-raise from Consumer.wait()."""
    _ctx, consumer, _q, channel = await consuming_client(monkeypatch)

    channel.declare_error = ConnectionError("re-declare refused")
    channel.underlay.consumers.clear()  # trigger the recovery path

    with pytest.raises(ConnectionError, match="re-declare refused"):
        await asyncio.wait_for(consumer.wait(), timeout=INTERVAL * 200)
    # cancel() after the fact is safe and idempotent.
    await consumer.cancel()


# ---------------------------------------------------------------------------
# Consumer exit: cancel-RPC failure must purge robust bookkeeping
# ---------------------------------------------------------------------------


async def test_failed_cancel_rpc_purges_robust_bookkeeping(monkeypatch):
    """If q.cancel() fails (broken channel), RobustQueue would keep the tag
    and resurrect the consumer on the next reconnect alongside a new one.
    The consumer task must purge the bookkeeping and the queue cache."""
    ctx, consumer, q, _channel = await consuming_client(monkeypatch)
    tag = next(iter(q._consumers))
    q.cancel_error = ConnectionError("channel is broken")

    await consumer.cancel()

    assert tag not in q._consumers, "stale tag would resurrect a duplicate consumer"
    assert "jobs" not in ctx.client._con_queues


async def test_successful_cancel_via_handle(monkeypatch):
    """Normal shutdown: the consumer is cancelled via the RPC and the robust
    bookkeeping path is NOT force-purged (queue cache stays warm)."""
    ctx, consumer, q, _channel = await consuming_client(monkeypatch)
    tag = next(iter(q._consumers))

    await consumer.cancel()
    assert q.cancelled == [tag]
    assert "jobs" in ctx.client._con_queues  # cache reusable for the next consume


# ---------------------------------------------------------------------------
# close() / connect() lifecycle edges
# ---------------------------------------------------------------------------


async def test_close_is_idempotent(monkeypatch):
    ctx = await connected_client(monkeypatch)
    await ctx.client.close()
    await ctx.client.close()  # second close: no error, no double close() call
    assert ctx.pub_conn.close_calls == 1
    assert ctx.con_conn.close_calls == 1
    assert ctx.client.is_connected is False


async def test_close_before_connect_is_a_safe_noop():
    client = RabbitClient(FAKE_URL)
    await client.close()  # never connected: nothing to close, no crash
    assert client.is_connected is False


async def test_reconnect_after_close_resets_caches_and_state(monkeypatch):
    ctx = await connected_client(monkeypatch)
    client = ctx.client
    client._declared_pub.add("stale")
    client._con_queues["stale"] = object()
    await client.close()

    await client.connect()  # fresh connections, fresh caches
    assert client.is_connected is True
    assert client._declared_pub == set()
    assert client._con_queues == {}
    assert client._pub_conn is not ctx.pub_conn
    assert client._con_conn is not ctx.con_conn


async def test_is_connected_requires_both_connections(monkeypatch):
    ctx = await connected_client(monkeypatch)
    assert ctx.client.is_connected is True
    ctx.pub_conn.connected.clear()  # publish side drops; consume side still up
    assert ctx.client.is_connected is False
    ctx.pub_conn.connected.set()
    assert ctx.client.is_connected is True
    ctx.con_conn.is_closed = True
    assert ctx.client.is_connected is False


def test_is_connected_false_before_connect():
    assert RabbitClient(FAKE_URL).is_connected is False
