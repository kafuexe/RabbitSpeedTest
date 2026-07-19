"""Broker-free tests for the consume() broker-cancel watchdog and the
connect()/close() lifecycle edges.

The watchdog is the subtlest code in the library: a broker-sent Basic.Cancel
silently removes a consumer (aio-pika raises nothing and only restores
consumers on reconnect), so consume() polls the underlying aiormq channel's
consumer table. These tests drive every branch of that loop with fakes:

- silent consumer disappearance  -> ConsumerCancelledError after 2 misses
- one miss then reappearance     -> no raise (misses reset)
- connection mid-reconnect       -> no false positive
- underlay channel None (reset)  -> no false positive
- fresh underlay object adopted  -> no false positive while robust restore runs
- cancel-RPC failure on exit     -> robust bookkeeping purged (no duplicate
  consumer resurrection on the next reconnect)
"""

import asyncio

import aio_pika
import pytest

from rabbit_client import ConsumerCancelledError, RabbitClient

URL = "amqp://guest:guest@nowhere/"
INTERVAL = 0.01  # keep the watchdog fast in tests


# ---------------------------------------------------------------------------
# Fakes (watchdog-focused: swappable underlay, failable cancel)
# ---------------------------------------------------------------------------


class FakeUnderlay:
    """Stands in for the aiormq channel whose .consumers the watchdog polls."""

    def __init__(self) -> None:
        self.consumers: dict[str, object] = {}


class FakeQueue:
    def __init__(self, name: str, channel: "FakeChannel") -> None:
        self.name = name
        self._channel = channel
        self.cancelled: list[str] = []
        self.cancel_error: Exception | None = None
        self._consumers: dict[str, object] = {}  # RobustQueue bookkeeping

    async def consume(self, callback) -> str:
        tag = f"ctag-{self.name}"
        self._channel.underlay.consumers[tag] = callback
        self._consumers[tag] = callback
        return tag

    async def cancel(self, tag: str) -> None:
        if self.cancel_error is not None:
            raise self.cancel_error
        self.cancelled.append(tag)
        self._channel.underlay.consumers.pop(tag, None)
        self._consumers.pop(tag, None)


class FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = object()
        self.underlay = FakeUnderlay()
        self.underlay_none = False  # simulate "channel resetting"
        self.queues: dict[str, FakeQueue] = {}

    async def declare_queue(self, name: str, durable: bool = False) -> FakeQueue:
        q = self.queues.get(name)
        if q is None:
            q = FakeQueue(name, self)
            self.queues[name] = q
        return q

    async def queue_delete(self, name: str) -> None:
        pass

    async def set_qos(self, prefetch_count: int) -> None:
        pass

    async def get_underlay_channel(self) -> FakeUnderlay:
        if self.underlay_none:
            raise RuntimeError("channel is resetting")
        return self.underlay


class FakeConnection:
    def __init__(self) -> None:
        self.is_closed = False
        self.connected = asyncio.Event()
        self.connected.set()
        self.channels: list[FakeChannel] = []
        self.close_calls = 0

    async def channel(self, publisher_confirms: bool = False) -> FakeChannel:
        ch = FakeChannel()
        self.channels.append(ch)
        return ch

    async def close(self) -> None:
        self.close_calls += 1
        self.is_closed = True
        self.connected.clear()


async def connected_client(monkeypatch, **kwargs):
    """RabbitClient wired to fakes. Returns (client, pub_conn, con_conn)."""
    conns: list[FakeConnection] = []

    async def fake_connect(url: str) -> FakeConnection:
        conn = FakeConnection()
        conns.append(conn)
        return conn

    monkeypatch.setattr(aio_pika, "connect_robust", fake_connect)
    client = RabbitClient(URL, cancel_check_interval=INTERVAL, **kwargs)
    await client.connect()
    return client, conns[0], conns[1]


async def start_consumer(client: RabbitClient, con: FakeConnection, queue: str):
    """Start consume() and wait until the consumer tag is registered."""
    channel = con.channels[0]

    async def handler(body: bytes) -> None:  # pragma: no cover - no traffic
        pass

    task = asyncio.create_task(client.consume(queue, handler))
    for _ in range(50):
        await asyncio.sleep(0)
        q = channel.queues.get(queue)
        if q is not None and channel.underlay.consumers:
            return task, q, channel
    raise AssertionError("consumer was never registered")


async def assert_watchdog_stays_quiet(task: asyncio.Task, intervals: float = 6) -> None:
    """The consume task must still be running (no false positive) after
    several watchdog periods, and must still cancel cleanly."""
    await asyncio.sleep(INTERVAL * intervals)
    assert not task.done(), f"watchdog false positive: {task}"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# Watchdog: genuine broker-side cancel
# ---------------------------------------------------------------------------


async def test_silent_consumer_disappearance_raises_after_two_misses(monkeypatch):
    client, _, con = await connected_client(monkeypatch)
    task, _q, channel = await start_consumer(client, con, "jobs")

    # Broker deletes the queue: consumer vanishes from the aiormq channel
    # with NO exception raised anywhere — the exact failure mode aio-pika
    # swallows. Same underlay object, live connection.
    channel.underlay.consumers.clear()

    with pytest.raises(ConsumerCancelledError, match="jobs"):
        await asyncio.wait_for(task, timeout=INTERVAL * 100)
    # The consume-side declare cache must be purged so a retry re-declares.
    assert "jobs" not in client._con_queues


async def test_single_miss_then_reappearance_does_not_raise(monkeypatch):
    """One polling miss must not kill the consumer — 2 consecutive misses are
    required, and a reappearance in between resets the counter."""
    client, _, con = await connected_client(monkeypatch)
    task, _q, channel = await start_consumer(client, con, "jobs")
    tag = next(iter(channel.underlay.consumers))

    # Vanish for a bit less than one full interval, then come back: at most
    # one poll can observe the gap.
    saved = dict(channel.underlay.consumers)
    channel.underlay.consumers.clear()
    await asyncio.sleep(INTERVAL * 0.5)
    channel.underlay.consumers.update(saved)

    await assert_watchdog_stays_quiet(task)
    assert tag not in channel.underlay.consumers  # cancelled on exit


async def test_cancel_error_message_names_the_queue(monkeypatch):
    client, _, con = await connected_client(monkeypatch)
    task, _, channel = await start_consumer(client, con, "orders_q")
    channel.underlay.consumers.clear()
    with pytest.raises(ConsumerCancelledError, match=r"'orders_q'"):
        await asyncio.wait_for(task, timeout=INTERVAL * 100)


# ---------------------------------------------------------------------------
# Watchdog: reconnect scenarios must NOT be mistaken for a cancel
# ---------------------------------------------------------------------------


async def test_no_false_positive_while_connection_is_reconnecting(monkeypatch):
    """During an outage the consumer is gone AND the connection is down; the
    robust machinery will restore it on reconnect. Watchdog must not raise."""
    client, _, con = await connected_client(monkeypatch)
    task, _, channel = await start_consumer(client, con, "jobs")

    channel.underlay.consumers.clear()  # consumers lost with the connection
    con.connected.clear()  # robust connection mid-reconnect (not closed!)
    assert con.is_closed is False

    await assert_watchdog_stays_quiet(task)


async def test_no_false_positive_while_channel_is_resetting(monkeypatch):
    """Connection is back but the channel is still re-initializing (underlay
    unavailable). Watchdog must treat that as restore-in-progress."""
    client, _, con = await connected_client(monkeypatch)
    task, _, channel = await start_consumer(client, con, "jobs")

    channel.underlay.consumers.clear()
    channel.underlay_none = True  # get_underlay_channel() fails -> None

    await assert_watchdog_stays_quiet(task)


async def test_fresh_underlay_is_adopted_without_raising(monkeypatch):
    """After a reconnect a NEW aiormq channel appears whose consumer table the
    robust machinery is still refilling. The watchdog must adopt the new
    object and give the restore a full 2-miss grace, not raise instantly."""
    client, _, con = await connected_client(monkeypatch)
    task, _, channel = await start_consumer(client, con, "jobs")
    tag = next(iter(channel.underlay.consumers))

    # Reconnect: brand-new empty underlay object.
    channel.underlay = FakeUnderlay()
    await asyncio.sleep(INTERVAL * 1.5)  # adoption poll happens (misses = 0)

    # Robust restore completes before the next 2 polls elapse.
    channel.underlay.consumers[tag] = object()

    await assert_watchdog_stays_quiet(task)


async def test_fresh_underlay_that_never_restores_eventually_raises(monkeypatch):
    """Adoption is a grace period, not amnesty: if the consumer never comes
    back on the new channel, the cancel must still surface."""
    client, _, con = await connected_client(monkeypatch)
    task, _, channel = await start_consumer(client, con, "jobs")

    channel.underlay = FakeUnderlay()  # new channel, consumer never restored

    with pytest.raises(ConsumerCancelledError):
        await asyncio.wait_for(task, timeout=INTERVAL * 100)


# ---------------------------------------------------------------------------
# consume() exit: cancel-RPC failure must purge robust bookkeeping
# ---------------------------------------------------------------------------


async def test_failed_cancel_rpc_purges_robust_bookkeeping(monkeypatch):
    """If q.cancel() fails (broken channel), RobustQueue would keep the tag
    and resurrect the consumer on the next reconnect alongside the retry's
    new one. consume() must purge the bookkeeping and the queue cache."""
    client, _, con = await connected_client(monkeypatch)
    task, q, _channel = await start_consumer(client, con, "jobs")
    tag = next(iter(q._consumers))
    q.cancel_error = ConnectionError("channel is broken")

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert tag not in q._consumers, "stale tag would resurrect a duplicate consumer"
    assert "jobs" not in client._con_queues


async def test_successful_cancel_on_task_cancellation(monkeypatch):
    """Normal shutdown: the consumer is cancelled via the RPC and the robust
    bookkeeping path is NOT force-purged (queue cache stays warm)."""
    client, _, con = await connected_client(monkeypatch)
    task, q, _channel = await start_consumer(client, con, "jobs")
    tag = next(iter(q._consumers))

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert q.cancelled == [tag]
    assert "jobs" in client._con_queues  # cache reusable for the next consume


# ---------------------------------------------------------------------------
# close() / connect() lifecycle edges
# ---------------------------------------------------------------------------


async def test_close_is_idempotent(monkeypatch):
    client, pub, con = await connected_client(monkeypatch)
    await client.close()
    await client.close()  # second close: no error, no double close() call
    assert pub.close_calls == 1
    assert con.close_calls == 1
    assert client.is_connected is False


async def test_close_before_connect_is_a_safe_noop():
    client = RabbitClient(URL)
    await client.close()  # never connected: nothing to close, no crash
    assert client.is_connected is False


async def test_reconnect_after_close_resets_caches_and_state(monkeypatch):
    client, pub, con = await connected_client(monkeypatch)
    client._declared_pub.add("stale")
    client._con_queues["stale"] = object()
    await client.close()

    await client.connect()  # fresh connections, fresh caches
    assert client.is_connected is True
    assert client._declared_pub == set()
    assert client._con_queues == {}
    assert client._pub_conn is not pub
    assert client._con_conn is not con


async def test_is_connected_requires_both_connections(monkeypatch):
    client, pub, con = await connected_client(monkeypatch)
    assert client.is_connected is True
    pub.connected.clear()  # publish side drops; consume side still up
    assert client.is_connected is False
    pub.connected.set()
    assert client.is_connected is True
    con.is_closed = True
    assert client.is_connected is False


def test_is_connected_false_before_connect():
    assert RabbitClient(URL).is_connected is False
