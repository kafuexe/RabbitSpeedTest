"""Shared test plumbing for the hs_rabbit_client suites.

Two things live here:

- The fake aio_pika stack used by the broker-free suites (test_unit.py,
  test_watchdog.py). The library imports ``aio_pika`` wholesale, so
  monkeypatching attributes on the module is seen by the code under test —
  no RabbitMQ required.
- The real-broker coordinates and reachability probe used by the integration
  suite (test_rabbit_client.py). Host/port come from RABBIT_HOST/RABBIT_PORT,
  defaulting to localhost:5672.
"""

import asyncio
import os
import socket
from typing import NamedTuple

import aio_pika
import pytest

from hs_rabbit_client import RabbitClient

# ---------------------------------------------------------------------------
# Real-broker coordinates (integration suite)
# ---------------------------------------------------------------------------

RABBIT_HOST = os.environ.get("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.environ.get("RABBIT_PORT", "5672"))
AMQP_URL = f"amqp://guest:guest@{RABBIT_HOST}:{RABBIT_PORT}/"

# URL handed to the fakes — never dialed.
FAKE_URL = "amqp://guest:guest@nowhere/"


def broker_up() -> bool:
    """True when a real broker is reachable at RABBIT_HOST:RABBIT_PORT."""
    try:
        with socket.create_connection((RABBIT_HOST, RABBIT_PORT), timeout=0.5):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Fake aio_pika stack
# ---------------------------------------------------------------------------


class FakeUnderlay:
    """Stands in for the aiormq channel whose .consumers the watchdog polls."""

    def __init__(self) -> None:
        self.consumers: dict[str, object] = {}


class FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[aio_pika.Message, str]] = []

    async def publish(self, message: aio_pika.Message, routing_key: str) -> None:
        self.published.append((message, routing_key))


class FakeQueue:
    def __init__(self, name: str, channel: "FakeChannel") -> None:
        self.name = name
        self._channel = channel
        self.callback = None
        self.consume_tags: list[str] = []
        self.qos_at_consume: list[int | None] = []  # channel qos when consume() ran
        self.cancelled: list[str] = []
        self.cancel_error: Exception | None = None
        self._consumers: dict[str, object] = {}  # mirrors RobustQueue bookkeeping
        self._consume_seen = asyncio.Event()

    async def consume(self, callback) -> str:
        self.callback = callback
        tag = f"ctag-{self.name}-{len(self.consume_tags)}"
        self.consume_tags.append(tag)
        self.qos_at_consume.append(self._channel.qos)
        self._channel.underlay.consumers[tag] = callback
        self._consumers[tag] = callback
        self._consume_seen.set()
        return tag

    async def wait_for_consumes(self, count: int) -> None:
        """Block until consume() has been called `count` times in total
        (absolute count). Event-driven, like FakeChannel.wait_for_polls."""
        async with asyncio.timeout(5):
            while len(self.consume_tags) < count:
                self._consume_seen.clear()
                await self._consume_seen.wait()

    async def cancel(self, tag: str) -> None:
        if self.cancel_error is not None:
            raise self.cancel_error
        self.cancelled.append(tag)
        self._channel.underlay.consumers.pop(tag, None)
        self._consumers.pop(tag, None)


class FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = FakeExchange()
        self.declare_calls: list[tuple[str, bool]] = []
        self.declare_error: Exception | None = None  # raise on next declare_queue
        self.deleted: list[str] = []
        self.qos: int | None = None
        self.qos_calls: list[int] = []  # every set_qos prefetch_count, in order
        self.queues: dict[str, FakeQueue] = {}
        self.underlay = FakeUnderlay()
        self.underlay_none = False  # simulate "channel resetting"
        self.underlay_polls = 0  # how many times the watchdog looked
        self._poll_seen = asyncio.Event()

    async def declare_queue(self, name: str, durable: bool = False) -> FakeQueue:
        if self.declare_error is not None:
            raise self.declare_error
        self.declare_calls.append((name, durable))
        q = self.queues.get(name)
        if q is None:
            q = FakeQueue(name, self)
            self.queues[name] = q
        return q

    async def queue_delete(self, name: str) -> None:
        self.deleted.append(name)

    async def set_qos(self, prefetch_count: int) -> None:
        self.qos = prefetch_count
        self.qos_calls.append(prefetch_count)

    async def get_underlay_channel(self) -> FakeUnderlay:
        self.underlay_polls += 1
        self._poll_seen.set()
        if self.underlay_none:
            raise RuntimeError("channel is resetting")
        return self.underlay

    async def wait_for_polls(self, count: int) -> None:
        """Block until the watchdog has looked at the underlay `count` times
        (absolute count — snapshot `underlay_polls` first, then wait for +N).

        Event-driven, so tests gate on the watchdog actually polling instead
        of racing it on wall-clock sleeps: immune to event-loop stalls. Each
        poll's hit/miss decision is made synchronously right after
        get_underlay_channel() returns, so once this returns, poll `count`
        has fully completed.
        """
        async with asyncio.timeout(5):
            while self.underlay_polls < count:
                self._poll_seen.clear()
                await self._poll_seen.wait()


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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class Connected(NamedTuple):
    """Everything connected_client() wires up: the client and both fake
    connections, with the (single) channel on each side exposed as a property."""

    client: RabbitClient
    pub_conn: FakeConnection
    con_conn: FakeConnection

    @property
    def pub_channel(self) -> FakeChannel:
        return self.pub_conn.channels[0]

    @property
    def con_channel(self) -> FakeChannel:
        return self.con_conn.channels[0]


async def connected_client(monkeypatch: pytest.MonkeyPatch, **kwargs) -> Connected:
    """A RabbitClient wired to fakes via aio_pika.connect_robust."""
    conns: list[FakeConnection] = []

    async def fake_connect(url: str) -> FakeConnection:
        conn = FakeConnection()
        conns.append(conn)
        return conn

    monkeypatch.setattr(aio_pika, "connect_robust", fake_connect)
    client = RabbitClient(FAKE_URL, **kwargs)
    await client.connect()
    pub_conn, con_conn = conns
    return Connected(client, pub_conn, con_conn)


async def start_consumer(
    client: RabbitClient, con: FakeConnection, queue: str, handler=None, prefetch: int | None = None
):
    """Start a consumer via the handle API.

    consume() establishes the consumer before returning, so no polling is
    needed. Returns (consumer_handle, fake_queue, fake_channel).
    """
    channel = con.channels[0]
    if handler is None:

        async def handler(body: bytes) -> None:  # pragma: no cover - no traffic
            pass

    consumer = await client.consume(queue, handler, prefetch=prefetch)
    return consumer, channel.queues[queue], channel
