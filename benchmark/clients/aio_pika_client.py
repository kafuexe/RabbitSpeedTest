"""Asynchronous aio-pika client with natural pipelining for bulk publish."""
from __future__ import annotations

import asyncio

import aio_pika

from benchmark.clients.base import CONSUME_INACTIVITY_TIMEOUT, BenchmarkClient

_PIPELINE_BATCH = 500


class AioPikaClient(BenchmarkClient):
    name = "aio-pika"

    def __init__(
        self, amqp_url: str, *, prefetch: int = 100,
        publisher_confirms: bool = True, durable: bool = False,
        pipeline_batch: int = _PIPELINE_BATCH,
    ) -> None:
        self._url = amqp_url
        self._clone_kwargs = dict(
            prefetch=prefetch, publisher_confirms=publisher_confirms,
            durable=durable, pipeline_batch=pipeline_batch)
        self._prefetch = prefetch
        self._confirms = publisher_confirms
        self._durable = durable
        self._pipeline_batch = pipeline_batch
        self._conn: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        # Publisher confirms are a channel property, so confirm=False bulk
        # publishes go through this second, confirm-free channel.
        self._plain_channel: aio_pika.abc.AbstractChannel | None = None
        # Cache declared queue handles so consume paths don't pay a redundant
        # queue.declare RPC per call — keeps measurements symmetric with PikaClient.
        self._queues: dict[str, aio_pika.abc.AbstractQueue] = {}

    async def connect(self) -> None:
        self._conn = await aio_pika.connect_robust(self._url)
        self._channel = await self._conn.channel(publisher_confirms=self._confirms)
        await self._channel.set_qos(prefetch_count=self._prefetch)
        self._plain_channel = None
        self._queues.clear()

    async def _queue(self, name: str) -> aio_pika.abc.AbstractQueue:
        """Declare the queue once, then reuse the cached handle."""
        q = self._queues.get(name)
        if q is None:
            q = await self._channel.declare_queue(name, durable=self._durable)
            self._queues[name] = q
        return q

    async def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed:
            await self._conn.close()

    def _message(self, body: bytes) -> aio_pika.Message:
        mode = (aio_pika.DeliveryMode.PERSISTENT if self._durable
                else aio_pika.DeliveryMode.NOT_PERSISTENT)
        return aio_pika.Message(body=body, delivery_mode=mode)

    async def declare_queue(self, name: str) -> None:
        await self._queue(name)

    async def purge_queue(self, name: str) -> None:
        q = await self._queue(name)
        await q.purge()

    async def delete_queue(self, name: str) -> None:
        await self._channel.queue_delete(name)
        self._queues.pop(name, None)

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        ex = self._channel.default_exchange if exchange == "" else await self._channel.get_exchange(exchange)
        await ex.publish(self._message(body), routing_key=routing_key)

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        q = await self._queue(queue)
        msg = await q.get(no_ack=True, fail=False)
        return msg.body if msg is not None else None

    async def _bulk_channel(self, confirm: bool) -> aio_pika.abc.AbstractChannel:
        if confirm or not self._confirms:
            return self._channel
        if self._plain_channel is None or self._plain_channel.is_closed:
            self._plain_channel = await self._conn.channel(publisher_confirms=False)
        return self._plain_channel

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        ch = await self._bulk_channel(confirm)
        ex = ch.default_exchange if exchange == "" else await ch.get_exchange(exchange)
        step = self._pipeline_batch
        for start in range(0, len(bodies), step):
            batch = bodies[start:start + step]
            await asyncio.gather(*(ex.publish(self._message(b), routing_key=routing_key) for b in batch))

    async def consume_many(self, queue: str, count: int) -> int:
        """Push-based drain via basic.consume; the broker streams deliveries
        instead of paying one basic.get round-trip per message.

        Manual acks, not no_ack: the broker ignores prefetch for no_ack
        consumers, so with several workers it floods the first one and the
        surplus buffered in its iterator is dropped (already acked) on close.
        With acks, prefetch gives fair dispatch and anything buffered but
        unacked is requeued when the iterator closes.
        """
        if count <= 0:
            return 0
        q = await self._queue(queue)
        consumed = 0
        try:
            async with q.iterator(timeout=CONSUME_INACTIVITY_TIMEOUT) as it:
                async for msg in it:
                    await msg.ack()
                    consumed += 1
                    if consumed >= count:
                        break
        except asyncio.TimeoutError:
            pass  # queue ran dry -> short count; callers verify totals
        return consumed

    # consume_many_get: the base default (a consume_one loop over the cached
    # queue handle) is already the native aio-pika get loop; no override needed.

    async def queue_depth(self, name: str) -> int:
        # Raw RPC on the underlay channel: RobustChannel.declare_queue(passive=True)
        # returns a cached queue object with a stale declaration_result.
        underlay = await self._channel.get_underlay_channel()
        result = await underlay.queue_declare(name, passive=True)
        return result.message_count

    async def server_version(self) -> str | None:
        try:
            props = self._conn.transport.connection.server_properties  # best-effort
            v = props.get("version")
            return v.decode() if isinstance(v, bytes) else v
        except Exception:
            return None
