"""Asynchronous aio-pika client with natural pipelining for bulk publish."""
from __future__ import annotations

import asyncio

import aio_pika

from benchmark.clients.base import BenchmarkClient

_PIPELINE_BATCH = 500


class AioPikaClient(BenchmarkClient):
    name = "aio-pika"

    def __init__(self, amqp_url: str, *, prefetch: int = 100, management_url: str | None = None) -> None:
        self._url = amqp_url
        self._prefetch = prefetch
        self._management_url = management_url
        self._conn: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        # Cache declared queue handles so consume paths don't pay a redundant
        # queue.declare RPC per call — keeps measurements symmetric with PikaClient.
        self._queues: dict[str, aio_pika.abc.AbstractQueue] = {}

    async def connect(self) -> None:
        self._conn = await aio_pika.connect_robust(self._url)
        self._channel = await self._conn.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=self._prefetch)
        self._queues.clear()

    async def _queue(self, name: str) -> aio_pika.abc.AbstractQueue:
        """Declare the queue once, then reuse the cached handle."""
        q = self._queues.get(name)
        if q is None:
            q = await self._channel.declare_queue(name, durable=True)
            self._queues[name] = q
        return q

    async def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed:
            await self._conn.close()

    async def declare_queue(self, name: str) -> None:
        await self._queue(name)

    async def purge_queue(self, name: str) -> None:
        q = await self._channel.declare_queue(name, durable=True)
        await q.purge()

    async def delete_queue(self, name: str) -> None:
        await self._channel.queue_delete(name)

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        msg = aio_pika.Message(body=body)
        ex = self._channel.default_exchange if exchange == "" else await self._channel.get_exchange(exchange)
        await ex.publish(msg, routing_key=routing_key)

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        q = await self._queue(queue)
        msg = await q.get(no_ack=True, fail=False)
        return msg.body if msg is not None else None

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        ex = self._channel.default_exchange if exchange == "" else await self._channel.get_exchange(exchange)
        for start in range(0, len(bodies), _PIPELINE_BATCH):
            batch = bodies[start:start + _PIPELINE_BATCH]
            await asyncio.gather(*(ex.publish(aio_pika.Message(body=b), routing_key=routing_key) for b in batch))

    async def consume_many(self, queue: str, count: int) -> int:
        q = await self._queue(queue)
        consumed = 0
        while consumed < count:
            msg = await q.get(no_ack=True, fail=False)
            if msg is None:
                break
            consumed += 1
        return consumed

    async def server_version(self) -> str | None:
        try:
            props = self._conn.transport.connection.server_properties  # best-effort
            v = props.get("version")
            return v.decode() if isinstance(v, bytes) else v
        except Exception:
            return None
