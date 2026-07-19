"""Benchmark adapter for the app-facing RabbitClient client (the
``rabbit_client`` module from the ``rabbit-client-python`` library).

Measures RabbitClient's real paths — pipelined publish and the callback
consumer with per-message wait=False acks — through the suite's interface.
Admin/get operations (declare/purge/depth/basic_get) go through an
AioPikaClient; they are benchmark plumbing, not part of RabbitClient's API.

Quota drains reuse RabbitClient's own error path: once this worker hits its
count, the handler raises, RabbitClient nack-requeues that message, and a
peer worker picks it up — no over-consumption, no starved peers.

Note: RabbitClient always publishes with confirms; a --no-confirms run does
not change its publish numbers.
"""
from __future__ import annotations

import asyncio

from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import CONSUME_INACTIVITY_TIMEOUT, BenchmarkClient
from rabbit_client import RabbitClient


class _QuotaReached(Exception):
    pass


class RabbitClientBench(BenchmarkClient):
    name = "simple"

    def __init__(
        self, amqp_url: str, *, prefetch: int = 200,
        publisher_confirms: bool = True, durable: bool = False,
    ) -> None:
        self._url = amqp_url
        self._clone_kwargs = dict(
            prefetch=prefetch, publisher_confirms=publisher_confirms, durable=durable)
        self._confirms = publisher_confirms
        self._durable = durable
        self._sr = RabbitClient(amqp_url, prefetch=prefetch, durable=durable)
        self._admin = AioPikaClient(
            amqp_url, prefetch=prefetch,
            publisher_confirms=publisher_confirms, durable=durable)
        self._inactivity = CONSUME_INACTIVITY_TIMEOUT

    # ---- lifecycle ----
    async def connect(self) -> None:
        await asyncio.gather(self._sr.connect(), self._admin.connect())

    async def close(self) -> None:
        await self._sr.close()
        await self._admin.close()

    # ---- admin / get paths: benchmark plumbing via aio-pika ----
    async def declare_queue(self, name: str) -> None:
        await self._admin.declare_queue(name)

    async def purge_queue(self, name: str) -> None:
        await self._admin.purge_queue(name)

    async def delete_queue(self, name: str) -> None:
        await self._admin.delete_queue(name)

    async def queue_depth(self, name: str) -> int:
        return await self._admin.queue_depth(name)

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        return await self._admin.consume_one(queue, timeout)

    async def consume_many_get(self, queue: str, count: int) -> int:
        return await self._admin.consume_many_get(queue, count)

    async def server_version(self) -> str | None:
        return await self._admin.server_version()

    # ---- measured paths: RabbitClient ----
    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        await self._sr.publish(routing_key, body)

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        await self._sr.publish_many(routing_key, bodies)

    async def consume_many(self, queue: str, count: int) -> int:
        if count <= 0:
            return 0
        n = 0
        done = asyncio.Event()

        async def handler(body: bytes) -> None:
            nonlocal n
            if n >= count:
                raise _QuotaReached()  # RabbitClient nack-requeues it for peers
            n += 1
            if n >= count:
                done.set()

        task = asyncio.create_task(self._sr.consume(queue, handler))
        try:
            while not done.is_set():
                before = n
                try:
                    await asyncio.wait_for(done.wait(), timeout=self._inactivity)
                except asyncio.TimeoutError:
                    if n == before:
                        break  # queue ran dry -> short count; callers verify totals
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        return n
