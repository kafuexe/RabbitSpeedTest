"""Synchronous pika client wrapped in a single-thread executor."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

import pika
from pika.adapters.blocking_connection import BlockingChannel

from benchmark.clients.base import BenchmarkClient

T = TypeVar("T")


class PikaClient(BenchmarkClient):
    name = "pika"

    def __init__(self, amqp_url: str, *, prefetch: int = 100, management_url: str | None = None) -> None:
        self._url = amqp_url
        self._prefetch = prefetch
        self._management_url = management_url
        # One dedicated thread: a pika connection must be used from one thread.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pika")
        self._conn: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    async def _run(self, fn: Callable[..., T], *args: Any) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args))

    # ---- lifecycle ----
    def _connect_sync(self) -> None:
        self._conn = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._conn.channel()
        self._channel.basic_qos(prefetch_count=self._prefetch)

    async def connect(self) -> None:
        await self._run(self._connect_sync)

    def _close_sync(self) -> None:
        if self._conn and self._conn.is_open:
            self._conn.close()

    async def close(self) -> None:
        await self._run(self._close_sync)
        self._executor.shutdown(wait=True)

    # ---- queue admin ----
    async def declare_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_declare(queue=name, durable=False))

    async def purge_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_purge(queue=name))

    async def delete_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_delete(queue=name))

    # ---- publish / consume ----
    def _publish_sync(self, exchange: str, routing_key: str, body: bytes, confirm: bool) -> None:
        if confirm and not getattr(self._channel, "_delivery_confirmation", False):
            self._channel.confirm_delivery()
        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        await self._run(self._publish_sync, exchange, routing_key, body, confirm)

    def _consume_one_sync(self, queue: str) -> bytes | None:
        method, _props, body = self._channel.basic_get(queue=queue, auto_ack=True)
        return body if method is not None else None

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        return await self._run(self._consume_one_sync, queue)

    def _publish_many_sync(self, exchange: str, routing_key: str, bodies: list[bytes], confirm: bool) -> None:
        if confirm and not getattr(self._channel, "_delivery_confirmation", False):
            self._channel.confirm_delivery()
        for body in bodies:
            self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        await self._run(self._publish_many_sync, exchange, routing_key, bodies, confirm)

    def _consume_many_sync(self, queue: str, count: int) -> int:
        consumed = 0
        while consumed < count:
            method, _props, body = self._channel.basic_get(queue=queue, auto_ack=True)
            if method is None:
                break
            consumed += 1
        return consumed

    async def consume_many(self, queue: str, count: int) -> int:
        return await self._run(self._consume_many_sync, queue, count)

    async def server_version(self) -> str | None:
        def _ver() -> str | None:
            try:
                props = self._conn._impl._connection.server_properties  # best-effort
                v = props.get("version")
                return v.decode() if isinstance(v, bytes) else v
            except Exception:
                return None
        return await self._run(_ver)
