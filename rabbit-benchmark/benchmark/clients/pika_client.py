"""Synchronous pika client wrapped in a single-thread executor."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

import pika
from pika.adapters.blocking_connection import BlockingChannel

from benchmark.clients.base import CONSUME_INACTIVITY_TIMEOUT, BenchmarkClient

T = TypeVar("T")


class PikaClient(BenchmarkClient):
    name = "pika"

    def __init__(
        self, amqp_url: str, *, prefetch: int = 100,
        publisher_confirms: bool = True, durable: bool = False,
    ) -> None:
        self._url = amqp_url
        self._clone_kwargs = dict(
            prefetch=prefetch, publisher_confirms=publisher_confirms, durable=durable)
        self._prefetch = prefetch
        self._confirms = publisher_confirms
        self._durable = durable
        # One dedicated thread: a pika connection must be used from one thread.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pika")
        self._conn: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        # confirm_delivery() cannot be switched off again, so confirm=False
        # bulk publishes go through this second, confirm-free channel.
        self._plain_channel: BlockingChannel | None = None

    async def _run(self, fn: Callable[..., T], *args: Any) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args))

    # ---- lifecycle ----
    def _connect_sync(self) -> None:
        # pika sets TCP_NODELAY on every socket itself (connection_workflow);
        # params.tcp_options does not accept a TCP_NODELAY key.
        self._conn = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._conn.channel()
        self._channel.basic_qos(prefetch_count=self._prefetch)
        if self._confirms:
            self._channel.confirm_delivery()
        self._plain_channel = None

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
        # Always durable: RabbitMQ 4 denies transient non-exclusive queues.
        await self._run(lambda: self._channel.queue_declare(queue=name, durable=True))

    async def purge_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_purge(queue=name))

    async def delete_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_delete(queue=name))

    # ---- publish / consume ----
    def _properties(self) -> pika.BasicProperties | None:
        return pika.BasicProperties(delivery_mode=2) if self._durable else None

    def _publish_sync(self, exchange: str, routing_key: str, body: bytes, confirm: bool) -> None:
        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                                    properties=self._properties())

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        await self._run(self._publish_sync, exchange, routing_key, body, confirm)

    def _consume_one_sync(self, queue: str) -> bytes | None:
        method, _props, body = self._channel.basic_get(queue=queue, auto_ack=True)
        return body if method is not None else None

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        return await self._run(self._consume_one_sync, queue)

    def _bulk_channel(self, confirm: bool) -> BlockingChannel:
        if confirm or not self._confirms:
            return self._channel
        if self._plain_channel is None or not self._plain_channel.is_open:
            self._plain_channel = self._conn.channel()
        return self._plain_channel

    def _publish_many_sync(self, exchange: str, routing_key: str, bodies: list[bytes], confirm: bool) -> None:
        ch = self._bulk_channel(confirm)
        props = self._properties()
        for body in bodies:
            ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                             properties=props)

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        await self._run(self._publish_many_sync, exchange, routing_key, bodies, confirm)

    def _consume_many_sync(self, queue: str, count: int) -> int:
        """Push-based drain via basic.consume; the broker streams deliveries
        instead of paying one basic.get round-trip per message.

        Manual acks, not auto_ack: the broker ignores prefetch for auto-ack
        consumers, so with several workers it floods the first one and the
        surplus buffered in the generator is dropped (already acked) on
        cancel(). With acks, prefetch gives fair dispatch and cancel()
        requeues anything buffered but unacked.
        """
        if count <= 0:
            return 0
        consumed = 0
        for method, _props, _body in self._channel.consume(
                queue, auto_ack=False, inactivity_timeout=CONSUME_INACTIVITY_TIMEOUT):
            if method is None:
                break  # queue ran dry -> short count; callers verify totals
            self._channel.basic_ack(method.delivery_tag)
            consumed += 1
            if consumed >= count:
                break
        self._channel.cancel()
        return consumed

    async def consume_many(self, queue: str, count: int) -> int:
        return await self._run(self._consume_many_sync, queue, count)

    def _consume_many_get_sync(self, queue: str, count: int) -> int:
        consumed = 0
        while consumed < count:
            method, _props, _body = self._channel.basic_get(queue=queue, auto_ack=True)
            if method is None:
                break
            consumed += 1
        return consumed

    async def consume_many_get(self, queue: str, count: int) -> int:
        return await self._run(self._consume_many_get_sync, queue, count)

    async def queue_depth(self, name: str) -> int:
        def _depth() -> int:
            res = self._channel.queue_declare(queue=name, passive=True)
            return res.method.message_count
        return await self._run(_depth)

    async def server_version(self) -> str | None:
        def _ver() -> str | None:
            try:
                props = self._conn._impl._connection.server_properties  # best-effort
                v = props.get("version")
                return v.decode() if isinstance(v, bytes) else v
            except Exception:
                return None
        return await self._run(_ver)
