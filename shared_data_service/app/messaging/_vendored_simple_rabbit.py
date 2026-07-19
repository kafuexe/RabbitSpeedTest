"""Minimal RabbitMQ client for apps: aio-pika only, zero hand-rolled AMQP logic.

The maintenance-free counterpart to the benchmark suite's HybridClient.
Everything subtle is delegated to aio-pika, which is maintained for you:

- Reconnect: ``connect_robust`` re-establishes connections, channels, queues
  and consumers after a broker restart or network blip.
- Delivery safety: each message is acked only AFTER your handler returns; if
  the handler raises, that one message is requeued. Per-message acks are
  inherently safe under concurrency — no batch ack can ever cover an
  unfinished handler.
- Concurrency: deliveries run as concurrent tasks up to ``prefetch``, so a
  handler awaiting a database overlaps with up to ``prefetch`` others. For a
  DB-bound consumer this, not client speed, decides real throughput.

Built for many queues:

- Publishing and consuming use SEPARATE connections, so broker flow control
  on a busy publisher can never stall your consumers.
- Queue declares are cached (once per queue per side); aio-pika re-declares
  them automatically after a reconnect, so the cache stays valid.
- ``consume()`` can be called once per queue on one client — consumers are
  cheap, multiplexed on the consume connection, no extra threads. Prefetch
  applies per consumer: with many busy queues, size it accordingly
  (e.g. prefetch=50).

Measured on this repo's benchmark setup (1KB messages, local broker):
publish ~9k msg/s per connection (pipelined confirms), consume ceiling
~17.5k msg/s per process. If you outgrow that, run more consumer processes —
or see benchmark/clients/hybrid_client.py for the ~2x-faster,
higher-maintenance frontier consumer.

Usage:
    client = SimpleRabbit("amqp://user:pass@host/")
    await client.connect()
    await client.publish_many("jobs", [b"payload"] * 1000)

    async def handler(body: bytes) -> None:
        await db.insert(body)          # your async work; raise to requeue

    await client.consume("jobs", handler)   # runs until the task is cancelled
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import aio_pika

_PIPELINE = 1000  # confirm-pipeline depth; measured knee for bulk publishing


class ConsumerCancelledError(RuntimeError):
    """The broker cancelled our consumer (e.g. the queue was deleted).

    aio-pika handles a broker-sent Basic.Cancel by silently dropping the
    consumer — nothing is raised into the consume task, and consumers are
    only re-established on RECONNECT. consume() watches for this and raises
    so callers can retry (re-declare + re-consume) instead of parking dead.
    """


class SimpleRabbit:
    def __init__(self, amqp_url: str, *, prefetch: int = 200, durable: bool = False,
                 cancel_check_interval: float = 5.0) -> None:
        self._url = amqp_url
        self._prefetch = prefetch
        self._durable = durable
        self._cancel_check_interval = cancel_check_interval
        self._pub_conn: aio_pika.abc.AbstractRobustConnection | None = None
        self._pub_channel: aio_pika.abc.AbstractChannel | None = None
        self._con_conn: aio_pika.abc.AbstractRobustConnection | None = None
        self._con_channel: aio_pika.abc.AbstractChannel | None = None
        self._declared_pub: set[str] = set()
        self._con_queues: dict[str, aio_pika.abc.AbstractQueue] = {}

    async def connect(self) -> None:
        results = await asyncio.gather(
            aio_pika.connect_robust(self._url), aio_pika.connect_robust(self._url),
            return_exceptions=True)
        failures = [r for r in results if isinstance(r, BaseException)]
        if failures:
            # One side can succeed while the other fails (connection limit,
            # broker mid-restart). Close the survivor or it leaks unmanaged,
            # reconnect machinery and all, on every connect retry.
            for r in results:
                if not isinstance(r, BaseException):
                    try:
                        await r.close()
                    except Exception:
                        pass  # never mask the real connect failure below
            raise failures[0]
        self._pub_conn, self._con_conn = results
        self._pub_channel = await self._pub_conn.channel(publisher_confirms=True)
        self._con_channel = await self._con_conn.channel()
        await self._con_channel.set_qos(prefetch_count=self._prefetch)
        self._declared_pub.clear()
        self._con_queues.clear()

    async def close(self) -> None:
        for conn in (self._pub_conn, self._con_conn):
            if conn is not None and not conn.is_closed:
                await conn.close()

    @property
    def is_connected(self) -> bool:
        """True only when both connections are live RIGHT NOW.

        ``is_closed`` alone is a trap: a robust connection in its reconnect
        loop after a broker outage is not closed, but not usable either —
        the ``connected`` event is what clears during the outage.
        """
        return all(
            conn is not None and not conn.is_closed and conn.connected.is_set()
            for conn in (self._pub_conn, self._con_conn)
        )

    async def delete_queue(self, queue: str) -> None:
        await self._pub_channel.queue_delete(queue)
        self._declared_pub.discard(queue)
        self._con_queues.pop(queue, None)

    # Queues are always durable: RabbitMQ 4 denies transient non-exclusive
    # queues. The `durable` flag governs message persistence instead.
    async def _declare_for_publish(self, queue: str) -> None:
        if queue not in self._declared_pub:
            await self._pub_channel.declare_queue(queue, durable=True)
            self._declared_pub.add(queue)

    async def _queue(self, name: str) -> aio_pika.abc.AbstractQueue:
        q = self._con_queues.get(name)
        if q is None:
            q = await self._con_channel.declare_queue(name, durable=True)
            self._con_queues[name] = q
        return q

    def _message(self, body: bytes) -> aio_pika.Message:
        mode = (aio_pika.DeliveryMode.PERSISTENT if self._durable
                else aio_pika.DeliveryMode.NOT_PERSISTENT)
        return aio_pika.Message(body=body, delivery_mode=mode)

    async def publish(self, queue: str, body: bytes) -> None:
        await self._declare_for_publish(queue)
        await self._pub_channel.default_exchange.publish(self._message(body), routing_key=queue)

    async def publish_many(self, queue: str, bodies: list[bytes]) -> None:
        await self._declare_for_publish(queue)
        ex = self._pub_channel.default_exchange
        for i in range(0, len(bodies), _PIPELINE):
            await asyncio.gather(*(ex.publish(self._message(b), routing_key=queue)
                                   for b in bodies[i:i + _PIPELINE]))

    async def consume(self, queue: str, handler: Callable[[bytes], Awaitable[None]]) -> None:
        """Run until the surrounding task is cancelled."""
        q = await self._queue(queue)

        async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            try:
                await handler(message.body)
            except Exception:
                await message.channel.basic_nack(message.delivery_tag, requeue=True)
                return
            # wait=False skips awaiting the socket drain per ack (+10% measured).
            # Still one ack per message AFTER the handler, so no ack can ever
            # cover an unfinished handler; a crash may redeliver the last few
            # acked-but-unflushed messages (at-least-once, as before).
            await message.channel.basic_ack(message.delivery_tag, wait=False)

        tag = await q.consume(on_message)
        try:
            # Watchdog, not a bare sleep: a broker-side Basic.Cancel (queue
            # deleted) silently removes the consumer — no exception reaches
            # this task and aio-pika only restores consumers on reconnect.
            # Poll our tag so a silent cancel becomes a raise the caller can
            # retry on. A genuine cancel pops the tag from the SAME aiormq
            # channel object; a reconnect swaps in a NEW object whose
            # consumers the robust machinery is still restoring — tracking
            # the object identity tells the two apart, so a slow restore is
            # never mistaken for a cancel.
            underlay = await self._underlay_channel()
            misses = 0
            while True:
                await asyncio.sleep(self._cancel_check_interval)
                conn = self._con_conn
                if conn is None or conn.is_closed or not conn.connected.is_set():
                    misses = 0  # reconnecting; robust restore will re-consume
                    continue
                current = await self._underlay_channel()
                if current is None:
                    misses = 0  # channel resetting — restore in progress
                    continue
                if current is not underlay:
                    underlay = current  # fresh channel: adopt, let restore finish
                    misses = 0
                    continue
                if tag in current.consumers:
                    misses = 0
                    continue
                misses += 1
                if misses >= 2:
                    self._con_queues.pop(queue, None)  # force re-declare on retry
                    raise ConsumerCancelledError(
                        f"consumer for queue {queue!r} was cancelled by the broker")
        finally:
            try:
                await q.cancel(tag)
            except Exception:
                # Cancel RPC failed (broken channel). RobustQueue pops its
                # bookkeeping only AFTER a successful RPC, so purge it here —
                # otherwise the robust machinery resurrects this consumer on
                # the next reconnect alongside the retry's new one, and
                # duplicate consumers accumulate.
                getattr(q, "_consumers", {}).pop(tag, None)
                self._con_queues.pop(queue, None)

    async def _underlay_channel(self):
        """The live aiormq channel under the consume channel, or None while
        the channel is initializing/resetting (reconnect in progress)."""
        try:
            # aio-pika >= 9.4 accessor (async), falling back to the
            # deprecated .channel property on older versions.
            getter = getattr(self._con_channel, "get_underlay_channel", None)
            if getter is not None:
                return await getter()
            return self._con_channel.channel
        except Exception:
            return None
