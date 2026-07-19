"""Minimal RabbitMQ client for apps: aio-pika only, zero hand-rolled AMQP logic.

The maintenance-free counterpart to the benchmark suite's HybridClient, and
the canonical sibling of the TypeScript client
(rabbit-client-typescript/src/rabbit-client.ts). Everything subtle is
delegated to aio-pika, which is maintained for you:

- Reconnect: ``connect_robust`` re-establishes connections, channels, queues
  and consumers after a broker restart or network blip.
- Consumer resurrection: a broker-sent Basic.Cancel (e.g. the queue was
  deleted) silently drops an aio-pika consumer — nothing is raised, and
  consumers are only restored on RECONNECT. A watchdog inside every consumer
  detects that, logs a WARNING, re-declares the queue and re-consumes, so a
  consumer genuinely runs until *you* cancel it (parity with the TypeScript
  client, where amqp-connection-manager does the same resurrection).
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
  (e.g. prefetch=50, or pass ``prefetch=`` per consume call).

Measured on the companion benchmark setup (1KB messages, local broker):
publish ~9k msg/s per connection (pipelined confirms), consume ceiling
~17.5k msg/s per process. If you outgrow that, run more consumer processes —
or see the HybridClient in the rabbit-benchmark project in this repo
(github.com/kafuexe/RabbitSpeedTest) for the ~2x-faster,
higher-maintenance frontier consumer.

Usage:
    client = RabbitClient("amqp://user:pass@host/")
    await client.connect()
    await client.publish_many("jobs", [b"payload"] * 1000)

    async def handler(body: bytes) -> None:
        await db.insert(body)          # your async work; raise to requeue

    consumer = await client.consume("jobs", handler)
    # ... later:
    await consumer.cancel()            # or: await consumer.wait() to park
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, cast

import aio_pika

__all__ = ["Consumer", "ConsumerCancelledError", "RabbitClient"]

_PIPELINE = 1000  # confirm-pipeline depth; measured knee for bulk publishing

# Pause between detecting a broker-side cancel and re-declaring/re-consuming.
# Long enough not to hot-loop against a broker that keeps deleting the queue,
# short enough that consumption resumes promptly.
_RECONSUME_BACKOFF = 1.0

_log = logging.getLogger("hs_rabbit_client")

MessageHandler = Callable[[bytes], Awaitable[None]]


class ConsumerCancelledError(RuntimeError):
    """The broker cancelled our consumer (e.g. the queue was deleted).

    aio-pika handles a broker-sent Basic.Cancel by silently dropping the
    consumer — nothing is raised into the consume task, and consumers are
    only re-established on RECONNECT. The internal watchdog detects this and
    raises it INTERNALLY; ``consume()`` then recovers by itself (re-declare +
    re-consume), so this error never surfaces through a :class:`Consumer`
    handle. It stays exported for anyone driving the internals directly.
    """


class Consumer:
    """Handle for one running consumer, returned by :meth:`RabbitClient.consume`.

    The consumer runs (surviving reconnects AND broker-side cancels) until
    :meth:`cancel` is called or the owning client is closed.
    """

    def __init__(self, queue: str, task: asyncio.Task[None]) -> None:
        self.queue = queue
        self._task = task
        self._cancel_requested = False

    async def cancel(self) -> None:
        """Stop consuming. Idempotent and concurrent-safe.

        The first caller triggers the cancellation; every caller (including
        concurrent ones) awaits the same underlying teardown, so when
        ``cancel()`` returns the consumer is fully stopped.
        """
        if not self._cancel_requested:
            self._cancel_requested = True
            self._task.cancel()
        await asyncio.wait([self._task])
        if not self._task.cancelled():
            # Retrieve a stored exception so asyncio never logs "exception
            # was never retrieved"; wait() re-raises it for callers who care.
            self._task.exception()

    async def wait(self) -> None:
        """Park until the consumer is cancelled (via :meth:`cancel` or
        :meth:`RabbitClient.close`), then return ``None``.

        An unexpected INTERNAL error (the recovery machinery itself failing,
        never a raising message handler) is re-raised here. Cancelling the
        task that awaits ``wait()`` propagates ``CancelledError`` normally
        and leaves the consumer running.
        """
        await asyncio.wait([self._task])
        if self._task.cancelled():
            return
        exc = self._task.exception()
        if exc is not None:
            raise exc


class RabbitClient:
    def __init__(
        self,
        amqp_url: str,
        *,
        prefetch: int = 200,
        durable: bool = False,
        cancel_check_interval: float = 5.0,
    ) -> None:
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
        self._consumer_tasks: set[asyncio.Task[None]] = set()

    def _pub(self) -> aio_pika.abc.AbstractChannel:
        if self._pub_channel is None:
            raise RuntimeError("hs-rabbit-client is not connected — call connect() first")
        return self._pub_channel

    def _con(self) -> aio_pika.abc.AbstractChannel:
        if self._con_channel is None:
            raise RuntimeError("hs-rabbit-client is not connected — call connect() first")
        return self._con_channel

    async def connect(self) -> None:
        results = await asyncio.gather(
            aio_pika.connect_robust(self._url),
            aio_pika.connect_robust(self._url),
            return_exceptions=True,
        )
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
        # (typing) failures were re-raised above, so both results are connections.
        self._pub_conn, self._con_conn = cast(
            "list[aio_pika.abc.AbstractRobustConnection]", results
        )
        self._pub_channel = await self._pub_conn.channel(publisher_confirms=True)
        self._con_channel = await self._con_conn.channel()
        await self._con_channel.set_qos(prefetch_count=self._prefetch)
        self._declared_pub.clear()
        self._con_queues.clear()

    async def close(self) -> None:
        """Close both connections.

        Outstanding :class:`Consumer` handles are cancelled first (their
        internal tasks are stopped while the connection is still usable), so
        a pending ``Consumer.wait()`` returns ``None`` — exactly as if
        ``cancel()`` had been called on each handle.
        """
        tasks = list(self._consumer_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks)
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
        await self._pub().queue_delete(queue)
        self._declared_pub.discard(queue)
        self._con_queues.pop(queue, None)

    # Queues are always durable: RabbitMQ 4 denies transient non-exclusive
    # queues. The `durable` flag governs message persistence instead.
    async def _declare_for_publish(self, queue: str) -> None:
        if queue not in self._declared_pub:
            await self._pub().declare_queue(queue, durable=True)
            self._declared_pub.add(queue)

    async def _queue(self, name: str) -> aio_pika.abc.AbstractQueue:
        q = self._con_queues.get(name)
        if q is None:
            q = await self._con().declare_queue(name, durable=True)
            self._con_queues[name] = q
        return q

    def _message(
        self,
        body: bytes,
        *,
        persistent: bool | None = None,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        content_type: str | None = None,
        expiration: float | None = None,
        priority: int | None = None,
    ) -> aio_pika.Message:
        durable = self._durable if persistent is None else persistent
        mode = aio_pika.DeliveryMode.PERSISTENT if durable else aio_pika.DeliveryMode.NOT_PERSISTENT
        # Properties map straight onto aio_pika.Message kwargs — no
        # hand-rolled AMQP logic here (expiration is in SECONDS; aio-pika
        # converts to the per-message TTL the broker expects).
        return aio_pika.Message(
            body=body,
            delivery_mode=mode,
            headers=headers,
            correlation_id=correlation_id,
            message_id=message_id,
            content_type=content_type,
            expiration=expiration,
            priority=priority,
        )

    async def publish(
        self,
        queue: str,
        body: bytes,
        *,
        persistent: bool | None = None,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        content_type: str | None = None,
        expiration: float | None = None,
        priority: int | None = None,
    ) -> None:
        await self._declare_for_publish(queue)
        message = self._message(
            body,
            persistent=persistent,
            headers=headers,
            correlation_id=correlation_id,
            message_id=message_id,
            content_type=content_type,
            expiration=expiration,
            priority=priority,
        )
        await self._pub().default_exchange.publish(message, routing_key=queue)

    async def publish_many(
        self,
        queue: str,
        bodies: list[bytes],
        *,
        persistent: bool | None = None,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        content_type: str | None = None,
        expiration: float | None = None,
        priority: int | None = None,
    ) -> None:
        await self._declare_for_publish(queue)
        ex = self._pub().default_exchange
        for i in range(0, len(bodies), _PIPELINE):
            await asyncio.gather(
                *(
                    ex.publish(
                        self._message(
                            b,
                            persistent=persistent,
                            headers=headers,
                            correlation_id=correlation_id,
                            message_id=message_id,
                            content_type=content_type,
                            expiration=expiration,
                            priority=priority,
                        ),
                        routing_key=queue,
                    )
                    for b in bodies[i : i + _PIPELINE]
                )
            )

    async def consume(
        self,
        queue: str,
        handler: MessageHandler,
        *,
        prefetch: int | None = None,
    ) -> Consumer:
        """Consume `queue`, calling ``await handler(body)`` per message.

        The consumer is fully established (queue declared + basic.consume
        issued) BEFORE this returns, so setup errors raise at the call site.
        The returned :class:`Consumer` runs until its ``cancel()`` — it
        survives reconnects (aio-pika robust machinery) and broker-side
        cancels (internal watchdog: WARNING log, short backoff, re-declare +
        re-consume).

        When ``prefetch`` is not None it overrides the constructor prefetch
        for THIS consumer: ``basic.qos`` (global=false) is issued on the
        consume channel immediately before ``basic.consume`` — RabbitMQ
        applies the channel's current qos per consumer at consume time — and
        re-issued on every internal re-consume.
        """
        on_message = self._make_on_message(handler)
        q, tag = await self._establish_consumer(queue, on_message, prefetch)
        started = asyncio.Event()
        task = asyncio.create_task(
            self._consume_forever(queue, on_message, prefetch, q, tag, started)
        )
        self._consumer_tasks.add(task)
        task.add_done_callback(self._consumer_tasks.discard)
        # Let the task take its first step before handing out the handle: a
        # task cancelled before it ever ran would skip its teardown, leaving
        # the just-issued basic.consume alive at the broker.
        await started.wait()
        return Consumer(queue, task)

    def _make_on_message(
        self, handler: MessageHandler
    ) -> Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]:
        async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            tag = message.delivery_tag
            assert tag is not None  # a delivered message always carries a tag
            try:
                await handler(message.body)
            except Exception:
                await message.channel.basic_nack(tag, requeue=True)
                return
            # wait=False skips awaiting the socket drain per ack (+10% measured).
            # Still one ack per message AFTER the handler, so no ack can ever
            # cover an unfinished handler; a crash may redeliver the last few
            # acked-but-unflushed messages (at-least-once, as before).
            await message.channel.basic_ack(tag, wait=False)

        return on_message

    async def _establish_consumer(
        self,
        queue: str,
        on_message: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]],
        prefetch: int | None,
    ) -> tuple[aio_pika.abc.AbstractQueue, str]:
        """Declare (cached) and basic.consume; the per-consume prefetch
        override is applied immediately before basic.consume so it binds to
        this consumer (RabbitMQ applies global=false qos at consume time)."""
        q = await self._queue(queue)
        if prefetch is not None:
            await self._con().set_qos(prefetch_count=prefetch)
        tag = await q.consume(on_message)
        return q, tag

    async def _consume_forever(
        self,
        queue: str,
        on_message: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]],
        prefetch: int | None,
        q: aio_pika.abc.AbstractQueue,
        tag: str,
        started: asyncio.Event,
    ) -> None:
        """Body of a Consumer handle's task: watchdog + broker-cancel
        auto-recovery, forever until the task is cancelled."""
        # From here on, entering _watch_consumer's try block is synchronous
        # (no suspension point in between), so once `started` is set a later
        # cancellation is guaranteed to run the broker-side consumer cancel.
        started.set()
        while True:
            try:
                await self._watch_consumer(queue, q, tag)
            except ConsumerCancelledError:
                # Parity with amqp-connection-manager: a broker-side cancel is
                # survived, not surfaced — re-declare and resume after a short
                # backoff (the watchdog already purged the declare cache).
                _log.warning(
                    "consumer cancelled by broker; re-declaring and resuming",
                    extra={"queue": queue},
                )
                await asyncio.sleep(_RECONSUME_BACKOFF)
                q, tag = await self._establish_consumer(queue, on_message, prefetch)

    async def _watch_consumer(self, queue: str, q: aio_pika.abc.AbstractQueue, tag: str) -> None:
        """Never returns normally: raises ConsumerCancelledError on a silent
        broker-side cancel, or propagates CancelledError when the consumer
        handle is cancelled (cancelling the consumer at the broker on the
        way out)."""
        try:
            # Watchdog, not a bare sleep: a broker-side Basic.Cancel (queue
            # deleted) silently removes the consumer — no exception reaches
            # this task and aio-pika only restores consumers on reconnect.
            # Poll our tag so a silent cancel becomes a raise the recovery
            # loop can retry on. A genuine cancel pops the tag from the SAME
            # aiormq channel object; a reconnect swaps in a NEW object whose
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
                        f"consumer for queue {queue!r} was cancelled by the broker"
                    )
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

    async def _underlay_channel(self) -> Any:
        """The live aiormq channel under the consume channel, or None while
        the channel is initializing/resetting (reconnect in progress)."""
        try:
            # aio-pika >= 9.4 accessor (async), falling back to the
            # deprecated .channel property on older versions (typed Any:
            # the property is no longer declared on the newer ABC).
            channel: Any = self._con()
            getter = getattr(channel, "get_underlay_channel", None)
            if getter is not None:
                return await getter()
            return channel.channel
        except Exception:
            return None
