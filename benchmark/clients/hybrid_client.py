"""Best-of-both async client: aio-pika publishes, raw aiormq consumes.

Measured on this suite (1KB, confirms on), aio-pika publishes ~3x faster than
pika because it pipelines publisher confirms instead of waiting one round-trip
per message. For consuming, pika's blocking thread loop is the raw record
(~40k msg/s) but it lives outside the event loop — useless when each message
triggers async work (awaiting a DB, an HTTP call, ...). The fastest *async*
consume is a raw aiormq callback consumer with batched acks: ~93% of pika,
with every message delivered inside the event loop.

Concurrency model (aiormq spawns one asyncio task per delivery, so callbacks
overlap at every await — the design must not assume serialized delivery):

- Quota slots are reserved synchronously at callback entry, before any await,
  so prefetched deliveries beyond ``count`` are never processed; they are
  nack-requeued for other workers on the way out.
- Acks follow a contiguous-completion frontier: a tag is acked (multiple=True)
  only when every delivery up to it has finished its handler, so a batch ack
  can never cover a message whose handler is still running. Crash mid-handler
  therefore always means redelivery, never loss.
- A single acker task performs every ack: batch boundaries wake it via an
  event and its wait timeout doubles as the idle flush — no per-message
  timers, no concurrent ack paths.
- A handler that raises gets its message individually nack-requeued and the
  frontier passes it, so one poison message cannot wedge the stream.

Single-get paths (consume_one / consume_many_get) ride the aio-pika
connection: raw aiormq basic_get returns a GetEmpty pseudo-message instead of
None for an empty queue, and sharing the publish connection restores AMQP
same-channel ordering for publish-then-get callers (round_trip).

Baked-in tuning, measured on this suite's grid (2026-07-11, 1KB messages):
consume throughput flattens past prefetch=1000/ack-batch=500 (~35k msg/s vs
~31k at prefetch=100); confirm-pipeline depth peaks around 1000 (~9.1k msg/s).
"""
from __future__ import annotations

import asyncio

import aiormq

from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import CONSUME_INACTIVITY_TIMEOUT, BenchmarkClient

_TUNED_PREFETCH = 1000
_TUNED_PIPELINE = 1000


class HybridClient(BenchmarkClient):
    name = "hybrid"

    def __init__(
        self, amqp_url: str, *, prefetch: int = _TUNED_PREFETCH,
        publisher_confirms: bool = True, durable: bool = False,
        pipeline_batch: int = _TUNED_PIPELINE,
    ) -> None:
        self._url = amqp_url
        self._clone_kwargs = dict(
            prefetch=prefetch, publisher_confirms=publisher_confirms,
            durable=durable, pipeline_batch=pipeline_batch)
        self._prefetch = prefetch
        self._confirms = publisher_confirms
        self._durable = durable
        self._publisher = AioPikaClient(
            amqp_url, prefetch=prefetch, publisher_confirms=publisher_confirms,
            durable=durable, pipeline_batch=pipeline_batch)
        self._consume_conn: aiormq.abc.AbstractConnection | None = None
        self._consume_ch: aiormq.abc.AbstractChannel | None = None
        self._ack_batch = max(1, prefetch // 2)
        self._inactivity = CONSUME_INACTIVITY_TIMEOUT
        self._ack_flush_delay = 0.25

    # ---- lifecycle ----
    async def connect(self) -> None:
        # The two connections are independent; overlap their handshakes.
        _, self._consume_conn = await asyncio.gather(
            self._publisher.connect(), aiormq.connect(self._url))
        self._consume_ch = await self._consume_conn.channel()
        await self._consume_ch.basic_qos(prefetch_count=self._prefetch)

    async def close(self) -> None:
        if self._consume_conn is not None and not self._consume_conn.is_closed:
            await self._consume_conn.close()
        await self._publisher.close()

    # ---- queue admin ----
    async def declare_queue(self, name: str) -> None:
        await self._publisher.declare_queue(name)

    async def purge_queue(self, name: str) -> None:
        await self._publisher.purge_queue(name)

    async def delete_queue(self, name: str) -> None:
        await self._publisher.delete_queue(name)

    async def queue_depth(self, name: str) -> int:
        return await self._publisher.queue_depth(name)

    # ---- publish: aio-pika (pipelined confirms) ----
    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        await self._publisher.publish(exchange, routing_key, body, confirm=confirm)

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        await self._publisher.publish_many(exchange, routing_key, bodies, confirm=confirm)

    # ---- single gets: aio-pika (None on empty, publish-ordering preserved) ----
    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        return await self._publisher.consume_one(queue, timeout)

    async def consume_many_get(self, queue: str, count: int) -> int:
        return await self._publisher.consume_many_get(queue, count)

    # ---- push consume: raw aiormq callback consumer, frontier-batched acks ----
    async def consume_many(self, queue: str, count: int) -> int:
        return await self._consume(queue, count=count, handler=None)

    async def consume(self, queue, handler, *, count: int | None = None) -> int:
        """App-facing consumer: ``await handler(body)`` per message; a message
        is only ever acked after its handler (and every earlier one) finished.
        With ``count=None`` it runs until the task is cancelled. A handler
        exception nack-requeues that one message and the stream continues.
        Deliveries are handled concurrently up to the prefetch window.
        """
        return await self._consume(queue, count=count, handler=handler)

    async def _consume(self, queue: str, *, count: int | None, handler) -> int:
        if count is not None and count <= 0:
            return 0
        ch = self._consume_ch
        loop = asyncio.get_running_loop()
        done: asyncio.Future = loop.create_future()
        ack_event = asyncio.Event()
        reserved = 0     # quota slots taken, synchronously at cb entry
        settled = 0      # handlers finished, successfully or not
        completed = 0    # handlers finished successfully (the return value)
        first_tag = 0
        frontier = 0     # highest tag with every owned tag <= it settled
        ack_ref = 0      # highest completed (ackable) tag <= frontier
        acked = 0        # highest tag acked; written only by the acker/finally
        extra_tag = 0
        finished_ok: set[int] = set()
        finished_nack: set[int] = set()
        closing = False

        def advance_frontier() -> None:
            # Our deliveries carry consecutive tags starting at first_tag.
            nonlocal frontier, ack_ref
            nxt = first_tag if frontier == 0 else frontier + 1
            while nxt in finished_ok or nxt in finished_nack:
                if nxt in finished_ok:
                    finished_ok.discard(nxt)
                    ack_ref = nxt  # ack references must be completed, unacked tags
                else:
                    finished_nack.discard(nxt)
                frontier = nxt
                nxt += 1

        async def acker() -> None:
            # Sole ack sender: batch wake-ups via the event; the wait timeout
            # doubles as the idle flush for partially filled batches.
            nonlocal acked
            while True:
                try:
                    await asyncio.wait_for(ack_event.wait(), timeout=self._ack_flush_delay)
                except asyncio.TimeoutError:
                    pass
                ack_event.clear()
                if ack_ref > acked:
                    tag = ack_ref
                    acked = tag  # update before awaiting: monotonic, single writer
                    await ch.basic_ack(tag, multiple=True)

        def settle(tag: int, ok: bool) -> None:
            nonlocal settled, completed
            (finished_ok if ok else finished_nack).add(tag)
            advance_frontier()
            settled += 1
            if ok:
                completed += 1
            if ack_ref - acked >= self._ack_batch:
                ack_event.set()
            if count is not None and settled >= count and not done.done():
                done.set_result(None)

        async def cb(msg: aiormq.abc.DeliveredMessage) -> None:
            nonlocal reserved, first_tag, extra_tag
            tag = msg.delivery.delivery_tag
            if first_tag == 0:
                first_tag = tag
            # Reserve the quota slot before ANY await: deliveries arrive as
            # concurrent tasks, and a check after the handler await would let
            # every prefetched message through.
            if count is not None and reserved >= count:
                extra_tag = max(extra_tag, tag)
                return
            reserved += 1
            if handler is not None:
                try:
                    await handler(msg.body)
                except Exception:
                    if not closing:
                        # Requeue the poison message BEFORE the frontier may
                        # pass it, so no batch ack can settle it first.
                        await ch.basic_nack(tag, multiple=False, requeue=True)
                        settle(tag, ok=False)
                    return
            settle(tag, ok=True)

        acker_task = asyncio.create_task(acker())
        ok = await ch.basic_consume(queue, cb, no_ack=False)
        try:
            if count is None:
                await done  # runs until the surrounding task is cancelled
            else:
                while not done.done():
                    before = settled
                    try:
                        await asyncio.wait_for(asyncio.shield(done), timeout=self._inactivity)
                    except asyncio.TimeoutError:
                        if settled == before:
                            break  # queue ran dry -> short count; callers verify totals
        finally:
            closing = True
            acker_task.cancel()
            try:
                await acker_task
            except asyncio.CancelledError:
                pass
            await ch.basic_cancel(ok.consumer_tag)
            if ack_ref > acked:
                acked = ack_ref
                await ch.basic_ack(ack_ref, multiple=True)
            if extra_tag:
                # Everything still unacked up to here (quota extras plus any
                # cancelled-mid-handler deliveries) goes back to the queue.
                await ch.basic_nack(extra_tag, multiple=True, requeue=True)
        return completed

    async def server_version(self) -> str | None:
        return await self._publisher.server_version()
