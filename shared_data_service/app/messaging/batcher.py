"""Greedy micro-batcher for consumed events.

SimpleClient delivers each message as its own task; committing one PostgreSQL
transaction per message caps throughput at the database's commit (fsync)
rate. The batcher groups concurrent deliveries into one business call — one
transaction per batch — while keeping delivery semantics intact:

- GREEDY, never waits: a flush takes only the items already queued. At low
  traffic that is a batch of one — zero added latency. Under load, new
  deliveries queue up while the previous batch commits, so batches (and
  throughput) grow exactly when there is a backlog.
- submit() returns only after the batch containing the item has COMMITTED,
  so SimpleClient still acks each message strictly after its data is safe
  (at-least-once, exactly as before).
- If a batch fails, items are retried individually, so a poison item fails
  alone (its message requeues) and never blocks the others.
- Shutdown never hangs or leaks: submit() on a closed batcher, items still
  queued at close, and the batch in flight when close() lands all fail with
  BatcherClosedError — a plain Exception, so SimpleClient's handler nacks
  and the broker redelivers. Nothing is silently dropped, nothing awaits a
  future that will never resolve, and a late submit cannot resurrect the
  runner.

CONTRACT for apply_batch: it must be ALL-OR-NOTHING (a failed call left no
partial effects) and IDEMPOTENT (safe to re-run), because a failed batch is
re-applied item by item and failed messages are redelivered. A single
transaction plus an inbox/version guard — as in UserService.apply_user_events
— satisfies both.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Generic, Sequence, TypeVar

from app.logging.correlation import set_correlation_id

logger = logging.getLogger(__name__)

T = TypeVar("T")
ApplyBatch = Callable[[Sequence[T]], Awaitable[None]]


class BatcherClosedError(RuntimeError):
    """The batcher is shutting down; the message will be redelivered."""


class Batcher(Generic[T]):
    def __init__(self, apply_batch: ApplyBatch[T], *, max_batch: int = 100) -> None:
        self._apply = apply_batch
        self._max_batch = max_batch
        self._queue: asyncio.Queue[tuple[T, asyncio.Future[None]]] = asyncio.Queue()
        self._runner: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def submit(self, item: T) -> None:
        """Enqueue and wait until the item's batch is committed."""
        if self._closed:
            raise BatcherClosedError("batcher is closed")
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(self._run())
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._queue.put_nowait((item, future))
        await future

    async def close(self) -> None:
        self._closed = True
        if self._runner is not None and not self._runner.done():
            self._runner.cancel()
            try:
                await self._runner
            except asyncio.CancelledError:
                pass
        # Drain HERE too, not only in _run's finally: cancelling a task whose
        # coroutine never got its first step skips the coroutine body entirely
        # — including try/finally — and would leave queued futures pending
        # forever (handlers hung, messages neither acked nor nacked).
        self._drain_queue()

    def _drain_queue(self) -> None:
        """Fail anything still queued with a nackable error so its message
        requeues while the channel is still open."""
        while not self._queue.empty():
            _, future = self._queue.get_nowait()
            if not future.done():
                future.set_exception(
                    BatcherClosedError("batcher closed before the item was applied")
                )

    async def _run(self) -> None:
        try:
            while True:
                batch = [await self._queue.get()]
                # Greedy drain: take what is already there, wait for nothing.
                while len(batch) < self._max_batch and not self._queue.empty():
                    batch.append(self._queue.get_nowait())
                await self._flush(batch)
        finally:
            self._drain_queue()

    async def _flush(self, batch: list[tuple[T, asyncio.Future[None]]]) -> None:
        # Batches merge many delivery contexts; give each flush its own
        # correlation id instead of silently inheriting whichever message's
        # context created the runner task.
        set_correlation_id()
        try:
            await self._apply([item for item, _ in batch])
        except Exception as exc:
            if len(batch) == 1:
                _, future = batch[0]
                if not future.done():
                    future.set_exception(exc)
                return
            logger.warning(
                "batch apply failed; retrying items individually",
                extra={"batch_size": len(batch)}, exc_info=True,
            )
            await self._apply_individually(batch)
            return
        except BaseException:
            # Cancellation (close() mid-apply) or an injected SystemExit /
            # GeneratorExit: the interrupted apply's outcome is unknown; fail
            # every future so the handlers nack and the broker redelivers —
            # a future must NEVER be left pending, whatever unwinds us.
            for _, future in batch:
                if not future.done():
                    future.set_exception(
                        BatcherClosedError("batcher closed mid-batch")
                    )
            raise
        for _, future in batch:
            if not future.done():
                future.set_result(None)

    async def _apply_individually(
        self, batch: list[tuple[T, asyncio.Future[None]]]
    ) -> None:
        try:
            for item, future in batch:
                try:
                    await self._apply([item])
                except Exception as exc:
                    if not future.done():
                        future.set_exception(exc)
                else:
                    if not future.done():
                        future.set_result(None)
        except BaseException:
            # Cancellation (or worse) during the retry pass: fail whatever
            # wasn't reached — never leave a future pending.
            for _, future in batch:
                if not future.done():
                    future.set_exception(BatcherClosedError("batcher closed mid-retry"))
            raise
