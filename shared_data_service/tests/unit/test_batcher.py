"""Reliability contract of the greedy micro-batcher:

- submit() resolves only after its batch was applied (ack-after-commit)
- concurrent submits coalesce into one apply call (throughput)
- a lone item flushes immediately — no artificial wait (latency)
- a failing batch retries per item; only the poison item's submit fails
  (its message requeues, the others are acked)
- close() cancels pending submits (their messages requeue on shutdown)
"""
import asyncio

import pytest

from app.messaging.batcher import Batcher, BatcherClosedError


class Recorder:
    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.batches: list[list[int]] = []
        self.fail_on = fail_on or set()

    async def apply(self, items):
        self.batches.append(list(items))
        if any(i in self.fail_on for i in items):
            raise ConnectionError("db down for poison item")


async def test_single_item_flushes_immediately_without_waiting():
    rec = Recorder()
    batcher = Batcher(rec.apply, max_batch=100)
    started = asyncio.get_running_loop().time()
    await batcher.submit(1)
    elapsed = asyncio.get_running_loop().time() - started
    assert rec.batches == [[1]]
    assert elapsed < 0.05  # greedy: no batch-fill delay
    await batcher.close()


async def test_concurrent_submits_coalesce_into_batches():
    applied = asyncio.Event()

    class SlowRecorder(Recorder):
        async def apply(self, items):
            await asyncio.sleep(0.02)  # simulate a commit in flight
            await super().apply(items)
            applied.set()

    rec = SlowRecorder()
    batcher = Batcher(rec.apply, max_batch=100)
    await asyncio.gather(*(batcher.submit(i) for i in range(50)))
    total = sorted(i for b in rec.batches for i in b)
    assert total == list(range(50))
    # far fewer applies than items: the queue built up during the first flush
    assert len(rec.batches) <= 3
    await batcher.close()


async def test_max_batch_is_respected():
    rec = Recorder()
    batcher = Batcher(rec.apply, max_batch=10)

    async def stall_then_submit(i):
        await batcher.submit(i)

    await asyncio.gather(*(stall_then_submit(i) for i in range(35)))
    assert all(len(b) <= 10 for b in rec.batches)
    await batcher.close()


async def test_submit_resolves_only_after_apply():
    order: list[str] = []

    async def apply(items):
        await asyncio.sleep(0.01)
        order.append("applied")

    batcher = Batcher(apply, max_batch=10)

    async def submit():
        await batcher.submit(1)
        order.append("submit returned")

    await submit()
    assert order == ["applied", "submit returned"]  # ack strictly after commit
    await batcher.close()


async def test_poison_item_fails_alone_others_succeed():
    rec = Recorder(fail_on={13})
    batcher = Batcher(rec.apply, max_batch=100)
    results = await asyncio.gather(
        *(batcher.submit(i) for i in range(20)), return_exceptions=True
    )
    failures = [r for r in results if isinstance(r, Exception)]
    assert len(failures) == 1 and isinstance(failures[0], ConnectionError)
    # every item was attempted, the poison one individually
    assert [13] in rec.batches
    await batcher.close()


async def test_close_fails_pending_and_inflight_with_nackable_error():
    release = asyncio.Event()

    async def apply(items):
        await release.wait()  # hold the first batch forever

    batcher = Batcher(apply, max_batch=1)
    first = asyncio.create_task(batcher.submit(1))    # in-flight batch
    await asyncio.sleep(0.01)  # runner now stuck in apply
    second = asyncio.create_task(batcher.submit(2))   # still queued
    await asyncio.sleep(0.01)
    await batcher.close()
    # BOTH fail with a plain Exception (BatcherClosedError) — never a hang,
    # never CancelledError — so SimpleClient's handler nacks and the broker
    # redelivers. This is the whole shutdown contract.
    with pytest.raises(BatcherClosedError):
        await asyncio.wait_for(first, timeout=1)
    with pytest.raises(BatcherClosedError):
        await asyncio.wait_for(second, timeout=1)
    assert not isinstance(first.exception(), asyncio.CancelledError)


async def test_submit_after_close_raises_immediately():
    async def apply(items):
        pass

    batcher = Batcher(apply, max_batch=10)
    await batcher.submit(1)
    await batcher.close()
    # A late delivery cannot resurrect the runner during shutdown; it gets a
    # nackable error and the message requeues.
    with pytest.raises(BatcherClosedError):
        await batcher.submit(2)
    assert batcher._runner is None or batcher._runner.done()


async def test_close_during_individual_retry_fails_remaining():
    release = asyncio.Event()
    calls = {"n": 0}

    async def apply(items):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("force per-item retry")  # whole batch fails
        await release.wait()  # first individual retry hangs

    batcher = Batcher(apply, max_batch=10)
    submits = [asyncio.create_task(batcher.submit(i)) for i in range(3)]
    await asyncio.sleep(0.02)  # batch failed, retry pass stuck on item 0
    await batcher.close()
    results = await asyncio.gather(*submits, return_exceptions=True)
    assert all(isinstance(r, BatcherClosedError) for r in results)


async def test_each_batch_gets_fresh_correlation_id():
    from app.logging.correlation import get_correlation_id, set_correlation_id

    seen: list[str] = []

    async def apply(items):
        seen.append(get_correlation_id())

    batcher = Batcher(apply, max_batch=10)
    set_correlation_id("message-one")   # first submitter's context
    await batcher.submit(1)
    await batcher.submit(2)
    # The runner must not run every batch under the first message's id.
    assert "message-one" not in seen
    assert len(set(seen)) == 2  # a distinct id per flush
    await batcher.close()
