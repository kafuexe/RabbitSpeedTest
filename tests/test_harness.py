import asyncio
from benchmark.harness import timed_iterations, timed_bulk


async def test_timed_iterations_counts_and_summarizes():
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        await asyncio.sleep(0)

    res = await timed_iterations("fake", "b", {"x": 1}, warmup=2, measured=5, op=op)
    assert calls["n"] == 7               # warmup + measured
    assert res.summary.n_success == 5
    assert res.summary.n_failed == 0
    assert len(res.samples) == 5
    assert res.client == "fake" and res.benchmark == "b"


async def test_timed_iterations_records_failures():
    async def op():
        raise RuntimeError("boom")

    res = await timed_iterations("fake", "b", {}, warmup=0, measured=3, op=op)
    assert res.summary.n_success == 0
    assert res.summary.n_failed == 3
    assert all(s.success is False and s.error for s in res.samples)


async def test_timed_bulk_computes_throughput():
    async def op():
        await asyncio.sleep(0.01)

    res = await timed_bulk("fake", "pub_tp", {}, warmup=0, measured=3, op=op, message_count=1000)
    assert res.summary.messages_per_sec is not None
    assert res.summary.messages_per_sec > 0
    assert res.summary.total_duration_ns is not None
