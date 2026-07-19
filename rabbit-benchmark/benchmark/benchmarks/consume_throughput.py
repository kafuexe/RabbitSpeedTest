"""Drain N queued messages as fast as possible; measure msgs/sec.

Each measured iteration needs a freshly pre-loaded queue. The purge + preload
run as untimed setup so only the drain is measured. The op verifies that all
``message_count`` messages were actually consumed and raises otherwise, so a
partial drain (which some clients can return without erroring) is recorded as a
failed iteration rather than silently inflating the throughput number.

``run_drain`` is shared with ``consume_throughput_get``, which measures the
same drain through the slow basic.get loop for a push-vs-get comparison.
"""
from __future__ import annotations

from typing import Awaitable, Callable

from benchmark.benchmarks.preload import preload
from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_bulk_with_setup
from benchmark.results import BenchmarkResult

BENCHMARK = "consume_throughput"


async def run_drain(
    client: BenchmarkClient, config: BenchmarkConfig, benchmark: str,
    drain: Callable[[str, int], Awaitable[int]],
    message_count: int | None = None,
    extra_params: dict | None = None,
) -> list[BenchmarkResult]:
    count = message_count if message_count is not None else config.message_count
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        bodies = [body] * count
        await client.declare_queue(config.queue_name)

        async def setup(bs: list[bytes] = bodies) -> None:
            await preload(client, config, bs)

        async def op() -> None:
            drained = await drain(config.queue_name, count)
            if drained != count:
                raise RuntimeError(f"under-drained: consumed {drained}/{count}")

        result = await timed_bulk_with_setup(
            client.name, benchmark, {"size": label, **(extra_params or {})},
            warmup=config.warmup_iterations, measured=config.iterations,
            setup=setup, op=op, message_count=count)
        results.append(result)
    return results


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    return await run_drain(client, config, BENCHMARK, client.consume_many)
