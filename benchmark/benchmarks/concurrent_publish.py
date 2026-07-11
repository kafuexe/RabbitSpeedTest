"""Concurrent publishers: aggregate msgs/sec across concurrency levels."""
from __future__ import annotations

import asyncio
import time

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize

BENCHMARK = "concurrent_publish"


def _pick_size(config: BenchmarkConfig) -> str:
    return "1KB" if "1KB" in config.message_sizes else next(iter(config.message_sizes))


async def _one_run(client: BenchmarkClient, config: BenchmarkConfig, body: bytes, n: int) -> int:
    per_worker = max(1, config.message_count // n)
    bodies = [body] * per_worker
    await client.purge_queue(config.queue_name)

    async def worker() -> None:
        await client.publish_many(config.exchange, config.routing_key, bodies, confirm=config.publisher_confirms)

    start = time.perf_counter_ns()
    await asyncio.gather(*(worker() for _ in range(n)))
    return time.perf_counter_ns() - start


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    label = _pick_size(config)
    body = generate_payloads(config.message_sizes)[label]
    await client.declare_queue(config.queue_name)
    results: list[BenchmarkResult] = []
    for n in config.concurrency_levels:
        total_msgs = max(1, config.message_count // n) * n
        for _ in range(config.warmup_iterations):
            await _one_run(client, config, body, n)
        samples: list[IterationSample] = []
        values: list[int] = []
        n_failed = 0
        for i in range(config.iterations):
            try:
                elapsed = await _one_run(client, config, body, n)
                samples.append(IterationSample(client.name, BENCHMARK, i, elapsed, True, None,
                                              {"concurrency": n, "size": label}))
                values.append(elapsed)
            except Exception as exc:
                n_failed += 1
                samples.append(IterationSample(client.name, BENCHMARK, i, 0, False, repr(exc),
                                              {"concurrency": n, "size": label}))
        mean_duration = int(sum(values) / len(values)) if values else None
        summary = summarize(values, n_failed=n_failed,
                            total_duration_ns=mean_duration, message_count=total_msgs)
        results.append(BenchmarkResult(client.name, BENCHMARK, {"concurrency": n, "size": label}, summary, samples))
    return results
