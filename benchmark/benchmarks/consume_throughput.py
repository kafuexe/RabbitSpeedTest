"""Drain N queued messages as fast as possible; measure msgs/sec.

Uses an explicit loop because each measured iteration needs a freshly
pre-loaded queue (the pre-load must not be timed).
"""
from __future__ import annotations

import time

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize

BENCHMARK = "consume_throughput"


async def _one_run(client: BenchmarkClient, config: BenchmarkConfig, body: bytes) -> int:
    await client.purge_queue(config.queue_name)
    await client.publish_many(
        config.exchange, config.routing_key, [body] * config.message_count,
        confirm=config.publisher_confirms)
    start = time.perf_counter_ns()
    await client.consume_many(config.queue_name, config.message_count)
    return time.perf_counter_ns() - start


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        await client.declare_queue(config.queue_name)
        for _ in range(config.warmup_iterations):
            await _one_run(client, config, body)

        samples: list[IterationSample] = []
        values: list[int] = []
        n_failed = 0
        for i in range(config.iterations):
            try:
                elapsed = await _one_run(client, config, body)
                samples.append(IterationSample(client.name, BENCHMARK, i, elapsed, True, None, {"size": label}))
                values.append(elapsed)
            except Exception as exc:
                n_failed += 1
                samples.append(IterationSample(client.name, BENCHMARK, i, 0, False, repr(exc), {"size": label}))

        mean_duration = int(sum(values) / len(values)) if values else None
        summary = summarize(values, n_failed=n_failed,
                            total_duration_ns=mean_duration, message_count=config.message_count)
        results.append(BenchmarkResult(client.name, BENCHMARK, {"size": label}, summary, samples))
    return results
