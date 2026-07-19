"""Publish N messages as fast as possible; measure msgs/sec (publish only).

The queue purge runs as untimed setup before each iteration so the measured
region is a pure ``publish_many`` and the queue never grows unbounded.
"""
from __future__ import annotations

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_bulk_with_setup
from benchmark.results import BenchmarkResult

BENCHMARK = "publish_throughput"


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        bodies = [body] * config.message_count
        await client.declare_queue(config.queue_name)

        async def setup() -> None:
            await client.purge_queue(config.queue_name)

        async def op(bs: list[bytes] = bodies) -> None:
            await client.publish_many(config.exchange, config.routing_key, bs, confirm=config.publisher_confirms)

        result = await timed_bulk_with_setup(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.iterations,
            setup=setup, op=op, message_count=config.message_count)
        results.append(result)
    return results
