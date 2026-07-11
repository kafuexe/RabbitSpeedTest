"""Publish N messages as fast as possible; measure msgs/sec."""
from __future__ import annotations

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_bulk
from benchmark.results import BenchmarkResult

BENCHMARK = "publish_throughput"


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        bodies = [body] * config.message_count
        await client.declare_queue(config.queue_name)
        await client.purge_queue(config.queue_name)

        async def op(bs: list[bytes] = bodies) -> None:
            await client.publish_many(config.exchange, config.routing_key, bs, confirm=config.publisher_confirms)
            await client.purge_queue(config.queue_name)  # keep queue from growing unbounded

        result = await timed_bulk(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.iterations,
            op=op, message_count=config.message_count)
        results.append(result)
    return results
