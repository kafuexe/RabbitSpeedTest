"""Drain N queued messages as fast as possible; measure msgs/sec.

Each measured iteration needs a freshly pre-loaded queue. The purge + preload
run as untimed setup so only the drain is measured. The op verifies that all
``message_count`` messages were actually consumed and raises otherwise, so a
partial drain (which some clients can return without erroring) is recorded as a
failed iteration rather than silently inflating the throughput number.
"""
from __future__ import annotations

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_bulk_with_setup
from benchmark.results import BenchmarkResult

BENCHMARK = "consume_throughput"


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        bodies = [body] * config.message_count
        await client.declare_queue(config.queue_name)

        async def setup(bs: list[bytes] = bodies) -> None:
            await client.purge_queue(config.queue_name)
            await client.publish_many(config.exchange, config.routing_key, bs, confirm=config.publisher_confirms)

        async def op() -> None:
            drained = await client.consume_many(config.queue_name, config.message_count)
            if drained != config.message_count:
                raise RuntimeError(f"under-drained: consumed {drained}/{config.message_count}")

        result = await timed_bulk_with_setup(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.iterations,
            setup=setup, op=op, message_count=config.message_count)
        results.append(result)
    return results
