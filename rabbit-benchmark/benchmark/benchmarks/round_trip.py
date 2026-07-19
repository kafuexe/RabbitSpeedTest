"""Publish -> consume round-trip latency benchmark."""
from __future__ import annotations

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_iterations
from benchmark.results import BenchmarkResult

BENCHMARK = "round_trip"


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        await client.declare_queue(config.queue_name)
        await client.purge_queue(config.queue_name)

        async def op(b: bytes = body) -> None:
            await client.publish(config.exchange, config.routing_key, b, confirm=config.publisher_confirms)
            msg = await client.consume_one(config.queue_name)
            if msg is None:
                raise RuntimeError("round-trip consume returned no message")

        result = await timed_iterations(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.latency_sample_count, op=op)
        results.append(result)
    return results
