"""Single-consume latency benchmark (queue pre-loaded before measuring)."""
from __future__ import annotations

from benchmark.benchmarks.preload import preload
from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_iterations
from benchmark.results import BenchmarkResult

BENCHMARK = "consume_latency"


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        total = config.latency_sample_count + config.warmup_iterations
        await client.declare_queue(config.queue_name)
        await preload(client, config, [body] * total)

        async def op() -> None:
            await client.consume_one(config.queue_name)

        result = await timed_iterations(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.latency_sample_count, op=op)
        results.append(result)
    return results
