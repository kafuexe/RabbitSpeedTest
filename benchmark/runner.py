"""Orchestration: build clients, run all benchmarks, assemble suite result."""
from __future__ import annotations

from datetime import datetime, timezone

from benchmark.benchmarks import (
    publish_latency, consume_latency, publish_throughput, consume_throughput,
    round_trip, concurrent_publish, concurrent_consume,
)
from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.fake_client import FakeClient
from benchmark.clients.pika_client import PikaClient
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult, BenchmarkSuiteResult, collect_environment

BENCHMARKS = [
    ("publish_latency", publish_latency.run),
    ("consume_latency", consume_latency.run),
    ("publish_throughput", publish_throughput.run),
    ("consume_throughput", consume_throughput.run),
    ("round_trip", round_trip.run),
    ("concurrent_publish", concurrent_publish.run),
    ("concurrent_consume", concurrent_consume.run),
]


def build_client(name: str, config: BenchmarkConfig) -> BenchmarkClient:
    if name == "pika":
        return PikaClient(config.amqp_url, prefetch=config.prefetch, management_url=config.management_url)
    if name == "aio-pika":
        return AioPikaClient(config.amqp_url, prefetch=config.prefetch, management_url=config.management_url)
    if name == "fake":
        return FakeClient()
    raise ValueError(f"unknown client: {name}")


async def run_suite(config: BenchmarkConfig, *, client_factory=build_client) -> BenchmarkSuiteResult:
    all_results: list[BenchmarkResult] = []
    rabbitmq_version: str | None = None
    for client_name in config.clients:
        client = client_factory(client_name, config)
        await client.connect()
        try:
            if rabbitmq_version is None:
                rabbitmq_version = await client.server_version()
            for _name, run_fn in BENCHMARKS:
                all_results.extend(await run_fn(client, config))
        finally:
            await client.close()

    return BenchmarkSuiteResult(
        timestamp=datetime.now(timezone.utc).isoformat(),
        config=config.to_dict(),
        environment=collect_environment(rabbitmq_version),
        results=all_results,
    )


def scaling_efficiency(results: list[BenchmarkResult], benchmark: str, client: str) -> dict[int, float]:
    mps: dict[int, float] = {}
    for r in results:
        if r.benchmark == benchmark and r.client == client:
            n = int(r.params["concurrency"])
            mps[n] = r.summary.messages_per_sec or 0.0
        base = mps.get(1)
    base = mps.get(1)
    eff: dict[int, float] = {}
    for n, v in mps.items():
        eff[n] = (v / (n * base)) if base else 0.0
    return eff
