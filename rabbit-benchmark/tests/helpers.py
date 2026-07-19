"""Shared test fixtures: synthetic suite results and client-test helpers."""
from __future__ import annotations

import inspect

from benchmark.clients.fake_client import FakeClient
from benchmark.results import (
    BenchmarkResult, BenchmarkSuiteResult, EnvironmentInfo, IterationSample,
)
from benchmark.statistics import summarize

# The canonical async interface every BenchmarkClient must expose.
CLIENT_METHODS = [
    "connect", "close", "declare_queue", "purge_queue", "delete_queue",
    "publish", "consume_one", "publish_many", "consume_many",
    "consume_many_get", "queue_depth", "server_version",
]


def assert_client_methods_are_coroutines(client) -> None:
    for m in CLIENT_METHODS:
        assert inspect.iscoroutinefunction(getattr(client, m)), m


class RecordingFakeClient(FakeClient):
    """FakeClient that records (method, kwargs) for delegation/routing tests."""

    def __init__(self):
        super().__init__()
        self.calls: list[tuple[str, dict]] = []

    async def publish_many(self, exchange, routing_key, bodies, *, confirm):
        self.calls.append(("publish_many", {"confirm": confirm}))
        await super().publish_many(exchange, routing_key, bodies, confirm=confirm)

    async def consume_one(self, queue, timeout=5.0):
        self.calls.append(("consume_one", {}))
        return await super().consume_one(queue, timeout)

    async def consume_many(self, queue, count):
        self.calls.append(("consume_many", {}))
        return await super().consume_many(queue, count)

    async def consume_many_get(self, queue, count):
        self.calls.append(("consume_many_get", {}))
        return await super().consume_many_get(queue, count)


def _result(client, benchmark, params, values, *, mps=None, count=None, dur=None):
    samples = [IterationSample(client, benchmark, i, v, True, None, params) for i, v in enumerate(values)]
    summary = summarize(values, total_duration_ns=dur, message_count=count)
    return BenchmarkResult(client, benchmark, params, summary, samples)


def make_suite(clients=(("pika", 1.0), ("aio-pika", 0.7))) -> BenchmarkSuiteResult:
    results: list[BenchmarkResult] = []
    for client, scale in clients:
        for bench in ["publish_latency", "consume_latency", "round_trip"]:
            results.append(_result(client, bench, {"size": "1KB"},
                                    [int(v * scale) for v in (100, 120, 130, 140, 160)]))
        for bench in ["publish_throughput", "consume_throughput", "consume_throughput_get"]:
            results.append(_result(client, bench, {"size": "1KB"},
                                    [1_000_000, 1_100_000], count=1000,
                                    dur=int(1_050_000 * scale)))
        for bench in ["concurrent_publish", "concurrent_consume"]:
            for n in (1, 2, 4):
                results.append(_result(client, bench, {"concurrency": n, "size": "1KB"},
                                        [1_000_000], count=100 * n,
                                        dur=int(1_000_000 * scale)))
    env = EnvironmentInfo("3.12.0", "Windows 11", "TestCPU", 8, 16 * 1024**3, "3.13.0")
    return BenchmarkSuiteResult("2026-07-10T00:00:00+00:00", {"message_count": 100}, env, results)
