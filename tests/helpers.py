"""Synthetic BenchmarkSuiteResult for reporting tests (no broker needed)."""
from __future__ import annotations

from benchmark.results import (
    BenchmarkResult, BenchmarkSuiteResult, EnvironmentInfo, IterationSample,
)
from benchmark.statistics import summarize


def _result(client, benchmark, params, values, *, mps=None, count=None, dur=None):
    samples = [IterationSample(client, benchmark, i, v, True, None, params) for i, v in enumerate(values)]
    summary = summarize(values, total_duration_ns=dur, message_count=count)
    return BenchmarkResult(client, benchmark, params, summary, samples)


def make_suite() -> BenchmarkSuiteResult:
    results: list[BenchmarkResult] = []
    for client, scale in [("pika", 1.0), ("aio-pika", 0.7)]:
        for bench in ["publish_latency", "consume_latency", "round_trip"]:
            results.append(_result(client, bench, {"size": "1KB"},
                                    [int(v * scale) for v in (100, 120, 130, 140, 160)]))
        for bench in ["publish_throughput", "consume_throughput"]:
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
