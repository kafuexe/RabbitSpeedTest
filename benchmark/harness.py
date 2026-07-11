"""Shared timing harness: warm-up + measured iterations with failure capture."""
from __future__ import annotations

import time
from typing import Awaitable, Callable

from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize


async def _collect(
    client_name: str, benchmark_name: str, params: dict,
    *, warmup: int, measured: int, op: Callable[[], Awaitable[None]],
) -> tuple[list[IterationSample], list[int], int]:
    for _ in range(warmup):
        try:
            await op()
        except Exception:
            pass  # warm-up failures are ignored
    samples: list[IterationSample] = []
    values: list[int] = []
    n_failed = 0
    for i in range(measured):
        start = time.perf_counter_ns()
        try:
            await op()
            elapsed = time.perf_counter_ns() - start
            samples.append(IterationSample(client_name, benchmark_name, i, elapsed, True, None, dict(params)))
            values.append(elapsed)
        except Exception as exc:  # record, do not abort
            n_failed += 1
            samples.append(IterationSample(client_name, benchmark_name, i, 0, False, repr(exc), dict(params)))
    return samples, values, n_failed


async def timed_iterations(
    client_name: str, benchmark_name: str, params: dict,
    *, warmup: int, measured: int, op: Callable[[], Awaitable[None]],
) -> BenchmarkResult:
    samples, values, n_failed = await _collect(
        client_name, benchmark_name, params, warmup=warmup, measured=measured, op=op)
    summary = summarize(values, n_failed=n_failed)
    return BenchmarkResult(client_name, benchmark_name, dict(params), summary, samples)


async def timed_bulk(
    client_name: str, benchmark_name: str, params: dict,
    *, warmup: int, measured: int, op: Callable[[], Awaitable[None]], message_count: int,
) -> BenchmarkResult:
    samples, values, n_failed = await _collect(
        client_name, benchmark_name, params, warmup=warmup, measured=measured, op=op)
    mean_duration = int(sum(values) / len(values)) if values else None
    summary = summarize(
        values, n_failed=n_failed,
        total_duration_ns=mean_duration, message_count=message_count)
    return BenchmarkResult(client_name, benchmark_name, dict(params), summary, samples)
