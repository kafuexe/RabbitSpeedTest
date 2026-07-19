"""Shared timing harness: warm-up + measured iterations with failure capture."""
from __future__ import annotations

import time
from typing import Awaitable, Callable

from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize


async def _collect(
    client_name: str, benchmark_name: str, params: dict,
    *, warmup: int, measured: int, op: Callable[[], Awaitable[None]],
    setup: Callable[[], Awaitable[None]] | None = None,
) -> tuple[list[IterationSample], list[int], int]:
    async def _prepare() -> None:
        if setup is not None:
            await setup()  # untimed per-iteration preparation

    for _ in range(warmup):
        try:
            await _prepare()
            await op()
        except Exception:
            pass  # warm-up failures are ignored
    samples: list[IterationSample] = []
    values: list[int] = []
    n_failed = 0
    for i in range(measured):
        try:
            await _prepare()  # setup is excluded from the measured region
            start = time.perf_counter_ns()
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


async def timed_bulk_with_setup(
    client_name: str, benchmark_name: str, params: dict,
    *, warmup: int, measured: int,
    setup: Callable[[], Awaitable[None]], op: Callable[[], Awaitable[None]],
    message_count: int,
) -> BenchmarkResult:
    """Like ``timed_bulk`` but runs an untimed ``setup()`` before each timed
    ``op()``. Use when every measured iteration needs fresh state (e.g. a
    freshly (re)loaded queue) that must not be counted in the measured cost.
    """
    samples, values, n_failed = await _collect(
        client_name, benchmark_name, params,
        warmup=warmup, measured=measured, op=op, setup=setup)
    mean_duration = int(sum(values) / len(values)) if values else None
    summary = summarize(
        values, n_failed=n_failed,
        total_duration_ns=mean_duration, message_count=message_count)
    return BenchmarkResult(client_name, benchmark_name, dict(params), summary, samples)
