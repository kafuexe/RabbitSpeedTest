"""Pure statistical helpers over raw nanosecond samples."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass
class StatSummary:
    avg_ns: float
    median_ns: float
    min_ns: float
    max_ns: float
    stddev_ns: float
    p95_ns: float
    p99_ns: float
    n_success: int
    n_failed: int
    messages_per_sec: float | None = None
    total_duration_ns: int | None = None


def summarize(
    values_ns: Sequence[int],
    *,
    n_failed: int = 0,
    total_duration_ns: int | None = None,
    message_count: int | None = None,
) -> StatSummary:
    """Compute the standard stat block from raw ns samples.

    Percentiles use linear interpolation. Stddev is the population stddev.
    Throughput fields are populated only when duration and count are given.
    """
    messages_per_sec: float | None = None
    if total_duration_ns is not None and message_count is not None and total_duration_ns > 0:
        messages_per_sec = message_count / (total_duration_ns / 1_000_000_000)

    if not values_ns:
        return StatSummary(
            avg_ns=0.0, median_ns=0.0, min_ns=0.0, max_ns=0.0, stddev_ns=0.0,
            p95_ns=0.0, p99_ns=0.0, n_success=0, n_failed=n_failed,
            messages_per_sec=messages_per_sec, total_duration_ns=total_duration_ns,
        )

    arr = np.asarray(values_ns, dtype=np.float64)
    return StatSummary(
        avg_ns=float(arr.mean()),
        median_ns=float(np.median(arr)),
        min_ns=float(arr.min()),
        max_ns=float(arr.max()),
        stddev_ns=float(arr.std()),  # population stddev
        p95_ns=float(np.percentile(arr, 95)),
        p99_ns=float(np.percentile(arr, 99)),
        n_success=len(values_ns),
        n_failed=n_failed,
        messages_per_sec=messages_per_sec,
        total_duration_ns=total_duration_ns,
    )
