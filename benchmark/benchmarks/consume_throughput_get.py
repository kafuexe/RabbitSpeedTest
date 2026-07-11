"""Same drain as consume_throughput, but through the basic.get loop.

Run side by side with the push consumer (``consume_throughput``) this shows
the per-message round-trip cost of polling with basic.get versus letting the
broker stream deliveries.
"""
from __future__ import annotations

from benchmark.benchmarks.consume_throughput import run_drain
from benchmark.clients.base import BenchmarkClient
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult

BENCHMARK = "consume_throughput_get"

# Each basic.get is a fixed broker round-trip, so the msgs/sec rate converges
# after a couple thousand samples; draining the full message_count (50k by
# default) would multiply the runtime for no extra signal. The actual count
# used is recorded in the result params.
_MESSAGE_CAP = 2000


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    count = min(config.message_count, _MESSAGE_CAP)
    return await run_drain(client, config, BENCHMARK, client.consume_many_get,
                           message_count=count, extra_params={"messages": count})
