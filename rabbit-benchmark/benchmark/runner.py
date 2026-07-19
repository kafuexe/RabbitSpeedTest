"""Orchestration: build clients, run all benchmarks, assemble suite result."""
from __future__ import annotations

import time
from datetime import datetime, timezone

from benchmark.benchmarks import (
    publish_latency, consume_latency, publish_throughput, consume_throughput,
    consume_throughput_get, round_trip, concurrent_publish, concurrent_consume,
)
from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.fake_client import FakeClient
from benchmark.clients.hybrid_client import HybridClient
from benchmark.clients.pika_client import PikaClient
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult, BenchmarkSuiteResult, collect_environment

BENCHMARKS = [
    ("publish_latency", publish_latency.run),
    ("consume_latency", consume_latency.run),
    ("publish_throughput", publish_throughput.run),
    ("consume_throughput", consume_throughput.run),
    ("consume_throughput_get", consume_throughput_get.run),
    ("round_trip", round_trip.run),
    ("concurrent_publish", concurrent_publish.run),
    ("concurrent_consume", concurrent_consume.run),
]


def _fmt_secs(s: float) -> str:
    s = int(s)
    return f"{s // 60}m{s % 60:02d}s" if s >= 60 else f"{s}s"


class _Progress:
    """Tiny stdlib progress reporter: current stage, bar, elapsed + ETA.

    Prints to stdout with flush so it shows live even when output is piped.
    Disabled (silent) unless ``enabled`` is True, so tests stay quiet.
    """

    _WIDTH = 22

    def __init__(self, total: int, enabled: bool) -> None:
        self.total = total
        self.enabled = enabled
        self.done = 0
        self.start = time.perf_counter()
        self._t0 = 0.0

    def _say(self, msg: str) -> None:
        if self.enabled:
            print(msg, flush=True)

    def suite_header(self, clients: int, benchmarks: int) -> None:
        self._say(f"Suite: {clients} client(s) x {benchmarks} benchmarks = {self.total} stages")

    def client(self, name: str, phase: str) -> None:
        self._say(f"\n=== client: {name} - {phase} ===")

    def begin(self, client: str, benchmark: str) -> None:
        self._t0 = time.perf_counter()
        self._say(f"-> [{self.done + 1:2d}/{self.total}] {client} - {benchmark} ...")

    def end(self) -> None:
        self.done += 1
        dt = time.perf_counter() - self._t0
        elapsed = time.perf_counter() - self.start
        eta = (elapsed / self.done) * (self.total - self.done) if self.done else 0.0
        filled = int(self._WIDTH * self.done / self.total) if self.total else self._WIDTH
        bar = "#" * filled + "." * (self._WIDTH - filled)
        pct = (100 * self.done / self.total) if self.total else 100
        self._say(f"   done {dt:4.1f}s  [{bar}] {pct:3.0f}%  "
                  f"elapsed {_fmt_secs(elapsed)}  eta ~{_fmt_secs(eta)}")


def build_client(name: str, config: BenchmarkConfig) -> BenchmarkClient:
    if name == "fake":
        return FakeClient()
    # Uniform wiring for every real client: unset (None) config knobs fall
    # through to each client's own defaults, so results.json labels are honest.
    kwargs: dict = {"publisher_confirms": config.publisher_confirms, "durable": config.durable}
    if config.prefetch is not None:
        kwargs["prefetch"] = config.prefetch
    if config.pipeline_batch is not None and name in ("aio-pika", "hybrid"):
        kwargs["pipeline_batch"] = config.pipeline_batch
    if name == "pika":
        return PikaClient(config.amqp_url, **kwargs)
    if name == "aio-pika":
        return AioPikaClient(config.amqp_url, **kwargs)
    if name == "hybrid":
        return HybridClient(config.amqp_url, **kwargs)
    if name == "simple":
        from benchmark.clients.simple_client import RabbitClientBench
        return RabbitClientBench(config.amqp_url, **kwargs)
    raise ValueError(f"unknown client: {name}")


async def run_suite(
    config: BenchmarkConfig, *, client_factory=build_client, show_progress: bool = False,
) -> BenchmarkSuiteResult:
    all_results: list[BenchmarkResult] = []
    rabbitmq_version: str | None = None
    progress = _Progress(len(config.clients) * len(BENCHMARKS), show_progress)
    progress.suite_header(len(config.clients), len(BENCHMARKS))
    for client_name in config.clients:
        progress.client(client_name, "connecting")
        client = client_factory(client_name, config)
        await client.connect()
        try:
            # A queue left over from a run with a different `durable` setting would
            # fail the redeclare (PRECONDITION_FAILED); queue.delete is idempotent.
            await client.delete_queue(config.queue_name)
            if rabbitmq_version is None:
                rabbitmq_version = await client.server_version()
            for bench_name, run_fn in BENCHMARKS:
                progress.begin(client_name, bench_name)
                all_results.extend(await run_fn(client, config))
                progress.end()
        finally:
            await client.close()
            progress.client(client_name, "done")

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
