# RabbitMQ Benchmark Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python suite that benchmarks pika vs aio-pika against RabbitMQ and generates statistically sound HTML + PDF reports with Plotly charts.

**Architecture:** Six one-directional layers — config, clients (async interface wrapping sync pika in a thread executor), benchmarks (depend only on the client interface), statistics (pure functions), results (dataclasses + JSON/CSV persistence), and reporting (reads only the results schema). A shared harness runs warm-up + measured iterations timed with `perf_counter_ns()`.

**Tech Stack:** Python 3.12+, pika, aio-pika, numpy, pandas, plotly + kaleido, jinja2, weasyprint (optional at runtime), psutil (optional), dataclasses, full type hints.

## Global Constraints

- Python 3.12+ (env has 3.14). Full type hints throughout.
- All timing uses `time.perf_counter_ns()`.
- Payloads pre-generated once as `bytes`; allocation never timed.
- Every benchmark: warm-up iterations (default 5, discarded) + measured iterations (default 10). Per-iteration failures recorded (`success=False`), never abort the run.
- Stats collected everywhere: avg, median, min, max, stddev, p95, p99. Throughput also: messages/sec, total duration.
- Benchmarks depend ONLY on `BenchmarkClient`. Reporting depends ONLY on the results schema.
- Default `amqp_url = amqp://guest:guest@localhost:5672/`. Default `message_count = 50_000`. Concurrency levels `[1,2,4,8,16,32]`. Publisher confirms ON by default. Message sizes 256 B / 1 KB / 10 KB / 100 KB.
- PDF is pluggable with graceful fallback: HTML always produced; missing WeasyPrint native libs → warn + continue.
- Package root is `benchmark/`. Tests in `tests/`. Frequent commits, TDD.

---

### Task 1: Project scaffolding and dependencies

**Files:**
- Create: `pyproject.toml`
- Create: `requirements.txt`
- Create: `benchmark/__init__.py`
- Create: `benchmark/clients/__init__.py`
- Create: `benchmark/benchmarks/__init__.py`
- Create: `benchmark/reporting/__init__.py`
- Create: `benchmark/reporting/templates/.gitkeep`
- Create: `benchmark/reporting/assets/.gitkeep`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

**Interfaces:**
- Produces: importable `benchmark` package; `pytest` runnable.

- [ ] **Step 1: Create `requirements.txt`**

```
pika>=1.3
aio-pika>=9.4
numpy>=1.26
pandas>=2.2
plotly>=5.20
kaleido>=0.2
jinja2>=3.1
weasyprint>=61
psutil>=5.9
pytest>=8.0
pytest-asyncio>=0.23
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "rabbitmq-benchmark"
version = "0.1.0"
description = "Benchmark suite comparing pika and aio-pika against RabbitMQ"
requires-python = ">=3.12"
dynamic = ["dependencies"]

[tool.setuptools.dynamic]
dependencies = { file = ["requirements.txt"] }

[tool.setuptools.packages.find]
include = ["benchmark*"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 3: Create the package `__init__.py` files** (all empty except root)

`benchmark/__init__.py`:
```python
"""RabbitMQ client benchmark suite."""

__version__ = "0.1.0"
```

Create empty `benchmark/clients/__init__.py`, `benchmark/benchmarks/__init__.py`, `benchmark/reporting/__init__.py`, `tests/__init__.py`, and empty `.gitkeep` files for `benchmark/reporting/templates/` and `benchmark/reporting/assets/`.

- [ ] **Step 4: Create `tests/conftest.py`** (shared pytest config marker)

```python
"""Shared pytest fixtures and configuration."""
```

- [ ] **Step 5: Create and activate a venv, install deps**

Run:
```bash
python -m venv .venv
.venv/Scripts/python -m pip install -U pip
.venv/Scripts/python -m pip install -r requirements.txt
```
Expected: all install. If `weasyprint` or `kaleido` fail to build on this Python, note it — they are optional at runtime and handled by fallbacks in later tasks. Core (pika, aio-pika, numpy, pandas, plotly, jinja2, pytest) must install.

- [ ] **Step 6: Verify import**

Run: `.venv/Scripts/python -c "import benchmark; print(benchmark.__version__)"`
Expected: `0.1.0`

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "chore: scaffold benchmark package and dependencies"
```

---

### Task 2: Statistics module

**Files:**
- Create: `benchmark/statistics.py`
- Test: `tests/test_statistics.py`

**Interfaces:**
- Produces:
  - `@dataclass StatSummary` with fields `avg_ns, median_ns, min_ns, max_ns, stddev_ns, p95_ns, p99_ns: float`; `n_success, n_failed: int`; `messages_per_sec: float | None`; `total_duration_ns: int | None`.
  - `summarize(values_ns: Sequence[int], *, n_failed: int = 0, total_duration_ns: int | None = None, message_count: int | None = None) -> StatSummary`. When `total_duration_ns` and `message_count` are given, computes `messages_per_sec`. Empty `values_ns` → all stat fields `0.0`.

- [ ] **Step 1: Write the failing test**

```python
import math
from benchmark.statistics import summarize, StatSummary


def test_summarize_basic_percentiles():
    values = list(range(1, 101))  # 1..100 ns
    s = summarize(values)
    assert s.min_ns == 1
    assert s.max_ns == 100
    assert s.avg_ns == 50.5
    assert s.median_ns == 50.5
    assert math.isclose(s.p95_ns, 95.05, rel_tol=1e-6)
    assert math.isclose(s.p99_ns, 99.01, rel_tol=1e-6)
    assert s.n_success == 100
    assert s.n_failed == 0
    assert s.messages_per_sec is None


def test_summarize_empty_is_zeroed():
    s = summarize([], n_failed=3)
    assert s.avg_ns == 0.0 and s.max_ns == 0.0 and s.p99_ns == 0.0
    assert s.n_success == 0
    assert s.n_failed == 3


def test_summarize_throughput_fields():
    s = summarize([10, 20, 30], total_duration_ns=1_000_000_000, message_count=500)
    assert s.total_duration_ns == 1_000_000_000
    assert math.isclose(s.messages_per_sec, 500.0, rel_tol=1e-9)


def test_summarize_stddev():
    s = summarize([2, 4, 4, 4, 5, 5, 7, 9])
    assert math.isclose(s.stddev_ns, 2.0, rel_tol=1e-9)  # population stddev
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_statistics.py -v`
Expected: FAIL (`ModuleNotFoundError` / cannot import `summarize`).

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/test_statistics.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/statistics.py tests/test_statistics.py
git commit -m "feat: add statistics summarize with 7-stat block"
```

---

### Task 3: Configuration module

**Files:**
- Create: `benchmark/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces:
  - `MESSAGE_SIZES: dict[str, int]` = `{"256B": 256, "1KB": 1024, "10KB": 10240, "100KB": 102400}`.
  - `@dataclass BenchmarkConfig` fields: `amqp_url: str`, `management_url: str | None`, `queue_name: str`, `exchange: str`, `routing_key: str`, `message_count: int`, `message_sizes: dict[str, int]`, `iterations: int`, `warmup_iterations: int`, `concurrency_levels: list[int]`, `publisher_confirms: bool`, `prefetch: int`, `clients: list[str]`, `output_dir: str`, `latency_sample_count: int`.
  - `BenchmarkConfig.default() -> BenchmarkConfig`.
  - `BenchmarkConfig.load(json_path: str | None = None, overrides: dict | None = None) -> BenchmarkConfig` — merge order defaults → JSON file → env (`RABBITMQ_URL`, `RABBITMQ_MANAGEMENT_URL`) → overrides dict.
  - `BenchmarkConfig.to_dict() -> dict`.

- [ ] **Step 1: Write the failing test**

```python
import json
import os
from benchmark.config import BenchmarkConfig, MESSAGE_SIZES


def test_default_config():
    c = BenchmarkConfig.default()
    assert c.amqp_url == "amqp://guest:guest@localhost:5672/"
    assert c.message_count == 50_000
    assert c.concurrency_levels == [1, 2, 4, 8, 16, 32]
    assert c.publisher_confirms is True
    assert c.iterations == 10 and c.warmup_iterations == 5
    assert set(c.message_sizes) == set(MESSAGE_SIZES)
    assert c.clients == ["pika", "aio-pika"]


def test_load_merges_json_then_env_then_overrides(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg.json"
    cfg_file.write_text(json.dumps({"message_count": 100, "queue_name": "from_json"}))
    monkeypatch.setenv("RABBITMQ_URL", "amqp://env-host/")
    c = BenchmarkConfig.load(str(cfg_file), overrides={"message_count": 7})
    assert c.message_count == 7          # override wins
    assert c.queue_name == "from_json"   # json applied
    assert c.amqp_url == "amqp://env-host/"  # env applied over default


def test_to_dict_roundtrip():
    c = BenchmarkConfig.default()
    d = c.to_dict()
    assert d["message_count"] == 50_000
    assert isinstance(d["concurrency_levels"], list)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_config.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
"""Benchmark configuration with defaults / JSON / env / override merge."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict

MESSAGE_SIZES: dict[str, int] = {
    "256B": 256,
    "1KB": 1024,
    "10KB": 10240,
    "100KB": 102400,
}


@dataclass
class BenchmarkConfig:
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    management_url: str | None = "http://guest:guest@localhost:15672"
    queue_name: str = "benchmark_queue"
    exchange: str = ""
    routing_key: str = "benchmark_queue"
    message_count: int = 50_000
    message_sizes: dict[str, int] = field(default_factory=lambda: dict(MESSAGE_SIZES))
    iterations: int = 10
    warmup_iterations: int = 5
    concurrency_levels: list[int] = field(default_factory=lambda: [1, 2, 4, 8, 16, 32])
    publisher_confirms: bool = True
    prefetch: int = 100
    clients: list[str] = field(default_factory=lambda: ["pika", "aio-pika"])
    output_dir: str = "results"
    latency_sample_count: int = 1000

    @classmethod
    def default(cls) -> "BenchmarkConfig":
        return cls()

    @classmethod
    def load(
        cls,
        json_path: str | None = None,
        overrides: dict | None = None,
    ) -> "BenchmarkConfig":
        data: dict = asdict(cls.default())
        if json_path and os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as fh:
                data.update(json.load(fh))
        env_url = os.environ.get("RABBITMQ_URL")
        if env_url:
            data["amqp_url"] = env_url
        env_mgmt = os.environ.get("RABBITMQ_MANAGEMENT_URL")
        if env_mgmt:
            data["management_url"] = env_mgmt
        if overrides:
            data.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**data)

    def to_dict(self) -> dict:
        return asdict(self)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/test_config.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/config.py tests/test_config.py
git commit -m "feat: add BenchmarkConfig with layered merge"
```

---

### Task 4: Results schema and persistence

**Files:**
- Create: `benchmark/results.py`
- Test: `tests/test_results.py`

**Interfaces:**
- Consumes: `StatSummary` from `benchmark.statistics`; `BenchmarkConfig` from `benchmark.config`.
- Produces:
  - `@dataclass IterationSample`: `client: str, benchmark: str, iteration: int, value_ns: int, success: bool, error: str | None, params: dict`.
  - `@dataclass EnvironmentInfo`: `python_version, os_platform, cpu_model, cpu_count, total_memory_bytes, rabbitmq_version` (all `str | int | None`), plus `collect_environment(rabbitmq_version: str | None = None) -> EnvironmentInfo`.
  - `@dataclass BenchmarkResult`: `client: str, benchmark: str, params: dict, summary: StatSummary, samples: list[IterationSample]`.
  - `@dataclass BenchmarkSuiteResult`: `timestamp: str, config: dict, environment: EnvironmentInfo, results: list[BenchmarkResult]`.
  - `save_json(suite: BenchmarkSuiteResult, path: str) -> None`, `load_json(path: str) -> BenchmarkSuiteResult`, `save_csv(suite: BenchmarkSuiteResult, path: str) -> None` (long format, one row per sample).

- [ ] **Step 1: Write the failing test**

```python
import os
from benchmark.results import (
    IterationSample, BenchmarkResult, BenchmarkSuiteResult,
    EnvironmentInfo, collect_environment, save_json, load_json, save_csv,
)
from benchmark.statistics import summarize


def _sample_suite():
    samples = [
        IterationSample("pika", "publish_latency", i, 100 + i, True, None, {"size": "256B"})
        for i in range(5)
    ]
    summary = summarize([s.value_ns for s in samples])
    res = BenchmarkResult("pika", "publish_latency", {"size": "256B"}, summary, samples)
    env = EnvironmentInfo("3.12.0", "Windows", "TestCPU", 8, 16 * 1024**3, "3.13.0")
    return BenchmarkSuiteResult("2026-07-10T00:00:00", {"message_count": 5}, env, [res])


def test_collect_environment_populates_python():
    env = collect_environment("3.13.0")
    assert env.python_version.startswith("3.")
    assert env.rabbitmq_version == "3.13.0"
    assert isinstance(env.cpu_count, int)


def test_json_roundtrip(tmp_path):
    suite = _sample_suite()
    p = str(tmp_path / "out.json")
    save_json(suite, p)
    loaded = load_json(p)
    assert loaded.timestamp == suite.timestamp
    assert loaded.results[0].client == "pika"
    assert loaded.results[0].summary.min_ns == 100
    assert len(loaded.results[0].samples) == 5
    assert loaded.environment.cpu_model == "TestCPU"


def test_csv_is_long_format(tmp_path):
    suite = _sample_suite()
    p = str(tmp_path / "out.csv")
    save_csv(suite, p)
    lines = open(p, encoding="utf-8").read().strip().splitlines()
    assert len(lines) == 1 + 5  # header + 5 samples
    assert "value_ns" in lines[0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_results.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
"""Result schema dataclasses and JSON/CSV persistence."""
from __future__ import annotations

import json
import os
import platform
from dataclasses import dataclass, asdict, field

import pandas as pd

from benchmark.statistics import StatSummary


@dataclass
class IterationSample:
    client: str
    benchmark: str
    iteration: int
    value_ns: int
    success: bool
    error: str | None
    params: dict = field(default_factory=dict)


@dataclass
class EnvironmentInfo:
    python_version: str
    os_platform: str
    cpu_model: str
    cpu_count: int | None
    total_memory_bytes: int | None
    rabbitmq_version: str | None


@dataclass
class BenchmarkResult:
    client: str
    benchmark: str
    params: dict
    summary: StatSummary
    samples: list[IterationSample]


@dataclass
class BenchmarkSuiteResult:
    timestamp: str
    config: dict
    environment: EnvironmentInfo
    results: list[BenchmarkResult]


def collect_environment(rabbitmq_version: str | None = None) -> EnvironmentInfo:
    cpu_model = platform.processor() or platform.machine() or "unknown"
    total_mem: int | None = None
    try:
        import psutil  # optional
        total_mem = int(psutil.virtual_memory().total)
    except Exception:
        total_mem = None
    return EnvironmentInfo(
        python_version=platform.python_version(),
        os_platform=f"{platform.system()} {platform.release()}",
        cpu_model=cpu_model,
        cpu_count=os.cpu_count(),
        total_memory_bytes=total_mem,
        rabbitmq_version=rabbitmq_version,
    )


def save_json(suite: BenchmarkSuiteResult, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(asdict(suite), fh, indent=2)


def load_json(path: str) -> BenchmarkSuiteResult:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    env = EnvironmentInfo(**data["environment"])
    results: list[BenchmarkResult] = []
    for r in data["results"]:
        summary = StatSummary(**r["summary"])
        samples = [IterationSample(**s) for s in r["samples"]]
        results.append(BenchmarkResult(r["client"], r["benchmark"], r["params"], summary, samples))
    return BenchmarkSuiteResult(data["timestamp"], data["config"], env, results)


def save_csv(suite: BenchmarkSuiteResult, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    rows: list[dict] = []
    for r in suite.results:
        for s in r.samples:
            row = {
                "client": s.client,
                "benchmark": s.benchmark,
                "iteration": s.iteration,
                "value_ns": s.value_ns,
                "success": s.success,
                "error": s.error,
            }
            for k, v in s.params.items():
                row[f"param_{k}"] = v
            rows.append(row)
    pd.DataFrame(rows).to_csv(path, index=False)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/test_results.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/results.py tests/test_results.py
git commit -m "feat: add results schema with JSON/CSV persistence"
```

---

### Task 5: Client interface, payloads, and a fake client

**Files:**
- Create: `benchmark/clients/base.py`
- Create: `benchmark/clients/fake_client.py`
- Test: `tests/test_base_and_fake.py`

**Interfaces:**
- Produces:
  - `generate_payloads(sizes: dict[str, int]) -> dict[str, bytes]` — deterministic bytes per size label (`b"x" * n`).
  - `class BenchmarkClient(abc.ABC)` with `name: str` and async methods: `connect()`, `close()`, `declare_queue(name: str)`, `purge_queue(name: str)`, `delete_queue(name: str)`, `publish(exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None`, `consume_one(queue: str, timeout: float = 5.0) -> bytes | None`, `publish_many(exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None`, `consume_many(queue: str, count: int) -> int` (returns number consumed), `server_version() -> str | None`.
  - `class FakeClient(BenchmarkClient)` — in-memory queue dict, `name="fake"`, used to unit-test the harness and benchmarks without a broker.

- [ ] **Step 1: Write the failing test**

```python
import pytest
from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.clients.fake_client import FakeClient


def test_generate_payloads_sizes():
    payloads = generate_payloads({"256B": 256, "1KB": 1024})
    assert len(payloads["256B"]) == 256
    assert len(payloads["1KB"]) == 1024
    assert isinstance(payloads["256B"], bytes)


async def test_fake_client_publish_consume_roundtrip():
    c = FakeClient()
    await c.connect()
    await c.declare_queue("q")
    await c.publish("", "q", b"hello", confirm=True)
    msg = await c.consume_one("q")
    assert msg == b"hello"
    assert await c.consume_one("q", timeout=0.01) is None
    await c.close()


async def test_fake_client_many():
    c = FakeClient()
    await c.connect()
    await c.declare_queue("q")
    await c.publish_many("", "q", [b"a", b"b", b"c"], confirm=True)
    assert await c.consume_many("q", 3) == 3


def test_fake_is_benchmark_client():
    assert issubclass(FakeClient, BenchmarkClient)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_base_and_fake.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write `benchmark/clients/base.py`**

```python
"""Abstract benchmark client interface and payload generation."""
from __future__ import annotations

import abc


def generate_payloads(sizes: dict[str, int]) -> dict[str, bytes]:
    """Pre-generate a reusable byte payload for each size label."""
    return {label: b"x" * n for label, n in sizes.items()}


class BenchmarkClient(abc.ABC):
    """Uniform async interface both pika and aio-pika implement.

    Sync clients (pika) wrap blocking calls in a thread executor so the
    runner can drive every client through one async code path.
    """

    name: str = "base"

    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def declare_queue(self, name: str) -> None: ...

    @abc.abstractmethod
    async def purge_queue(self, name: str) -> None: ...

    @abc.abstractmethod
    async def delete_queue(self, name: str) -> None: ...

    @abc.abstractmethod
    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None: ...

    @abc.abstractmethod
    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None: ...

    @abc.abstractmethod
    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None: ...

    @abc.abstractmethod
    async def consume_many(self, queue: str, count: int) -> int: ...

    @abc.abstractmethod
    async def server_version(self) -> str | None: ...
```

- [ ] **Step 4: Write `benchmark/clients/fake_client.py`**

```python
"""In-memory fake client for testing the harness without a broker."""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque

from benchmark.clients.base import BenchmarkClient


class FakeClient(BenchmarkClient):
    name = "fake"

    def __init__(self) -> None:
        self._queues: dict[str, deque[bytes]] = defaultdict(deque)

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def declare_queue(self, name: str) -> None:
        self._queues.setdefault(name, deque())

    async def purge_queue(self, name: str) -> None:
        self._queues[name].clear()

    async def delete_queue(self, name: str) -> None:
        self._queues.pop(name, None)

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        self._queues[routing_key].append(body)

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        q = self._queues[queue]
        if q:
            return q.popleft()
        await asyncio.sleep(0)
        return None

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        self._queues[routing_key].extend(bodies)

    async def consume_many(self, queue: str, count: int) -> int:
        q = self._queues[queue]
        consumed = 0
        while q and consumed < count:
            q.popleft()
            consumed += 1
        return consumed

    async def server_version(self) -> str | None:
        return "fake-1.0"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_base_and_fake.py -v`
Expected: PASS (4 passed).

- [ ] **Step 6: Commit**

```bash
git add benchmark/clients/base.py benchmark/clients/fake_client.py tests/test_base_and_fake.py
git commit -m "feat: add BenchmarkClient interface, payloads, fake client"
```

---

### Task 6: pika (sync) client

**Files:**
- Create: `benchmark/clients/pika_client.py`
- Test: `tests/test_pika_client.py`

**Interfaces:**
- Consumes: `BenchmarkClient` base.
- Produces: `class PikaClient(BenchmarkClient)` with `name="pika"`, constructed as `PikaClient(amqp_url: str, *, prefetch: int = 100, management_url: str | None = None)`. All blocking pika calls run in a dedicated single-thread executor so each `PikaClient` owns one pika connection on one thread (pika connections are not thread-safe). Uses `pika.BlockingConnection`; publisher confirms via `channel.confirm_delivery()` when `confirm=True`.

**Note:** This task's tests are import/structure tests only (no broker). Live behavior is covered by the smoke test (Task 15).

- [ ] **Step 1: Write the failing test**

```python
import inspect
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.pika_client import PikaClient


def test_pika_client_is_benchmark_client():
    assert issubclass(PikaClient, BenchmarkClient)
    assert PikaClient("amqp://x/").name == "pika"


def test_pika_methods_are_coroutines():
    c = PikaClient("amqp://x/")
    for m in ["connect", "close", "declare_queue", "publish", "consume_one",
              "publish_many", "consume_many", "server_version"]:
        assert inspect.iscoroutinefunction(getattr(c, m)), m
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_pika_client.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
"""Synchronous pika client wrapped in a single-thread executor."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pika

from benchmark.clients.base import BenchmarkClient


class PikaClient(BenchmarkClient):
    name = "pika"

    def __init__(self, amqp_url: str, *, prefetch: int = 100, management_url: str | None = None) -> None:
        self._url = amqp_url
        self._prefetch = prefetch
        self._management_url = management_url
        # One dedicated thread: a pika connection must be used from one thread.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pika")
        self._conn: pika.BlockingConnection | None = None
        self._channel = None

    async def _run(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args))

    # ---- lifecycle ----
    def _connect_sync(self) -> None:
        self._conn = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._conn.channel()
        self._channel.basic_qos(prefetch_count=self._prefetch)

    async def connect(self) -> None:
        await self._run(self._connect_sync)

    def _close_sync(self) -> None:
        if self._conn and self._conn.is_open:
            self._conn.close()

    async def close(self) -> None:
        await self._run(self._close_sync)
        self._executor.shutdown(wait=True)

    # ---- queue admin ----
    async def declare_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_declare(queue=name, durable=False))

    async def purge_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_purge(queue=name))

    async def delete_queue(self, name: str) -> None:
        await self._run(lambda: self._channel.queue_delete(queue=name))

    # ---- publish / consume ----
    def _publish_sync(self, exchange: str, routing_key: str, body: bytes, confirm: bool) -> None:
        if confirm and not getattr(self._channel, "_delivery_confirmation", False):
            self._channel.confirm_delivery()
        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        await self._run(self._publish_sync, exchange, routing_key, body, confirm)

    def _consume_one_sync(self, queue: str) -> bytes | None:
        method, _props, body = self._channel.basic_get(queue=queue, auto_ack=True)
        return body if method is not None else None

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        return await self._run(self._consume_one_sync, queue)

    def _publish_many_sync(self, exchange: str, routing_key: str, bodies: list[bytes], confirm: bool) -> None:
        if confirm and not getattr(self._channel, "_delivery_confirmation", False):
            self._channel.confirm_delivery()
        for body in bodies:
            self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        await self._run(self._publish_many_sync, exchange, routing_key, bodies, confirm)

    def _consume_many_sync(self, queue: str, count: int) -> int:
        consumed = 0
        while consumed < count:
            method, _props, body = self._channel.basic_get(queue=queue, auto_ack=True)
            if method is None:
                break
            consumed += 1
        return consumed

    async def consume_many(self, queue: str, count: int) -> int:
        return await self._run(self._consume_many_sync, queue, count)

    async def server_version(self) -> str | None:
        def _ver() -> str | None:
            try:
                props = self._conn._impl._connection.server_properties  # best-effort
                v = props.get("version")
                return v.decode() if isinstance(v, bytes) else v
            except Exception:
                return None
        return await self._run(_ver)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_pika_client.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/clients/pika_client.py tests/test_pika_client.py
git commit -m "feat: add pika client over thread executor"
```

---

### Task 7: aio-pika (async) client

**Files:**
- Create: `benchmark/clients/aio_pika_client.py`
- Test: `tests/test_aio_pika_client.py`

**Interfaces:**
- Consumes: `BenchmarkClient` base.
- Produces: `class AioPikaClient(BenchmarkClient)` with `name="aio-pika"`, constructed as `AioPikaClient(amqp_url: str, *, prefetch: int = 100, management_url: str | None = None)`. Uses `aio_pika.connect_robust`. `publish_many` overlaps publishes with `asyncio.gather` in bounded batches (natural async pipelining). Publisher confirms map to aio-pika's default publish acknowledgement (delivery confirmation is on by default for `Channel.default_exchange.publish`).

- [ ] **Step 1: Write the failing test**

```python
import inspect
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.aio_pika_client import AioPikaClient


def test_aio_pika_client_is_benchmark_client():
    assert issubclass(AioPikaClient, BenchmarkClient)
    assert AioPikaClient("amqp://x/").name == "aio-pika"


def test_aio_pika_methods_are_coroutines():
    c = AioPikaClient("amqp://x/")
    for m in ["connect", "close", "declare_queue", "publish", "consume_one",
              "publish_many", "consume_many", "server_version"]:
        assert inspect.iscoroutinefunction(getattr(c, m)), m
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_aio_pika_client.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
"""Asynchronous aio-pika client with natural pipelining for bulk publish."""
from __future__ import annotations

import asyncio

import aio_pika

from benchmark.clients.base import BenchmarkClient

_PIPELINE_BATCH = 500


class AioPikaClient(BenchmarkClient):
    name = "aio-pika"

    def __init__(self, amqp_url: str, *, prefetch: int = 100, management_url: str | None = None) -> None:
        self._url = amqp_url
        self._prefetch = prefetch
        self._management_url = management_url
        self._conn: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None

    async def connect(self) -> None:
        self._conn = await aio_pika.connect_robust(self._url)
        self._channel = await self._conn.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=self._prefetch)

    async def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed:
            await self._conn.close()

    async def declare_queue(self, name: str) -> None:
        await self._channel.declare_queue(name, durable=False)

    async def purge_queue(self, name: str) -> None:
        q = await self._channel.declare_queue(name, durable=False)
        await q.purge()

    async def delete_queue(self, name: str) -> None:
        await self._channel.queue_delete(name)

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        msg = aio_pika.Message(body=body)
        ex = self._channel.default_exchange if exchange == "" else await self._channel.get_exchange(exchange)
        await ex.publish(msg, routing_key=routing_key)

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        q = await self._channel.declare_queue(queue, durable=False)
        msg = await q.get(no_ack=True, fail=False)
        return msg.body if msg is not None else None

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        ex = self._channel.default_exchange if exchange == "" else await self._channel.get_exchange(exchange)
        for start in range(0, len(bodies), _PIPELINE_BATCH):
            batch = bodies[start:start + _PIPELINE_BATCH]
            await asyncio.gather(*(ex.publish(aio_pika.Message(body=b), routing_key=routing_key) for b in batch))

    async def consume_many(self, queue: str, count: int) -> int:
        q = await self._channel.declare_queue(queue, durable=False)
        consumed = 0
        while consumed < count:
            msg = await q.get(no_ack=True, fail=False)
            if msg is None:
                break
            consumed += 1
        return consumed

    async def server_version(self) -> str | None:
        try:
            props = self._conn.transport.connection.server_properties  # best-effort
            v = props.get("version")
            return v.decode() if isinstance(v, bytes) else v
        except Exception:
            return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_aio_pika_client.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/clients/aio_pika_client.py tests/test_aio_pika_client.py
git commit -m "feat: add aio-pika client with pipelined bulk publish"
```

---

### Task 8: Benchmark harness helpers

**Files:**
- Create: `benchmark/harness.py`
- Test: `tests/test_harness.py`

**Interfaces:**
- Consumes: `IterationSample`, `BenchmarkResult` from `benchmark.results`; `summarize` from `benchmark.statistics`.
- Produces:
  - `async def timed_iterations(client_name, benchmark_name, params, *, warmup, measured, op) -> BenchmarkResult` where `op` is `Callable[[], Awaitable[None]]`. Runs `warmup` discarded calls then `measured` timed calls with `perf_counter_ns()`, catching exceptions per iteration (records `success=False`, `value_ns=0`, error string), and returns a `BenchmarkResult` whose summary is built from successful sample values and `n_failed` count.
  - `async def timed_bulk(client_name, benchmark_name, params, *, warmup, measured, op, message_count) -> BenchmarkResult` — like above but `op` returns nothing; each measured call is one bulk run, and the summary includes `messages_per_sec`/`total_duration_ns` computed from the mean per-run duration and `message_count`.

- [ ] **Step 1: Write the failing test**

```python
import asyncio
from benchmark.harness import timed_iterations, timed_bulk


async def test_timed_iterations_counts_and_summarizes():
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        await asyncio.sleep(0)

    res = await timed_iterations("fake", "b", {"x": 1}, warmup=2, measured=5, op=op)
    assert calls["n"] == 7               # warmup + measured
    assert res.summary.n_success == 5
    assert res.summary.n_failed == 0
    assert len(res.samples) == 5
    assert res.client == "fake" and res.benchmark == "b"


async def test_timed_iterations_records_failures():
    async def op():
        raise RuntimeError("boom")

    res = await timed_iterations("fake", "b", {}, warmup=0, measured=3, op=op)
    assert res.summary.n_success == 0
    assert res.summary.n_failed == 3
    assert all(s.success is False and s.error for s in res.samples)


async def test_timed_bulk_computes_throughput():
    async def op():
        await asyncio.sleep(0.01)

    res = await timed_bulk("fake", "pub_tp", {}, warmup=0, measured=3, op=op, message_count=1000)
    assert res.summary.messages_per_sec is not None
    assert res.summary.messages_per_sec > 0
    assert res.summary.total_duration_ns is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_harness.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_harness.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/harness.py tests/test_harness.py
git commit -m "feat: add timing harness with warmup and failure capture"
```

---

### Task 9: Latency benchmarks (publish + consume)

**Files:**
- Create: `benchmark/benchmarks/publish_latency.py`
- Create: `benchmark/benchmarks/consume_latency.py`
- Test: `tests/test_latency_benchmarks.py`

**Interfaces:**
- Consumes: `BenchmarkClient`, `BenchmarkConfig`, `generate_payloads`, `timed_iterations`.
- Produces:
  - `async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]` in each module.
  - `publish_latency.run`: for each message size, times single `client.publish(...)` calls; one `BenchmarkResult` per size (`benchmark="publish_latency"`, `params={"size": label}`). `measured = config.latency_sample_count`.
  - `consume_latency.run`: for each size, pre-loads `latency_sample_count + warmup` messages, then times single `client.consume_one(...)` calls; one result per size (`benchmark="consume_latency"`).

- [ ] **Step 1: Write the failing test**

```python
from benchmark.config import BenchmarkConfig
from benchmark.clients.fake_client import FakeClient
from benchmark.benchmarks import publish_latency, consume_latency


def _cfg():
    c = BenchmarkConfig.default()
    c.message_sizes = {"256B": 256, "1KB": 1024}
    c.warmup_iterations = 2
    c.latency_sample_count = 10
    return c


async def test_publish_latency_one_result_per_size():
    client = FakeClient(); await client.connect()
    results = await publish_latency.run(client, _cfg())
    assert {r.params["size"] for r in results} == {"256B", "1KB"}
    assert all(r.benchmark == "publish_latency" for r in results)
    assert all(r.summary.n_success == 10 for r in results)


async def test_consume_latency_measures_after_preload():
    client = FakeClient(); await client.connect()
    results = await consume_latency.run(client, _cfg())
    assert all(r.benchmark == "consume_latency" for r in results)
    assert all(r.summary.n_success == 10 for r in results)
    assert all(r.summary.n_failed == 0 for r in results)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_latency_benchmarks.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write `benchmark/benchmarks/publish_latency.py`**

```python
"""Single-publish latency benchmark."""
from __future__ import annotations

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_iterations
from benchmark.results import BenchmarkResult

BENCHMARK = "publish_latency"


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        await client.declare_queue(config.queue_name)
        await client.purge_queue(config.queue_name)

        async def op(b: bytes = body) -> None:
            await client.publish(config.exchange, config.routing_key, b, confirm=config.publisher_confirms)

        result = await timed_iterations(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.latency_sample_count, op=op)
        results.append(result)
    return results
```

- [ ] **Step 4: Write `benchmark/benchmarks/consume_latency.py`**

```python
"""Single-consume latency benchmark (queue pre-loaded before measuring)."""
from __future__ import annotations

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
        await client.purge_queue(config.queue_name)
        await client.publish_many(
            config.exchange, config.routing_key, [body] * total, confirm=config.publisher_confirms)

        async def op() -> None:
            await client.consume_one(config.queue_name)

        result = await timed_iterations(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.latency_sample_count, op=op)
        results.append(result)
    return results
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_latency_benchmarks.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add benchmark/benchmarks/publish_latency.py benchmark/benchmarks/consume_latency.py tests/test_latency_benchmarks.py
git commit -m "feat: add publish and consume latency benchmarks"
```

---

### Task 10: Throughput benchmarks (publish + consume)

**Files:**
- Create: `benchmark/benchmarks/publish_throughput.py`
- Create: `benchmark/benchmarks/consume_throughput.py`
- Test: `tests/test_throughput_benchmarks.py`

**Interfaces:**
- Produces: `run(client, config) -> list[BenchmarkResult]` in each module, using `timed_bulk`. One result per message size. `publish_throughput` publishes `config.message_count` messages per iteration. `consume_throughput` pre-loads `message_count` messages before each measured iteration (via a `setup` before each run) and drains them. Because `timed_bulk` needs a clean queue per measured run, `consume_throughput` uses its own loop (below) rather than `timed_bulk`.

- [ ] **Step 1: Write the failing test**

```python
from benchmark.config import BenchmarkConfig
from benchmark.clients.fake_client import FakeClient
from benchmark.benchmarks import publish_throughput, consume_throughput


def _cfg():
    c = BenchmarkConfig.default()
    c.message_sizes = {"256B": 256}
    c.message_count = 200
    c.iterations = 3
    c.warmup_iterations = 1
    return c


async def test_publish_throughput_has_msgs_per_sec():
    client = FakeClient(); await client.connect()
    results = await publish_throughput.run(client, _cfg())
    assert len(results) == 1
    r = results[0]
    assert r.benchmark == "publish_throughput"
    assert r.summary.messages_per_sec and r.summary.messages_per_sec > 0


async def test_consume_throughput_drains_all():
    client = FakeClient(); await client.connect()
    results = await consume_throughput.run(client, _cfg())
    r = results[0]
    assert r.benchmark == "consume_throughput"
    assert r.summary.messages_per_sec and r.summary.messages_per_sec > 0
    assert r.summary.n_failed == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_throughput_benchmarks.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write `benchmark/benchmarks/publish_throughput.py`**

```python
"""Publish N messages as fast as possible; measure msgs/sec."""
from __future__ import annotations

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.harness import timed_bulk
from benchmark.results import BenchmarkResult

BENCHMARK = "publish_throughput"


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        bodies = [body] * config.message_count
        await client.declare_queue(config.queue_name)
        await client.purge_queue(config.queue_name)

        async def op(bs: list[bytes] = bodies) -> None:
            await client.publish_many(config.exchange, config.routing_key, bs, confirm=config.publisher_confirms)
            await client.purge_queue(config.queue_name)  # keep queue from growing unbounded

        result = await timed_bulk(
            client.name, BENCHMARK, {"size": label},
            warmup=config.warmup_iterations, measured=config.iterations,
            op=op, message_count=config.message_count)
        results.append(result)
    return results
```

- [ ] **Step 4: Write `benchmark/benchmarks/consume_throughput.py`**

```python
"""Drain N queued messages as fast as possible; measure msgs/sec.

Uses an explicit loop because each measured iteration needs a freshly
pre-loaded queue (the pre-load must not be timed).
"""
from __future__ import annotations

import time

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize

BENCHMARK = "consume_throughput"


async def _one_run(client: BenchmarkClient, config: BenchmarkConfig, body: bytes) -> int:
    await client.purge_queue(config.queue_name)
    await client.publish_many(
        config.exchange, config.routing_key, [body] * config.message_count,
        confirm=config.publisher_confirms)
    start = time.perf_counter_ns()
    await client.consume_many(config.queue_name, config.message_count)
    return time.perf_counter_ns() - start


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    payloads = generate_payloads(config.message_sizes)
    results: list[BenchmarkResult] = []
    for label, body in payloads.items():
        await client.declare_queue(config.queue_name)
        for _ in range(config.warmup_iterations):
            await _one_run(client, config, body)

        samples: list[IterationSample] = []
        values: list[int] = []
        n_failed = 0
        for i in range(config.iterations):
            try:
                elapsed = await _one_run(client, config, body)
                samples.append(IterationSample(client.name, BENCHMARK, i, elapsed, True, None, {"size": label}))
                values.append(elapsed)
            except Exception as exc:
                n_failed += 1
                samples.append(IterationSample(client.name, BENCHMARK, i, 0, False, repr(exc), {"size": label}))

        mean_duration = int(sum(values) / len(values)) if values else None
        summary = summarize(values, n_failed=n_failed,
                            total_duration_ns=mean_duration, message_count=config.message_count)
        results.append(BenchmarkResult(client.name, BENCHMARK, {"size": label}, summary, samples))
    return results
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_throughput_benchmarks.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add benchmark/benchmarks/publish_throughput.py benchmark/benchmarks/consume_throughput.py tests/test_throughput_benchmarks.py
git commit -m "feat: add publish and consume throughput benchmarks"
```

---

### Task 11: Round-trip benchmark

**Files:**
- Create: `benchmark/benchmarks/round_trip.py`
- Test: `tests/test_round_trip.py`

**Interfaces:**
- Produces: `run(client, config) -> list[BenchmarkResult]`. For each size, times publish-then-consume-one of the same message; one result per size (`benchmark="round_trip"`, full 7-stat block). `measured = config.latency_sample_count`.

- [ ] **Step 1: Write the failing test**

```python
from benchmark.config import BenchmarkConfig
from benchmark.clients.fake_client import FakeClient
from benchmark.benchmarks import round_trip


def _cfg():
    c = BenchmarkConfig.default()
    c.message_sizes = {"256B": 256}
    c.warmup_iterations = 1
    c.latency_sample_count = 8
    return c


async def test_round_trip_produces_stats():
    client = FakeClient(); await client.connect()
    results = await round_trip.run(client, _cfg())
    r = results[0]
    assert r.benchmark == "round_trip"
    assert r.summary.n_success == 8
    assert r.summary.p99_ns >= r.summary.median_ns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_round_trip.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/test_round_trip.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/benchmarks/round_trip.py tests/test_round_trip.py
git commit -m "feat: add round-trip latency benchmark"
```

---

### Task 12: Concurrent benchmarks (publish + consume)

**Files:**
- Create: `benchmark/benchmarks/concurrent_publish.py`
- Create: `benchmark/benchmarks/concurrent_consume.py`
- Test: `tests/test_concurrent_benchmarks.py`

**Interfaces:**
- Produces: `run(client, config) -> list[BenchmarkResult]` in each module. For each concurrency level `n` in `config.concurrency_levels`, spawn `n` concurrent workers (each publishing/consuming `config.message_count // n` messages) via `asyncio.gather`. Times the whole aggregate run with `perf_counter_ns()`. One `BenchmarkResult` per concurrency level, `params={"concurrency": n, "size": <first size label>}`, summary with `messages_per_sec` (aggregate) and `total_duration_ns`. Uses a fixed representative size (`"1KB"` if present, else the first configured size) to keep the concurrency sweep one-dimensional.
- The runner (Task 13) computes scaling efficiency across levels for reporting; the benchmark stores per-level aggregate msgs/sec.

- [ ] **Step 1: Write the failing test**

```python
from benchmark.config import BenchmarkConfig
from benchmark.clients.fake_client import FakeClient
from benchmark.benchmarks import concurrent_publish, concurrent_consume


def _cfg():
    c = BenchmarkConfig.default()
    c.message_sizes = {"1KB": 1024}
    c.message_count = 400
    c.iterations = 2
    c.warmup_iterations = 1
    c.concurrency_levels = [1, 2, 4]
    return c


async def test_concurrent_publish_one_result_per_level():
    client = FakeClient(); await client.connect()
    results = await concurrent_publish.run(client, _cfg())
    assert {r.params["concurrency"] for r in results} == {1, 2, 4}
    assert all(r.benchmark == "concurrent_publish" for r in results)
    assert all(r.summary.messages_per_sec and r.summary.messages_per_sec > 0 for r in results)


async def test_concurrent_consume_one_result_per_level():
    client = FakeClient(); await client.connect()
    results = await concurrent_consume.run(client, _cfg())
    assert {r.params["concurrency"] for r in results} == {1, 2, 4}
    assert all(r.benchmark == "concurrent_consume" for r in results)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_concurrent_benchmarks.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write `benchmark/benchmarks/concurrent_publish.py`**

```python
"""Concurrent publishers: aggregate msgs/sec across concurrency levels."""
from __future__ import annotations

import asyncio
import time

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize

BENCHMARK = "concurrent_publish"


def _pick_size(config: BenchmarkConfig) -> str:
    return "1KB" if "1KB" in config.message_sizes else next(iter(config.message_sizes))


async def _one_run(client: BenchmarkClient, config: BenchmarkConfig, body: bytes, n: int) -> int:
    per_worker = max(1, config.message_count // n)
    bodies = [body] * per_worker
    await client.purge_queue(config.queue_name)

    async def worker() -> None:
        await client.publish_many(config.exchange, config.routing_key, bodies, confirm=config.publisher_confirms)

    start = time.perf_counter_ns()
    await asyncio.gather(*(worker() for _ in range(n)))
    return time.perf_counter_ns() - start


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    label = _pick_size(config)
    body = generate_payloads(config.message_sizes)[label]
    await client.declare_queue(config.queue_name)
    results: list[BenchmarkResult] = []
    for n in config.concurrency_levels:
        total_msgs = max(1, config.message_count // n) * n
        for _ in range(config.warmup_iterations):
            await _one_run(client, config, body, n)
        samples: list[IterationSample] = []
        values: list[int] = []
        n_failed = 0
        for i in range(config.iterations):
            try:
                elapsed = await _one_run(client, config, body, n)
                samples.append(IterationSample(client.name, BENCHMARK, i, elapsed, True, None,
                                              {"concurrency": n, "size": label}))
                values.append(elapsed)
            except Exception as exc:
                n_failed += 1
                samples.append(IterationSample(client.name, BENCHMARK, i, 0, False, repr(exc),
                                              {"concurrency": n, "size": label}))
        mean_duration = int(sum(values) / len(values)) if values else None
        summary = summarize(values, n_failed=n_failed,
                            total_duration_ns=mean_duration, message_count=total_msgs)
        results.append(BenchmarkResult(client.name, BENCHMARK, {"concurrency": n, "size": label}, summary, samples))
    return results
```

- [ ] **Step 4: Write `benchmark/benchmarks/concurrent_consume.py`**

```python
"""Concurrent consumers: aggregate msgs/sec across concurrency levels."""
from __future__ import annotations

import asyncio
import time

from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.config import BenchmarkConfig
from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize

BENCHMARK = "concurrent_consume"


def _pick_size(config: BenchmarkConfig) -> str:
    return "1KB" if "1KB" in config.message_sizes else next(iter(config.message_sizes))


async def _one_run(client: BenchmarkClient, config: BenchmarkConfig, body: bytes, n: int) -> int:
    per_worker = max(1, config.message_count // n)
    total = per_worker * n
    await client.purge_queue(config.queue_name)
    await client.publish_many(config.exchange, config.routing_key, [body] * total, confirm=config.publisher_confirms)

    async def worker() -> None:
        await client.consume_many(config.queue_name, per_worker)

    start = time.perf_counter_ns()
    await asyncio.gather(*(worker() for _ in range(n)))
    return time.perf_counter_ns() - start


async def run(client: BenchmarkClient, config: BenchmarkConfig) -> list[BenchmarkResult]:
    label = _pick_size(config)
    body = generate_payloads(config.message_sizes)[label]
    await client.declare_queue(config.queue_name)
    results: list[BenchmarkResult] = []
    for n in config.concurrency_levels:
        total_msgs = max(1, config.message_count // n) * n
        for _ in range(config.warmup_iterations):
            await _one_run(client, config, body, n)
        samples: list[IterationSample] = []
        values: list[int] = []
        n_failed = 0
        for i in range(config.iterations):
            try:
                elapsed = await _one_run(client, config, body, n)
                samples.append(IterationSample(client.name, BENCHMARK, i, elapsed, True, None,
                                              {"concurrency": n, "size": label}))
                values.append(elapsed)
            except Exception as exc:
                n_failed += 1
                samples.append(IterationSample(client.name, BENCHMARK, i, 0, False, repr(exc),
                                              {"concurrency": n, "size": label}))
        mean_duration = int(sum(values) / len(values)) if values else None
        summary = summarize(values, n_failed=n_failed,
                            total_duration_ns=mean_duration, message_count=total_msgs)
        results.append(BenchmarkResult(client.name, BENCHMARK, {"concurrency": n, "size": label}, summary, samples))
    return results
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_concurrent_benchmarks.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add benchmark/benchmarks/concurrent_publish.py benchmark/benchmarks/concurrent_consume.py tests/test_concurrent_benchmarks.py
git commit -m "feat: add concurrent publish and consume benchmarks"
```

---

### Task 13: Runner / orchestration

**Files:**
- Create: `benchmark/runner.py`
- Test: `tests/test_runner.py`

**Interfaces:**
- Consumes: all benchmark modules, `BenchmarkConfig`, `collect_environment`, `BenchmarkSuiteResult`, clients.
- Produces:
  - `BENCHMARKS: list[tuple[str, Callable]]` — ordered `(name, run_fn)` pairs for the seven benchmarks.
  - `def build_client(name: str, config: BenchmarkConfig) -> BenchmarkClient` — factory mapping `"pika"`/`"aio-pika"`/`"fake"` to client instances.
  - `async def run_suite(config: BenchmarkConfig, *, client_factory=build_client) -> BenchmarkSuiteResult` — for each client name in `config.clients`: build, connect, run every benchmark (collecting `BenchmarkResult`s), capture RabbitMQ version from the first client, close. Returns a populated `BenchmarkSuiteResult` with ISO timestamp and environment info.
  - `def scaling_efficiency(results: list[BenchmarkResult], benchmark: str, client: str) -> dict[int, float]` — from concurrent results, `eff[n] = mps[n] / (n * mps[1])`.

- [ ] **Step 1: Write the failing test**

```python
from benchmark.config import BenchmarkConfig
from benchmark.clients.fake_client import FakeClient
from benchmark.runner import run_suite, scaling_efficiency, BENCHMARKS


def _cfg():
    c = BenchmarkConfig.default()
    c.clients = ["fake"]
    c.message_sizes = {"256B": 256, "1KB": 1024}
    c.message_count = 100
    c.iterations = 2
    c.warmup_iterations = 1
    c.latency_sample_count = 5
    c.concurrency_levels = [1, 2]
    return c


def _factory(name, config):
    return FakeClient()


async def test_run_suite_covers_seven_benchmarks():
    suite = await run_suite(_cfg(), client_factory=_factory)
    names = {r.benchmark for r in suite.results}
    assert names == {
        "publish_latency", "consume_latency", "publish_throughput",
        "consume_throughput", "round_trip", "concurrent_publish", "concurrent_consume",
    }
    assert suite.environment.python_version.startswith("3.")
    assert suite.timestamp


async def test_scaling_efficiency():
    suite = await run_suite(_cfg(), client_factory=_factory)
    eff = scaling_efficiency(suite.results, "concurrent_publish", "fake")
    assert eff[1] == 1.0
    assert set(eff) == {1, 2}


def test_benchmarks_registry_has_seven():
    assert len(BENCHMARKS) == 7
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_runner.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_runner.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add benchmark/runner.py tests/test_runner.py
git commit -m "feat: add suite runner and scaling efficiency"
```

---

### Task 14: Charts module

**Files:**
- Create: `benchmark/reporting/charts.py`
- Test: `tests/test_charts.py`

**Interfaces:**
- Consumes: `BenchmarkSuiteResult`, `scaling_efficiency`.
- Produces a `Charts` builder returning Plotly `Figure` objects and rendered outputs:
  - `latency_comparison(suite) -> Figure` (grouped bar: publish/consume/round-trip median per client).
  - `throughput_comparison(suite) -> Figure` (horizontal bar: publish/consume msgs/sec per client).
  - `concurrent_chart(suite, benchmark) -> Figure` (line: msgs/sec vs concurrency, one line per client).
  - `scaling_chart(suite, benchmark) -> Figure` (line: efficiency vs concurrency).
  - `distribution_chart(suite, benchmark) -> Figure` (box plots of raw sample values per client).
  - `to_html_div(fig) -> str` (interactive, `include_plotlyjs="cdn"` for the report generator to inline as needed) and `to_png_bytes(fig) -> bytes | None` (via kaleido; returns `None` if kaleido unavailable).
  - `build_all(suite) -> dict[str, Figure]` returning the standard chart set keyed by name.

- [ ] **Step 1: Write the failing test**

```python
import plotly.graph_objects as go
from benchmark.reporting.charts import Charts
from tests.helpers import make_suite  # created below


def test_build_all_returns_expected_charts():
    suite = make_suite()
    charts = Charts()
    figs = charts.build_all(suite)
    for key in ["latency", "throughput", "concurrent_publish", "concurrent_consume",
                "scaling_publish", "distribution_round_trip"]:
        assert key in figs
        assert isinstance(figs[key], go.Figure)


def test_to_html_div_is_string():
    suite = make_suite()
    charts = Charts()
    html = charts.to_html_div(charts.latency_comparison(suite))
    assert isinstance(html, str) and "plotly" in html.lower()
```

- [ ] **Step 2: Create `tests/helpers.py`** (shared synthetic suite builder)

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_charts.py -v`
Expected: FAIL (cannot import `Charts`).

- [ ] **Step 4: Write minimal implementation**

```python
"""Plotly chart construction and rendering (HTML + PNG)."""
from __future__ import annotations

import plotly.graph_objects as go

from benchmark.results import BenchmarkResult, BenchmarkSuiteResult
from benchmark.runner import scaling_efficiency

_LATENCY_BENCHES = ["publish_latency", "consume_latency", "round_trip"]
_LATENCY_LABELS = {"publish_latency": "Publish", "consume_latency": "Consume", "round_trip": "Round-trip"}


def _clients(suite: BenchmarkSuiteResult) -> list[str]:
    seen: list[str] = []
    for r in suite.results:
        if r.client not in seen:
            seen.append(r.client)
    return seen


def _median_ms(suite: BenchmarkSuiteResult, client: str, benchmark: str) -> float:
    vals = [r.summary.median_ns for r in suite.results if r.client == client and r.benchmark == benchmark]
    return (sum(vals) / len(vals)) / 1_000_000 if vals else 0.0


def _mps(suite: BenchmarkSuiteResult, client: str, benchmark: str) -> float:
    vals = [r.summary.messages_per_sec or 0.0
            for r in suite.results if r.client == client and r.benchmark == benchmark]
    return sum(vals) / len(vals) if vals else 0.0


class Charts:
    def latency_comparison(self, suite: BenchmarkSuiteResult) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            fig.add_bar(name=client,
                        x=[_LATENCY_LABELS[b] for b in _LATENCY_BENCHES],
                        y=[_median_ms(suite, client, b) for b in _LATENCY_BENCHES])
        fig.update_layout(barmode="group", title="Latency comparison (median)",
                          yaxis_title="Latency (ms)", template="plotly_white")
        return fig

    def throughput_comparison(self, suite: BenchmarkSuiteResult) -> go.Figure:
        benches = ["publish_throughput", "consume_throughput"]
        labels = {"publish_throughput": "Publish", "consume_throughput": "Consume"}
        fig = go.Figure()
        for client in _clients(suite):
            fig.add_bar(name=client, orientation="h",
                        y=[labels[b] for b in benches],
                        x=[_mps(suite, client, b) for b in benches])
        fig.update_layout(barmode="group", title="Throughput comparison",
                          xaxis_title="Messages / sec", template="plotly_white")
        return fig

    def _concurrent_points(self, suite, client, benchmark) -> tuple[list[int], list[float]]:
        pts = sorted(
            (int(r.params["concurrency"]), r.summary.messages_per_sec or 0.0)
            for r in suite.results if r.client == client and r.benchmark == benchmark)
        return [p[0] for p in pts], [p[1] for p in pts]

    def concurrent_chart(self, suite: BenchmarkSuiteResult, benchmark: str) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            xs, ys = self._concurrent_points(suite, client, benchmark)
            fig.add_scatter(name=client, x=xs, y=ys, mode="lines+markers")
        fig.update_layout(title=f"{benchmark.replace('_', ' ').title()} scaling",
                          xaxis_title="Concurrent workers", yaxis_title="Messages / sec",
                          template="plotly_white")
        return fig

    def scaling_chart(self, suite: BenchmarkSuiteResult, benchmark: str) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            eff = scaling_efficiency(suite.results, benchmark, client)
            xs = sorted(eff)
            fig.add_scatter(name=client, x=xs, y=[eff[n] for n in xs], mode="lines+markers")
        fig.update_layout(title=f"Scaling efficiency: {benchmark.replace('_', ' ')}",
                          xaxis_title="Concurrent workers", yaxis_title="Efficiency (1.0 = linear)",
                          template="plotly_white")
        return fig

    def distribution_chart(self, suite: BenchmarkSuiteResult, benchmark: str) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            values: list[float] = []
            for r in suite.results:
                if r.client == client and r.benchmark == benchmark:
                    values.extend(s.value_ns / 1_000_000 for s in r.samples if s.success)
            if values:
                fig.add_box(name=client, y=values, boxpoints="outliers")
        fig.update_layout(title=f"Latency distribution: {benchmark.replace('_', ' ')}",
                          yaxis_title="Latency (ms)", template="plotly_white")
        return fig

    def build_all(self, suite: BenchmarkSuiteResult) -> dict[str, go.Figure]:
        return {
            "latency": self.latency_comparison(suite),
            "throughput": self.throughput_comparison(suite),
            "concurrent_publish": self.concurrent_chart(suite, "concurrent_publish"),
            "concurrent_consume": self.concurrent_chart(suite, "concurrent_consume"),
            "scaling_publish": self.scaling_chart(suite, "concurrent_publish"),
            "scaling_consume": self.scaling_chart(suite, "concurrent_consume"),
            "distribution_round_trip": self.distribution_chart(suite, "round_trip"),
        }

    def to_html_div(self, fig: go.Figure) -> str:
        return fig.to_html(full_html=False, include_plotlyjs="cdn")

    def to_png_bytes(self, fig: go.Figure) -> bytes | None:
        try:
            return fig.to_image(format="png", width=900, height=500, scale=2)
        except Exception:
            return None  # kaleido/chrome unavailable
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_charts.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add benchmark/reporting/charts.py tests/test_charts.py tests/helpers.py
git commit -m "feat: add Plotly charts builder"
```

---

### Task 15: Report generator, template, and PDF backend

**Files:**
- Create: `benchmark/reporting/report_generator.py`
- Create: `benchmark/reporting/templates/report.html.j2`
- Test: `tests/test_report_generator.py`

**Interfaces:**
- Consumes: `BenchmarkSuiteResult`, `Charts`.
- Produces:
  - `class PdfBackend(abc.ABC)` with `available() -> bool` and `render(html: str, out_path: str) -> bool`.
  - `class WeasyPrintBackend(PdfBackend)` — `available()` returns True only if `weasyprint` imports; `render` writes a PDF, returns True on success, False (with a printed warning) on failure.
  - `def build_executive_summary(suite) -> list[dict]` — per category, which client won and by how much (median latency lower is better; msgs/sec higher is better).
  - `def generate_report(suite, out_dir, *, pdf_backend=None) -> dict[str, str]` — renders HTML (embeds interactive chart divs + static PNGs base64 for PDF), writes `report.html`, attempts PDF via backend (default `WeasyPrintBackend`), returns `{"html": path, "pdf": path_or_empty}`. Never raises if PDF unavailable — logs a warning and returns `pdf=""`.

- [ ] **Step 1: Write the failing test**

```python
import os
from benchmark.reporting.report_generator import (
    generate_report, build_executive_summary, WeasyPrintBackend,
)
from tests.helpers import make_suite


class _NoPdf(WeasyPrintBackend):
    def available(self) -> bool:
        return False


def test_executive_summary_picks_winners():
    rows = build_executive_summary(make_suite())
    assert any(r["category"].lower().startswith("publish") for r in rows)
    assert all("winner" in r for r in rows)


def test_generate_report_writes_html_and_handles_missing_pdf(tmp_path):
    out = generate_report(make_suite(), str(tmp_path), pdf_backend=_NoPdf())
    assert os.path.exists(out["html"])
    html = open(out["html"], encoding="utf-8").read()
    assert "Executive Summary" in html
    assert "plotly" in html.lower()
    assert out["pdf"] == ""  # gracefully skipped
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_report_generator.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write `benchmark/reporting/templates/report.html.j2`**

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>RabbitMQ Client Benchmark Report</title>
<style>
  body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; color: #1a1a2e; margin: 0; padding: 0 2rem 4rem; }
  .title-page { text-align: center; padding: 6rem 0 4rem; border-bottom: 3px solid #ff6600; }
  .title-page h1 { font-size: 2.6rem; margin: 0; }
  .subtitle { color: #666; font-size: 1.1rem; }
  h2 { border-bottom: 2px solid #eee; padding-bottom: .3rem; margin-top: 3rem; }
  table { border-collapse: collapse; width: 100%; margin: 1rem 0; font-size: .92rem; }
  th, td { border: 1px solid #ddd; padding: .5rem .7rem; text-align: right; }
  th:first-child, td:first-child { text-align: left; }
  th { background: #fff3e6; }
  .summary-card { background: #f7f7fb; border-left: 4px solid #ff6600; padding: .8rem 1rem; margin: .6rem 0; }
  .chart { margin: 1.5rem 0; }
  .kv { display: grid; grid-template-columns: max-content 1fr; gap: .3rem 1.5rem; }
  .muted { color: #888; }
</style>
</head>
<body>
  <section class="title-page">
    <h1>RabbitMQ Client Benchmark Report</h1>
    <p class="subtitle">pika vs aio-pika &middot; {{ suite.timestamp }}</p>
  </section>

  <h2>Benchmark Configuration</h2>
  <div class="kv">
    {% for k, v in suite.config.items() %}<div class="muted">{{ k }}</div><div>{{ v }}</div>{% endfor %}
  </div>

  <h2>Environment</h2>
  <div class="kv">
    <div class="muted">Python</div><div>{{ suite.environment.python_version }}</div>
    <div class="muted">OS</div><div>{{ suite.environment.os_platform }}</div>
    <div class="muted">CPU</div><div>{{ suite.environment.cpu_model }} ({{ suite.environment.cpu_count }} cores)</div>
    <div class="muted">Memory</div><div>{{ memory_gb }} GB</div>
    <div class="muted">RabbitMQ</div><div>{{ suite.environment.rabbitmq_version or "unknown" }}</div>
  </div>

  <h2>Executive Summary</h2>
  {% for row in exec_summary %}
    <div class="summary-card"><strong>{{ row.category }}:</strong> {{ row.winner }} &mdash; {{ row.detail }}</div>
  {% endfor %}

  <h2>Interactive Charts</h2>
  {% for name, div in chart_divs.items() %}
    <div class="chart"><h3>{{ name.replace('_', ' ').title() }}</h3>{{ div | safe }}</div>
  {% endfor %}

  {% if chart_pngs %}
  <h2>Static Charts</h2>
  {% for name, uri in chart_pngs.items() %}
    <div class="chart"><h3>{{ name.replace('_', ' ').title() }}</h3><img src="{{ uri }}" style="max-width:100%"></div>
  {% endfor %}
  {% endif %}

  <h2>Comparison Tables</h2>
  {% for bench, rows in tables.items() %}
    <h3>{{ bench.replace('_', ' ').title() }}</h3>
    <table>
      <tr><th>Client</th><th>Params</th><th>Median (ms)</th><th>P95 (ms)</th><th>P99 (ms)</th><th>Msgs/sec</th><th>Failed</th></tr>
      {% for r in rows %}
      <tr><td>{{ r.client }}</td><td>{{ r.params }}</td><td>{{ r.median }}</td><td>{{ r.p95 }}</td>
          <td>{{ r.p99 }}</td><td>{{ r.mps }}</td><td>{{ r.failed }}</td></tr>
      {% endfor %}
    </table>
  {% endfor %}

  <h2>Conclusions</h2>
  <ul>{% for c in conclusions %}<li>{{ c }}</li>{% endfor %}</ul>

  <h2>Appendix: Raw Statistics</h2>
  <table>
    <tr><th>Client</th><th>Benchmark</th><th>Params</th><th>Avg</th><th>Median</th><th>Min</th><th>Max</th><th>Stddev</th><th>P95</th><th>P99</th></tr>
    {% for r in appendix %}
    <tr><td>{{ r.client }}</td><td>{{ r.benchmark }}</td><td>{{ r.params }}</td><td>{{ r.avg }}</td>
        <td>{{ r.median }}</td><td>{{ r.min }}</td><td>{{ r.max }}</td><td>{{ r.stddev }}</td>
        <td>{{ r.p95 }}</td><td>{{ r.p99 }}</td></tr>
    {% endfor %}
  </table>
</body>
</html>
```

- [ ] **Step 4: Write `benchmark/reporting/report_generator.py`**

```python
"""HTML report rendering + pluggable PDF backend."""
from __future__ import annotations

import abc
import base64
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

from benchmark.reporting.charts import Charts
from benchmark.results import BenchmarkSuiteResult

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


class PdfBackend(abc.ABC):
    @abc.abstractmethod
    def available(self) -> bool: ...

    @abc.abstractmethod
    def render(self, html: str, out_path: str) -> bool: ...


class WeasyPrintBackend(PdfBackend):
    def available(self) -> bool:
        try:
            import weasyprint  # noqa: F401  (native libs load lazily)
            return True
        except Exception:
            return False

    def render(self, html: str, out_path: str) -> bool:
        try:
            import weasyprint
            weasyprint.HTML(string=html).write_pdf(out_path)
            return True
        except Exception as exc:
            print(f"[report] PDF generation skipped: {exc}")
            return False


def _ns_to_ms(ns: float) -> float:
    return round(ns / 1_000_000, 4)


def build_executive_summary(suite: BenchmarkSuiteResult) -> list[dict]:
    clients = []
    for r in suite.results:
        if r.client not in clients:
            clients.append(r.client)

    def median_for(client: str, benchmark: str) -> float:
        vals = [r.summary.median_ns for r in suite.results if r.client == client and r.benchmark == benchmark]
        return sum(vals) / len(vals) if vals else float("inf")

    def mps_for(client: str, benchmark: str) -> float:
        vals = [r.summary.messages_per_sec or 0.0
                for r in suite.results if r.client == client and r.benchmark == benchmark]
        return sum(vals) / len(vals) if vals else 0.0

    rows: list[dict] = []
    latency_cats = [("Publish latency", "publish_latency"), ("Consume latency", "consume_latency"),
                    ("Round-trip latency", "round_trip")]
    for label, bench in latency_cats:
        ranked = sorted(clients, key=lambda c: median_for(c, bench))
        best, worst = ranked[0], ranked[-1]
        b, w = median_for(best, bench), median_for(worst, bench)
        pct = ((w - b) / w * 100) if w else 0.0
        rows.append({"category": label, "winner": best,
                     "detail": f"{_ns_to_ms(b)} ms median, {pct:.1f}% faster than {worst}"})
    tp_cats = [("Publish throughput", "publish_throughput"), ("Consume throughput", "consume_throughput")]
    for label, bench in tp_cats:
        ranked = sorted(clients, key=lambda c: mps_for(c, bench), reverse=True)
        best, worst = ranked[0], ranked[-1]
        b, w = mps_for(best, bench), mps_for(worst, bench)
        pct = ((b - w) / w * 100) if w else 0.0
        rows.append({"category": label, "winner": best,
                     "detail": f"{b:,.0f} msgs/sec, {pct:.1f}% higher than {worst}"})
    return rows


def _tables(suite: BenchmarkSuiteResult) -> dict[str, list[dict]]:
    tables: dict[str, list[dict]] = {}
    for r in suite.results:
        tables.setdefault(r.benchmark, []).append({
            "client": r.client,
            "params": ", ".join(f"{k}={v}" for k, v in r.params.items()),
            "median": _ns_to_ms(r.summary.median_ns),
            "p95": _ns_to_ms(r.summary.p95_ns),
            "p99": _ns_to_ms(r.summary.p99_ns),
            "mps": f"{r.summary.messages_per_sec:,.0f}" if r.summary.messages_per_sec else "-",
            "failed": r.summary.n_failed,
        })
    return tables


def _appendix(suite: BenchmarkSuiteResult) -> list[dict]:
    rows: list[dict] = []
    for r in suite.results:
        rows.append({
            "client": r.client, "benchmark": r.benchmark,
            "params": ", ".join(f"{k}={v}" for k, v in r.params.items()),
            "avg": _ns_to_ms(r.summary.avg_ns), "median": _ns_to_ms(r.summary.median_ns),
            "min": _ns_to_ms(r.summary.min_ns), "max": _ns_to_ms(r.summary.max_ns),
            "stddev": _ns_to_ms(r.summary.stddev_ns),
            "p95": _ns_to_ms(r.summary.p95_ns), "p99": _ns_to_ms(r.summary.p99_ns),
        })
    return rows


def _conclusions(exec_summary: list[dict]) -> list[str]:
    return [f"{row['winner']} leads on {row['category'].lower()} ({row['detail']})." for row in exec_summary]


def generate_report(
    suite: BenchmarkSuiteResult, out_dir: str, *, pdf_backend: PdfBackend | None = None,
) -> dict[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    charts = Charts()
    figs = charts.build_all(suite)
    chart_divs = {name: charts.to_html_div(fig) for name, fig in figs.items()}
    chart_pngs: dict[str, str] = {}
    for name, fig in figs.items():
        png = charts.to_png_bytes(fig)
        if png is not None:
            chart_pngs[name] = "data:image/png;base64," + base64.b64encode(png).decode()

    exec_summary = build_executive_summary(suite)
    mem = suite.environment.total_memory_bytes
    env = Environment(loader=FileSystemLoader(_TEMPLATE_DIR), autoescape=select_autoescape(["html"]))
    template = env.get_template("report.html.j2")
    html = template.render(
        suite=suite,
        memory_gb=(round(mem / 1024**3, 1) if mem else "unknown"),
        exec_summary=exec_summary,
        chart_divs=chart_divs,
        chart_pngs=chart_pngs,
        tables=_tables(suite),
        appendix=_appendix(suite),
        conclusions=_conclusions(exec_summary),
    )

    html_path = os.path.join(out_dir, "report.html")
    with open(html_path, "w", encoding="utf-8") as fh:
        fh.write(html)

    pdf_path = ""
    backend = pdf_backend if pdf_backend is not None else WeasyPrintBackend()
    if backend.available():
        candidate = os.path.join(out_dir, "report.pdf")
        if backend.render(html, candidate):
            pdf_path = candidate
    else:
        print("[report] WeasyPrint unavailable; wrote HTML only.")

    return {"html": html_path, "pdf": pdf_path}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_report_generator.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add benchmark/reporting/report_generator.py benchmark/reporting/templates/report.html.j2 tests/test_report_generator.py
git commit -m "feat: add report generator with pluggable PDF backend"
```

---

### Task 16: CLI entry point (`main.py`)

**Files:**
- Create: `benchmark/main.py`
- Test: `tests/test_main.py`

**Interfaces:**
- Consumes: `BenchmarkConfig`, `run_suite`, `save_json`, `save_csv`, `generate_report`.
- Produces:
  - `def parse_args(argv: list[str]) -> argparse.Namespace` — flags: `--config`, `--amqp-url`, `--message-count`, `--iterations`, `--clients` (comma-sep), `--output-dir`, `--no-report`.
  - `async def async_main(argv: list[str]) -> str` — load config (with CLI overrides), run suite, write JSON+CSV into `output_dir/<timestamp>/`, generate report unless `--no-report`, return the run directory path.
  - `def main() -> None` — `asyncio.run(async_main(sys.argv[1:]))`.

- [ ] **Step 1: Write the failing test**

```python
from benchmark.main import parse_args, async_main


def test_parse_args_overrides():
    ns = parse_args(["--message-count", "500", "--clients", "fake", "--amqp-url", "amqp://h/"])
    assert ns.message_count == 500
    assert ns.clients == "fake"
    assert ns.amqp_url == "amqp://h/"


async def test_async_main_end_to_end_with_fake(tmp_path):
    run_dir = await async_main([
        "--clients", "fake", "--message-count", "50", "--iterations", "2",
        "--output-dir", str(tmp_path),
    ])
    import os
    assert os.path.exists(os.path.join(run_dir, "results.json"))
    assert os.path.exists(os.path.join(run_dir, "results.csv"))
    assert os.path.exists(os.path.join(run_dir, "report.html"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_main.py -v`
Expected: FAIL (cannot import).

- [ ] **Step 3: Write minimal implementation**

```python
"""CLI entry point: run the suite, persist results, generate the report."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

from benchmark.config import BenchmarkConfig
from benchmark.reporting.report_generator import generate_report
from benchmark.results import save_csv, save_json
from benchmark.runner import run_suite


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark pika vs aio-pika against RabbitMQ.")
    p.add_argument("--config")
    p.add_argument("--amqp-url")
    p.add_argument("--message-count", type=int)
    p.add_argument("--iterations", type=int)
    p.add_argument("--clients", help="comma-separated client names")
    p.add_argument("--output-dir")
    p.add_argument("--no-report", action="store_true")
    return p.parse_args(argv)


async def async_main(argv: list[str]) -> str:
    ns = parse_args(argv)
    overrides = {
        "amqp_url": ns.amqp_url,
        "message_count": ns.message_count,
        "iterations": ns.iterations,
        "clients": ns.clients.split(",") if ns.clients else None,
        "output_dir": ns.output_dir,
    }
    config = BenchmarkConfig.load(ns.config, overrides={k: v for k, v in overrides.items() if v is not None})

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(config.output_dir, stamp)
    os.makedirs(run_dir, exist_ok=True)

    print(f"Running benchmarks for {config.clients} ...")
    suite = await run_suite(config)

    save_json(suite, os.path.join(run_dir, "results.json"))
    save_csv(suite, os.path.join(run_dir, "results.csv"))
    print(f"Raw results written to {run_dir}")

    if not ns.no_report:
        paths = generate_report(suite, run_dir)
        print(f"Report: {paths['html']}" + (f" / {paths['pdf']}" if paths["pdf"] else " (HTML only)"))

    return run_dir


def main() -> None:
    asyncio.run(async_main(sys.argv[1:]))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_main.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Full test-suite gate**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add benchmark/main.py tests/test_main.py
git commit -m "feat: add CLI entry point and end-to-end fake run"
```

---

### Task 17: README and live smoke verification

**Files:**
- Create: `README.md`
- Test: manual live run (broker required)

**Interfaces:** none (documentation + verification).

- [ ] **Step 1: Write `README.md`**

Document: purpose; install (`pip install -r requirements.txt`); how to start a RabbitMQ (`docker run -p 5672:5672 -p 15672:15672 rabbitmq:3-management` or a local install); quick fake run (`python -m benchmark.main --clients fake --message-count 100`); full run (`python -m benchmark.main`); config options table; output layout (`results/<timestamp>/results.json|csv|report.html|report.pdf`); note that PDF requires WeasyPrint native libs and gracefully degrades to HTML-only; note the async-pipelining and threads-vs-coroutines fairness caveats from the spec.

- [ ] **Step 2: Fake end-to-end run (no broker)**

Run: `.venv/Scripts/python -m benchmark.main --clients fake --message-count 200 --iterations 3 --output-dir results`
Expected: prints run dir; `results/<stamp>/report.html` exists and opens in a browser.

- [ ] **Step 3: Live smoke run (requires local RabbitMQ)**

Precondition: RabbitMQ reachable at `amqp://guest:guest@localhost:5672/`.
Run: `.venv/Scripts/python -m benchmark.main --clients pika,aio-pika --message-count 2000 --iterations 3 --output-dir results`
Expected: completes without unhandled exceptions; both clients present in `results.json`; `report.html` shows non-zero msgs/sec for both; `n_failed` is 0 for the core benchmarks. If WeasyPrint native libs are installed, `report.pdf` is also produced; otherwise the console prints the HTML-only notice.

- [ ] **Step 4: Verify results integrity**

Run: `.venv/Scripts/python -c "from benchmark.results import load_json; import glob; p=sorted(glob.glob('results/*/results.json'))[-1]; s=load_json(p); print({r.client for r in s.results}, len(s.results))"`
Expected: prints `{'pika', 'aio-pika'}` and a result count > 20.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: add README and verify live smoke run"
```

---

## Self-Review

**Spec coverage:**
- 7 benchmarks → Tasks 9–12. ✓
- Config (URL, queue/exchange/routing, count, sizes, iterations, warmup, concurrency) → Task 3. ✓
- Warm-up, repeats, `perf_counter_ns`, pre-generated payloads, failure recording → Task 8 harness + benchmarks. ✓
- Stats (7-stat block; throughput msgs/sec + duration) → Task 2, used everywhere. ✓
- JSON + CSV with raw iteration data → Task 4. ✓
- Environment info (Python/OS/CPU/memory/RabbitMQ version) → Task 4 `collect_environment`. ✓
- Report HTML + PDF, pluggable/graceful → Task 15. ✓
- All required charts (latency grouped bar, throughput horizontal bar, concurrent publish/consume lines, scaling efficiency, distribution box plots) → Task 14. ✓
- Report contents (title, config, environment, exec summary, results, interactive charts, static charts, comparison tables, conclusions, appendix) → Task 15 template. ✓
- Extensibility (add a client = new impl; reporting reads only schema) → Task 5 interface + Task 13 factory + Task 15 schema-only reporting. ✓
- Project structure matches spec (`clients/`, `benchmarks/`, `reporting/`, `statistics.py`, `config.py`, `runner.py`, `results.py`, `main.py`). ✓ (adds `harness.py` and `clients/fake_client.py` as internal support — noted deviations, justified for testability.)

**Placeholder scan:** No TBD/TODO; every code step has complete code. ✓

**Type consistency:** `summarize` signature consistent across Tasks 2/8/10/12. `BenchmarkResult`/`IterationSample`/`StatSummary` field names consistent across Tasks 4/8/14/15. `run(client, config) -> list[BenchmarkResult]` uniform across all benchmark modules and consumed identically in Task 13. `Charts` method names match Task 14 definitions and Task 15 usage. `scaling_efficiency` defined in Task 13, consumed in Task 14. ✓

**Deviations from spec structure (intentional):**
- Added `benchmark/harness.py` — shared timing loop, keeps benchmarks DRY.
- Added `benchmark/clients/fake_client.py` — enables broker-free unit tests.
Both are additive and don't change the specified public structure.
