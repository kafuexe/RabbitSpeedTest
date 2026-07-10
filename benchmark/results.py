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
