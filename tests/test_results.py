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
