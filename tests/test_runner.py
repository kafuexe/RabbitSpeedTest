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


async def test_run_suite_progress_output(capsys):
    await run_suite(_cfg(), client_factory=_factory, show_progress=True)
    out = capsys.readouterr().out
    assert "stages" in out          # suite header
    assert "eta" in out             # per-stage ETA
    assert out.count("done") >= 7   # one line per benchmark stage


async def test_run_suite_progress_silent_by_default(capsys):
    await run_suite(_cfg(), client_factory=_factory)
    assert capsys.readouterr().out == ""
