from benchmark.config import BenchmarkConfig
from benchmark.clients.fake_client import FakeClient
from benchmark.runner import run_suite, scaling_efficiency, build_client, BENCHMARKS


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


async def test_run_suite_covers_eight_benchmarks():
    suite = await run_suite(_cfg(), client_factory=_factory)
    names = {r.benchmark for r in suite.results}
    assert names == {
        "publish_latency", "consume_latency", "publish_throughput",
        "consume_throughput", "consume_throughput_get", "round_trip",
        "concurrent_publish", "concurrent_consume",
    }
    assert suite.environment.python_version.startswith("3.")
    assert suite.timestamp


async def test_scaling_efficiency():
    suite = await run_suite(_cfg(), client_factory=_factory)
    eff = scaling_efficiency(suite.results, "concurrent_publish", "fake")
    assert eff[1] == 1.0
    assert set(eff) == {1, 2}


def test_benchmarks_registry_has_eight():
    assert len(BENCHMARKS) == 8


def test_build_client_wires_confirms_and_durable():
    cfg = BenchmarkConfig.default()
    cfg.publisher_confirms = False
    cfg.durable = True
    for name in ("pika", "aio-pika", "hybrid"):
        c = build_client(name, cfg)
        assert c._confirms is False, name
        assert c._durable is True, name


class _FailingDeleteFake(FakeClient):
    def __init__(self):
        super().__init__()
        self.closed = False

    async def delete_queue(self, name):
        raise RuntimeError("RESOURCE_LOCKED")

    async def close(self):
        self.closed = True


async def test_run_suite_closes_client_when_queue_delete_fails():
    # The pre-benchmark queue delete must not leak the connection on failure.
    client = _FailingDeleteFake()
    try:
        await run_suite(_cfg(), client_factory=lambda name, config: client)
    except RuntimeError:
        pass
    assert client.closed


async def test_run_suite_progress_output(capsys):
    await run_suite(_cfg(), client_factory=_factory, show_progress=True)
    out = capsys.readouterr().out
    assert "stages" in out          # suite header
    assert "eta" in out             # per-stage ETA
    assert out.count("done") >= 7   # one line per benchmark stage


async def test_run_suite_progress_silent_by_default(capsys):
    await run_suite(_cfg(), client_factory=_factory)
    assert capsys.readouterr().out == ""
