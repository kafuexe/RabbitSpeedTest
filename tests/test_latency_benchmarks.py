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
