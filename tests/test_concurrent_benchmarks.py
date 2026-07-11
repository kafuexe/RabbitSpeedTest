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
