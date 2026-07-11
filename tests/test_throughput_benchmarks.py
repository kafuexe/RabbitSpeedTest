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
