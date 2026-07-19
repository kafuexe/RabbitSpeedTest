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
