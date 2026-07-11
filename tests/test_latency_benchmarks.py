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


class _CountingFake(FakeClient):
    """FakeClient that records whether each consume returned a real message.

    FakeClient.consume_one returns None (never raises) on an empty queue, so
    n_failed alone cannot tell a real consume from an underflow. Counting real
    vs empty consumes gives the test genuine signal about the preload.
    """

    def __init__(self) -> None:
        super().__init__()
        self.real_consumes = 0
        self.empty_consumes = 0

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        msg = await super().consume_one(queue, timeout)
        if msg is None:
            self.empty_consumes += 1
        else:
            self.real_consumes += 1
        return msg


async def test_consume_latency_measures_after_preload():
    client = _CountingFake(); await client.connect()
    cfg = _cfg()
    results = await consume_latency.run(client, cfg)
    assert all(r.benchmark == "consume_latency" for r in results)
    assert all(r.summary.n_success == 10 for r in results)
    assert all(r.summary.n_failed == 0 for r in results)
    # Every timed consume must pull a real preloaded message — never an empty
    # queue. This catches a preload-count regression that n_failed can't see.
    assert client.empty_consumes == 0
    per_size = cfg.warmup_iterations + cfg.latency_sample_count
    assert client.real_consumes == per_size * len(cfg.message_sizes)
