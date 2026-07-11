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


class _UnderDrainingFake(FakeClient):
    """consume_many always drains one fewer than requested (no exception).

    Mimics a real client that can return consumed < count on a delivery race;
    the benchmark must record that as a failed iteration, not a success.
    """

    async def consume_many(self, queue: str, count: int) -> int:
        return await super().consume_many(queue, max(count - 1, 0))


async def test_publish_throughput_has_msgs_per_sec():
    client = FakeClient(); await client.connect()
    results = await publish_throughput.run(client, _cfg())
    assert len(results) == 1
    r = results[0]
    assert r.benchmark == "publish_throughput"
    assert r.summary.messages_per_sec and r.summary.messages_per_sec > 0


async def test_publish_throughput_purges_between_runs():
    client = FakeClient(); await client.connect()
    cfg = _cfg()
    await publish_throughput.run(client, cfg)
    # Purge runs as setup before each publish, so the queue holds at most one
    # run's worth of messages rather than growing across warm-up + iterations.
    assert len(client._queues[cfg.queue_name]) == cfg.message_count


async def test_consume_throughput_drains_all():
    client = FakeClient(); await client.connect()
    results = await consume_throughput.run(client, _cfg())
    r = results[0]
    assert r.benchmark == "consume_throughput"
    assert r.summary.messages_per_sec and r.summary.messages_per_sec > 0
    assert r.summary.n_failed == 0


async def test_consume_throughput_flags_underdrain():
    client = _UnderDrainingFake(); await client.connect()
    cfg = _cfg()
    results = await consume_throughput.run(client, cfg)
    r = results[0]
    # Every measured iteration under-drains -> all recorded as failures, none success.
    assert r.summary.n_failed == cfg.iterations
    assert r.summary.n_success == 0
