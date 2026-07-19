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


class _UnderDrainingFake(FakeClient):
    """Each worker silently drains one message fewer than requested."""

    async def consume_many(self, queue: str, count: int) -> int:
        return await super().consume_many(queue, max(count - 1, 0))


async def test_concurrent_consume_flags_underdrain():
    client = _UnderDrainingFake(); await client.connect()
    cfg = _cfg()
    results = await concurrent_consume.run(client, cfg)
    for r in results:
        # Every iteration under-drains -> recorded as failure, not silent success.
        assert r.summary.n_failed == cfg.iterations, r.params
        assert r.summary.n_success == 0, r.params


class _CloneTrackingFake(FakeClient):
    """Fake whose clones share the queue store and log lifecycle calls."""

    def __init__(self, store=None, log=None):
        super().__init__()
        if store is not None:
            self._queues = store
        self.log = log if log is not None else []

    async def connect(self):
        self.log.append("connected")

    async def close(self):
        self.log.append("closed")

    def clone(self):
        self.log.append("cloned")
        return _CloneTrackingFake(self._queues, self.log)


async def test_concurrent_consume_uses_fresh_client_per_worker():
    client = _CloneTrackingFake(); await client.connect()
    cfg = _cfg()
    await concurrent_consume.run(client, cfg)
    # One clone per worker per level, reused across warmup + iterations —
    # reconnecting every iteration would dominate the wall clock.
    expected = sum(cfg.concurrency_levels)
    assert client.log.count("cloned") == expected
    assert client.log.count("connected") == expected + 1  # +1 for the test's own connect
    assert client.log.count("closed") == expected


async def test_concurrent_publish_uses_fresh_client_per_worker():
    client = _CloneTrackingFake(); await client.connect()
    cfg = _cfg()
    await concurrent_publish.run(client, cfg)
    expected = sum(cfg.concurrency_levels)
    assert client.log.count("cloned") == expected
    assert client.log.count("closed") == expected
