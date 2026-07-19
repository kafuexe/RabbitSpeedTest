import asyncio

from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.rabbit_client_bench import RabbitClientBench
from benchmark.config import BenchmarkConfig
from benchmark.runner import build_client
from hs_rabbit_client import RabbitClient
from tests.helpers import assert_client_methods_are_coroutines


def test_rabbit_client_bench_is_benchmark_client():
    c = RabbitClientBench("amqp://x/")
    assert issubclass(RabbitClientBench, BenchmarkClient)
    assert c.name == "simple"


def test_rabbit_client_bench_methods_are_coroutines():
    assert_client_methods_are_coroutines(RabbitClientBench("amqp://x/"))


def test_rabbit_client_bench_composition_and_clone():
    c = RabbitClientBench("amqp://x/", prefetch=7, durable=True)
    assert isinstance(c._sr, RabbitClient)
    assert isinstance(c._admin, AioPikaClient)
    d = c.clone()
    assert d is not c and d._sr is not c._sr


def test_build_client_knows_simple():
    c = build_client("simple", BenchmarkConfig.default())
    assert isinstance(c, RabbitClientBench)


class _FakeConsumer:
    """Mirrors hs_rabbit_client.Consumer: cancel() is idempotent, wait() parks
    until cancelled."""

    def __init__(self, task):
        self._task = task

    async def cancel(self):
        self._task.cancel()
        await asyncio.wait([self._task])

    async def wait(self):
        await asyncio.wait([self._task])
        if not self._task.cancelled() and self._task.exception() is not None:
            raise self._task.exception()


class _FakeSR:
    """Feeds queued bodies to the consume handler; a raising handler requeues,
    mirroring RabbitClient's nack-requeue semantics. consume() returns a
    Consumer-like handle (the 0.2.0 API) instead of parking."""

    def __init__(self, bodies):
        self.bodies = list(bodies)

    async def consume(self, queue, handler):
        async def _run():
            while self.bodies:
                body = self.bodies.pop(0)
                try:
                    await handler(body)
                except Exception:
                    self.bodies.append(body)
                await asyncio.sleep(0)
            await asyncio.Future()  # like the real consumer: run until cancelled

        return _FakeConsumer(asyncio.create_task(_run()))


async def test_simple_consume_many_stops_exactly_at_quota():
    c = RabbitClientBench("amqp://x/")
    c._sr = _FakeSR([b"m"] * 5)
    got = await c.consume_many("q", 3)
    assert got == 3
    # The two extras were requeued for other workers, not consumed.
    assert len(c._sr.bodies) == 2


async def test_simple_consume_many_returns_short_on_dry_queue():
    c = RabbitClientBench("amqp://x/")
    c._sr = _FakeSR([b"m"] * 4)
    c._inactivity = 0.05
    assert await c.consume_many("q", 10) == 4
