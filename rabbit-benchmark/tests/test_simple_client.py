import asyncio

from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.simple_client import SimpleRabbitClient
from benchmark.config import BenchmarkConfig
from benchmark.runner import build_client
from rabbit_client import RabbitClient
from tests.helpers import assert_client_methods_are_coroutines


def test_simple_client_is_benchmark_client():
    c = SimpleRabbitClient("amqp://x/")
    assert issubclass(SimpleRabbitClient, BenchmarkClient)
    assert c.name == "simple"


def test_simple_client_methods_are_coroutines():
    assert_client_methods_are_coroutines(SimpleRabbitClient("amqp://x/"))


def test_simple_client_composition_and_clone():
    c = SimpleRabbitClient("amqp://x/", prefetch=7, durable=True)
    assert isinstance(c._sr, RabbitClient)
    assert isinstance(c._admin, AioPikaClient)
    d = c.clone()
    assert d is not c and d._sr is not c._sr


def test_build_client_knows_simple():
    c = build_client("simple", BenchmarkConfig.default())
    assert isinstance(c, SimpleRabbitClient)


class _FakeSR:
    """Feeds queued bodies to the consume handler; a raising handler requeues,
    mirroring RabbitClient's nack-requeue semantics."""

    def __init__(self, bodies):
        self.bodies = list(bodies)

    async def consume(self, queue, handler):
        while self.bodies:
            body = self.bodies.pop(0)
            try:
                await handler(body)
            except Exception:
                self.bodies.append(body)
            await asyncio.sleep(0)
        await asyncio.Future()  # like the real consume: park until cancelled


async def test_simple_consume_many_stops_exactly_at_quota():
    c = SimpleRabbitClient("amqp://x/")
    c._sr = _FakeSR([b"m"] * 5)
    got = await c.consume_many("q", 3)
    assert got == 3
    # The two extras were requeued for other workers, not consumed.
    assert len(c._sr.bodies) == 2


async def test_simple_consume_many_returns_short_on_dry_queue():
    c = SimpleRabbitClient("amqp://x/")
    c._sr = _FakeSR([b"m"] * 4)
    c._inactivity = 0.05
    assert await c.consume_many("q", 10) == 4