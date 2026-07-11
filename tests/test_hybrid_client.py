import asyncio
from types import SimpleNamespace

from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.hybrid_client import HybridClient
from benchmark.config import BenchmarkConfig
from benchmark.runner import build_client
from tests.helpers import RecordingFakeClient, assert_client_methods_are_coroutines


def test_hybrid_client_is_benchmark_client():
    c = HybridClient("amqp://x/")
    assert issubclass(HybridClient, BenchmarkClient)
    assert c.name == "hybrid"


def test_hybrid_methods_are_coroutines():
    assert_client_methods_are_coroutines(HybridClient("amqp://x/"))


def test_hybrid_wires_aio_publisher_and_async_consumer():
    c = HybridClient("amqp://x/", prefetch=7, publisher_confirms=False, durable=True)
    assert isinstance(c._publisher, AioPikaClient)
    assert c._publisher._confirms is False and c._publisher._durable is True
    # Consume side is a raw aiormq channel (async, no thread), opened on connect.
    assert c._consume_conn is None and c._consume_ch is None
    # Ack batch must stay below prefetch or the broker stops delivering.
    assert c._ack_batch == 3  # max(1, 7 // 2)


def test_hybrid_bakes_tuned_defaults():
    # Measured sweet spots (2026-07-11 grid): consume flattens past
    # prefetch=1000/batch=500; publish pipeline peaks around 1000.
    c = HybridClient("amqp://x/")
    assert c._prefetch == 1000
    assert c._ack_batch == 500
    assert c._publisher._pipeline_batch == 1000


def test_hybrid_clone_is_fresh_pair():
    c = HybridClient("amqp://x/", publisher_confirms=False, durable=True)
    d = c.clone()
    assert d is not c and isinstance(d, HybridClient)
    assert d._publisher is not c._publisher
    assert d._publisher._confirms is False and d._publisher._durable is True
    assert d._prefetch == c._prefetch


async def test_hybrid_routes_publish_and_gets_to_publisher():
    # publish_many, consume_one and consume_many_get all ride the aio-pika
    # connection: same-channel ordering fixes the round-trip race, and
    # aio-pika's get correctly returns None on an empty queue (raw aiormq
    # basic_get returns a GetEmpty message instead — the phantom-b'' bug).
    c = HybridClient("amqp://x/")
    pub = RecordingFakeClient()
    c._publisher = pub
    await c.publish_many("", "q", [b"a", b"b"], confirm=True)
    assert await c.consume_one("q") == b"a"
    assert await c.consume_many_get("q", 1) == 1
    assert await c.consume_one("q") is None  # empty -> None, not b""
    # consume_many_get's base default loops consume_one, hence the nesting.
    assert [m for m, _ in pub.calls] == [
        "publish_many", "consume_one", "consume_many_get", "consume_one", "consume_one"]


class _Delivery(SimpleNamespace):
    pass


def _msg(tag: int) -> _Delivery:
    return _Delivery(delivery=SimpleNamespace(delivery_tag=tag), body=b"x")


class _ConcurrentStubChannel:
    """Delivers every message as its own overlapping task — exactly aiormq's
    task-per-delivery model, which the old sequential stub failed to simulate.

    Records acks/nacks and, at every ack, snapshots which tags had finished
    their handler so tests can assert nothing is acked before it completed.
    """

    def __init__(self, deliver: int):
        self.deliver = deliver
        self.handled: set[int] = set()   # tags whose handler finished (test-updated)
        self.acks: list[tuple[int, bool, frozenset]] = []
        self.nacks: list[tuple[int, bool, bool]] = []
        self.cancelled = False
        self.tasks: list[asyncio.Task] = []

    async def basic_consume(self, queue, cb, no_ack=False):
        self.tasks = [asyncio.create_task(cb(_msg(tag)))
                      for tag in range(1, self.deliver + 1)]
        return SimpleNamespace(consumer_tag="ctag-test")

    async def basic_ack(self, delivery_tag, multiple=False):
        self.acks.append((delivery_tag, multiple, frozenset(self.handled)))

    async def basic_nack(self, delivery_tag, multiple=False, requeue=True):
        self.nacks.append((delivery_tag, multiple, requeue))

    async def basic_cancel(self, consumer_tag):
        self.cancelled = True


def _stubbed(deliver: int, ack_batch: int = 50) -> tuple[HybridClient, _ConcurrentStubChannel]:
    c = HybridClient("amqp://x/", prefetch=ack_batch * 2)
    ch = _ConcurrentStubChannel(deliver)
    c._consume_ch = ch
    c._inactivity = 0.05
    c._ack_flush_delay = 0.01
    return c, ch


def _assert_acks_sound(ch: _ConcurrentStubChannel, nacked: set[int] = frozenset(),
                       check_completion: bool = True):
    """Every multiple-ack must only cover tags whose handler completed (or
    that were individually nacked), and ack tags must be strictly increasing.
    check_completion=False for handler-less drains, where nothing tracks
    ch.handled (there is no handler to complete)."""
    prev = 0
    for tag, multiple, handled_at_ack in ch.acks:
        assert multiple is True
        assert tag > prev, f"ack tags regressed: {ch.acks}"
        prev = tag
        if check_completion:
            owed = set(range(1, tag + 1)) - nacked
            assert owed <= handled_at_ack, (
                f"acked up to {tag} but only {sorted(handled_at_ack)} had completed")


async def test_hybrid_consume_many_acks_in_batches():
    c, ch = _stubbed(deliver=120)
    assert await c.consume_many("q", 120) == 120
    assert ch.acks and ch.acks[-1][0] == 120
    _assert_acks_sound(ch, check_completion=False)
    assert ch.cancelled and ch.nacks == []


async def test_hybrid_consume_many_returns_short_on_dry_queue():
    c, ch = _stubbed(deliver=73)
    assert await c.consume_many("q", 120) == 73
    assert ch.acks[-1][0] == 73  # the mid-batch remainder is acked on exit
    _assert_acks_sound(ch, check_completion=False)
    assert ch.nacks == []


async def test_hybrid_consume_quota_reserved_before_handler_await():
    # 100 deliveries land as concurrent tasks while every handler is parked on
    # an await; only `count` slots may be reserved — extras are nack-requeued,
    # never processed or acked.
    c, ch = _stubbed(deliver=100)
    started: list[int] = []
    gate = asyncio.Event()

    async def handler(body: bytes) -> None:
        started.append(1)
        await gate.wait()

    task = asyncio.create_task(c.consume("q", handler, count=10))
    await asyncio.sleep(0.02)   # let all 100 delivery tasks hit the quota check
    assert len(started) == 10, f"{len(started)} handlers ran; quota was 10"
    for _ in started:
        ch.handled.add(len(ch.handled) + 1)
    gate.set()
    assert await task == 10
    assert ch.acks and ch.acks[-1][0] == 10
    # The 90 extras are requeued for other workers in one multiple nack.
    assert (100, True, True) in ch.nacks


async def test_hybrid_consume_never_acks_unfinished_handlers():
    # Tag 1 is the slowest handler; later tags finish first. No ack may cover
    # tag 1 until its handler completed — the frontier must hold acks back.
    c, ch = _stubbed(deliver=60, ack_batch=10)
    seen = 0

    async def tracking_handler(body: bytes) -> None:
        nonlocal seen
        seen += 1
        me = seen
        if me == 1:
            await asyncio.sleep(0.05)   # first delivery finishes LAST
        ch.handled.add(me)

    assert await c.consume("q", tracking_handler, count=60) == 60
    _assert_acks_sound(ch)
    assert ch.acks[-1][0] == 60


async def test_hybrid_consume_failing_handler_is_nacked_not_lost():
    c, ch = _stubbed(deliver=5, ack_batch=2)
    seen = 0

    async def handler(body: bytes) -> None:
        nonlocal seen
        seen += 1
        me = seen
        if me == 3:
            raise RuntimeError("poison message")
        ch.handled.add(me)

    got = await c.consume("q", handler, count=5)
    assert got == 4                      # the poison message is not counted
    assert (3, False, True) in ch.nacks  # ...it is individually requeued
    assert ch.acks[-1][0] == 5           # later successes are still acked
    _assert_acks_sound(ch, nacked={3})


async def test_hybrid_consume_forever_flushes_acks_when_idle():
    c, ch = _stubbed(deliver=3)  # far below the ack batch of 50

    async def handler(body: bytes) -> None:
        ch.handled.add(len(ch.handled) + 1)

    task = asyncio.create_task(c.consume("q", handler))  # no count: runs until cancelled
    await asyncio.sleep(0.1)
    # Idle flush (the acker's timeout) must ack processed messages
    # instead of sitting on a partial batch.
    assert ch.acks and ch.acks[-1][0] == 3
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert ch.cancelled
    _assert_acks_sound(ch)


def test_build_client_knows_hybrid():
    cfg = BenchmarkConfig.default()
    cfg.publisher_confirms = False
    c = build_client("hybrid", cfg)
    assert isinstance(c, HybridClient)
    assert c._publisher._confirms is False
    assert c._prefetch == 1000  # config leaves prefetch unset -> tuned default


def test_build_client_prefetch_applies_uniformly():
    # No silent special case: when the config sets prefetch, every client
    # gets it — including the hybrid — so results.json labels are truthful.
    cfg = BenchmarkConfig.default()
    cfg.prefetch = 500
    assert build_client("hybrid", cfg)._prefetch == 500
    assert build_client("pika", cfg)._prefetch == 500
    assert build_client("aio-pika", cfg)._prefetch == 500