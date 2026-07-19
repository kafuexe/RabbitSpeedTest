"""Supervision tests: per-queue consumer retry (including broker-side
consumer cancellation), container-owned consumer task, readiness reflecting
consumer death, and stop() surviving a crashed consumer."""
import asyncio
import logging

from app.messaging.consumer import EventConsumer
from app.messaging.rabbit_client_adapter import ConsumerCancelledError
from app.messaging.registry import EventHandlerRegistry


class _FlakyBus:
    """consume() fails for a queue a fixed number of times, then parks."""

    def __init__(self, failures: dict[str, int]) -> None:
        self.failures = dict(failures)
        self.attempts: dict[str, int] = {}
        self.consuming: set[str] = set()

    async def consume(self, queue: str, handler) -> None:
        self.attempts[queue] = self.attempts.get(queue, 0) + 1
        if self.failures.get(queue, 0) > 0:
            self.failures[queue] -= 1
            raise RuntimeError(f"declare failed for {queue}")
        self.consuming.add(queue)
        try:
            await asyncio.Future()  # park like the real consume
        finally:
            self.consuming.discard(queue)


async def test_failing_queue_is_retried_without_touching_siblings():
    bus = _FlakyBus({"bad": 2})
    consumer = EventConsumer(bus, EventHandlerRegistry(), ["bad", "good"],
                             retry_delay=0.01)
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.1)
    # 'good' consumed on the first attempt and stayed up throughout;
    # 'bad' was retried until it recovered — never silently dead.
    assert "good" in bus.consuming and "bad" in bus.consuming
    assert bus.attempts["bad"] == 3 and bus.attempts["good"] == 1
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert not bus.consuming  # cancellation reached every queue task


async def test_clean_consumer_exit_is_still_loud(caplog):
    # run() parks forever on a real bus, so ANY uncancelled completion —
    # including a clean return — means nothing is being consumed. Lives in
    # the unit suite: it needs no broker/DB and must run everywhere.
    from app.bootstrap.container import Container

    done_task = asyncio.create_task(asyncio.sleep(0))
    await done_task
    with caplog.at_level("CRITICAL"):
        Container._on_consumer_done(done_task)
    assert any("no events are being consumed" in r.message for r in caplog.records)


class _CancelledByBrokerBus:
    """consume() raises ConsumerCancelledError a fixed number of times per
    queue (broker-side cancel: queue deleted/recreated), then parks like the
    real consume."""

    def __init__(self, cancels: dict) -> None:
        self.cancels = dict(cancels)
        self.attempts: dict[str, int] = {}
        self.consuming: set[str] = set()

    async def consume(self, queue: str, handler) -> None:
        self.attempts[queue] = self.attempts.get(queue, 0) + 1
        if self.cancels.get(queue, 0) > 0:
            self.cancels[queue] -= 1
            raise ConsumerCancelledError(
                f"consumer for queue {queue!r} was cancelled by the broker"
            )
        self.consuming.add(queue)
        try:
            await asyncio.Future()  # park like the real consume
        finally:
            self.consuming.discard(queue)


async def test_broker_cancel_is_warned_and_consumption_resumes(caplog):
    # Repeated broker cancels keep retrying (the client re-declares the queue
    # on each retry), and the log is a WARNING — an expected operational
    # event, never an error/critical page.
    bus = _CancelledByBrokerBus({"jobs": 2})
    consumer = EventConsumer(bus, EventHandlerRegistry(), ["jobs"],
                             retry_delay=0.01)
    with caplog.at_level(logging.WARNING):
        task = asyncio.create_task(consumer.run())
        await asyncio.sleep(0.1)
    assert "jobs" in bus.consuming and bus.attempts["jobs"] == 3
    cancels = [r for r in caplog.records if "cancelled by broker" in r.message]
    assert len(cancels) == 2
    assert all(r.levelno == logging.WARNING for r in cancels)
    assert all(r.queue == "jobs" for r in cancels)  # queue name in the log
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_broker_cancel_restart_waits_full_backoff(monkeypatch):
    # The restart happens after the backoff, never in a hot loop: every
    # cancel is followed by one sleep of exactly retry_delay.
    real_sleep = asyncio.sleep
    slept: list[float] = []

    async def recording_sleep(delay: float) -> None:
        slept.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", recording_sleep)
    bus = _CancelledByBrokerBus({"jobs": 2})
    consumer = EventConsumer(bus, EventHandlerRegistry(), ["jobs"],
                             retry_delay=1.0)
    task = asyncio.create_task(consumer.run())
    await real_sleep(0.05)
    assert "jobs" in bus.consuming and bus.attempts["jobs"] == 3
    assert slept.count(1.0) == 2  # one full backoff per cancel
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_other_exceptions_keep_the_generic_error_path(caplog):
    # A non-cancel failure must NOT take the broker-cancel warning path:
    # it keeps the existing generic behavior (ERROR-level exception log,
    # retry after retry_delay) exactly as before.
    bus = _FlakyBus({"bad": 1})
    consumer = EventConsumer(bus, EventHandlerRegistry(), ["bad"],
                             retry_delay=0.01)
    with caplog.at_level(logging.WARNING):
        task = asyncio.create_task(consumer.run())
        await asyncio.sleep(0.1)
    assert "bad" in bus.consuming and bus.attempts["bad"] == 2
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "consume failed" in errors[0].message
    assert not [r for r in caplog.records if "cancelled by broker" in r.message]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_cancellation_during_cancel_backoff_exits_promptly():
    # Shutdown must not wait out the backoff: cancelling the run task while
    # a queue task sleeps between broker-cancel retries exits immediately.
    bus = _CancelledByBrokerBus({"jobs": 1_000_000})  # every attempt cancels
    consumer = EventConsumer(bus, EventHandlerRegistry(), ["jobs"],
                             retry_delay=60.0)
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.05)  # first attempt raised; now parked in backoff
    assert bus.attempts["jobs"] == 1
    loop = asyncio.get_running_loop()
    start = loop.time()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert loop.time() - start < 1.0  # exited the 60s backoff at once
    assert task.cancelled()


async def test_run_cancellation_stops_all_quedone_tasks():
    bus = _FlakyBus({})
    consumer = EventConsumer(bus, EventHandlerRegistry(), ["a", "b", "c"],
                             retry_delay=0.01)
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.05)
    assert bus.consuming == {"a", "b", "c"}
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert bus.consuming == set()
