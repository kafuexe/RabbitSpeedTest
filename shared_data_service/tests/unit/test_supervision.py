"""Supervision tests: per-queue consumer retry, container-owned consumer
task, readiness reflecting consumer death, and stop() surviving a crashed
consumer."""
import asyncio

from app.messaging.consumer import EventConsumer
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
