"""Container lifecycle: supervised consumer task, readiness truthfulness,
restart-safety, and shutdown that survives a crashed consumer."""
import asyncio

import pytest

from app.bootstrap.container import Container
from tests.integration.conftest import make_settings, requires_pg, requires_rabbit

pytestmark = [requires_pg, requires_rabbit]


@pytest.fixture
async def both_container(make_container):
    return await make_container(service_mode="both")


async def test_readiness_reports_running_consumer(both_container):
    c = both_container
    c.start_consumer()
    await asyncio.sleep(0.1)
    checks = await c.readiness()
    assert checks == {"database": True, "rabbitmq": True, "consumer": True}


async def test_consumer_death_is_visible_and_loud(both_container, caplog):
    c = both_container

    async def exploding_run() -> None:
        raise RuntimeError("all queues gone")

    c.event_consumer.run = exploding_run  # type: ignore[method-assign]
    with caplog.at_level("CRITICAL"):
        c.start_consumer()
        await asyncio.sleep(0.05)
    checks = await c.readiness()
    assert checks["consumer"] is False  # a dead consumer can't look ready
    assert any("no events are being consumed" in r.message for r in caplog.records)
    # And stop() must still shut everything down cleanly after the crash
    # (regression: the old lifespan re-raised here and leaked engine/bus).


async def test_api_mode_readiness_has_no_consumer_key():
    c = Container(make_settings(service_mode="api"))
    await c.start()
    try:
        checks = await c.readiness()
        assert "consumer" not in checks
        assert checks["database"] is True and checks["rabbitmq"] is True
    finally:
        await c.stop()


async def test_stop_is_idempotent_under_double_call(both_container):
    c = both_container
    c.start_consumer()
    await asyncio.sleep(0.05)
    await c.stop()  # fixture will call stop() again — must not raise


async def test_container_restart_gets_a_working_batcher(both_container):
    # Regression: stop() closes the Batcher permanently; a start→stop→start
    # cycle used to run a healthy-looking consumer whose every message hit
    # BatcherClosedError → nack → infinite redelivery. start() must rebuild
    # the consumer graph.
    c = both_container
    c.start_consumer()
    await asyncio.sleep(0.05)
    await c.stop()
    assert c.user_batcher.closed
    await c.start()
    assert not c.user_batcher.closed  # fresh graph, submits will work
    c.start_consumer()
    await asyncio.sleep(0.1)
    checks = await c.readiness()
    assert checks == {"database": True, "rabbitmq": True, "consumer": True}
