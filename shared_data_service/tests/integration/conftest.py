"""Integration fixtures. Tests are skipped when Postgres (5434) or RabbitMQ
(5672) are not reachable, so the unit suite stays green anywhere."""
from __future__ import annotations

import socket

import pytest
from sqlalchemy import text

from app.bootstrap.container import Container
from app.config.settings import Settings


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False


PG_UP = port_open(5434)
RABBIT_UP = port_open(5672)

requires_pg = pytest.mark.skipif(not PG_UP, reason="no PostgreSQL on :5434")
requires_rabbit = pytest.mark.skipif(not RABBIT_UP, reason="no RabbitMQ on :5672")


def make_settings(**overrides) -> Settings:
    defaults = dict(
        consume_queues=["sds-test.events.in"],
        publish_queue="sds-test.events.out",
        service_mode="api",
        log_level="WARNING",
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
async def make_container():
    """Factory fixture: ONE place owns container setup/teardown (start,
    TRUNCATE, stop) for every integration test, whatever the settings."""
    created: list[Container] = []

    async def _make(**overrides) -> Container:
        c = Container(make_settings(**overrides))
        await c.start()
        async with c.engine.begin() as conn:
            await conn.execute(text("TRUNCATE users, projects, processed_events"))
        created.append(c)
        return c

    yield _make
    for c in created:
        await c.stop()


@pytest.fixture
async def container(make_container):
    return await make_container()
