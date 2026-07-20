"""Integration fixtures. Tests are skipped when Postgres (5434) or RabbitMQ
(5672) are not reachable, so the unit suite stays green anywhere."""
from __future__ import annotations

import socket

import httpx
import pytest
from sqlalchemy import text

from app.api.app import create_app
from app.bootstrap.container import Container
from app.config.settings import Settings
from app.modules import ALL_SPECS

# Derived from the registry so a new module needs NO edit here.
_TRUNCATE_TABLES = ", ".join(
    [spec.model.__tablename__ for spec in ALL_SPECS] + ["processed_events"]
)


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
            # The scoping migration (users.project_id) can't be applied to the
            # shared dev DB while it's parked on another worktree's revision;
            # add the column idempotently here so tests match the model. This
            # is additive/nullable — harmless to any other session on the DB.
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS project_id UUID")
            )
            await conn.execute(text(f"TRUNCATE {_TRUNCATE_TABLES}"))
        created.append(c)
        return c

    yield _make
    for c in created:
        await c.stop()


@pytest.fixture
async def container(make_container):
    return await make_container()


@pytest.fixture
async def client(container):
    """In-process ASGI client over the real app wiring. ONE definition —
    the integration and module-contract suites must test the same app."""
    app = create_app(container)  # lifespan not run; container fixture manages it
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
