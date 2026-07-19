"""Contract-suite fixtures. Reuses the integration fixtures (real
PostgreSQL, real bus) and fails COLLECTION — not skip — when a registered
spec has no fixtures entry."""
from __future__ import annotations

import httpx
import pytest

from app.api.app import create_app
from app.modules import ALL_SPECS
from tests.entity_contract.fixtures import FIXTURES
from tests.integration.conftest import (  # noqa: F401  (fixture re-export)
    container,
    make_container,
    requires_pg,
    requires_rabbit,
)

_registered = {spec.name for spec in ALL_SPECS}
_out_of_sync = _registered.symmetric_difference(FIXTURES)
assert not _out_of_sync, (
    f"tests/entity_contract/fixtures.py out of sync with ALL_SPECS: {_out_of_sync}"
)


@pytest.fixture
async def client(container):  # noqa: F811 (pytest fixture injection)
    app = create_app(container)  # lifespan not run; container fixture manages it
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
