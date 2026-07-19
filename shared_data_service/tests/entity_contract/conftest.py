"""Contract-suite fixtures. Reuses the integration fixtures (real
PostgreSQL, real bus) and fails COLLECTION — not skip — when a registered
spec has no fixtures entry."""
from __future__ import annotations

from app.modules import ALL_SPECS
from tests.entity_contract.fixtures import FIXTURES
from tests.integration.conftest import (  # noqa: F401  (fixture re-export)
    client,
    container,
    make_container,
    requires_pg,
    requires_rabbit,
)

# A spec without fixtures must fail COLLECTION of this whole directory —
# not skip, not pass silently.
_out_of_sync = {spec.name for spec in ALL_SPECS}.symmetric_difference(FIXTURES)
if _out_of_sync:
    raise RuntimeError(
        f"tests/entity_contract/fixtures.py out of sync with ALL_SPECS: "
        f"{_out_of_sync}"
    )
