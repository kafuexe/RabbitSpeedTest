"""Entity registry — the ONLY place a new entity is added.

Adding an entity = one module file (see app/modules/user.py for the
canonical shape) + one line in ALL_SPECS + one fixtures entry in
tests/entity_contract/fixtures.py. Container wiring, router mounting, and
the entity-contract test suite all iterate this tuple; none of them are
edited per entity.

Import discipline (keeps this cycle-free): entity modules import from
modules/shared/ only; this registry imports the entity modules; nothing in
modules/shared/ imports the registry.
"""
from __future__ import annotations

from typing import Any

from app.modules.project import PROJECT_SPEC
from app.modules.shared.spec import EntitySpec
from app.modules.user import USER_SPEC

# Order matters: it is the router mount order (and therefore the OpenAPI
# path order — keep (user, project) to stay baseline-identical).
ALL_SPECS: tuple[EntitySpec[Any, Any, Any], ...] = (USER_SPEC, PROJECT_SPEC)

# Startup footgun guard — fails at import, not at first request. A raise,
# not an assert: asserts vanish under `python -O`, which is exactly the
# environment this guard exists for. (Per-spec shape invariants live in
# EntitySpec.__post_init__.)
_names = [spec.name for spec in ALL_SPECS]
if len(set(_names)) != len(_names):
    raise RuntimeError(f"duplicate entity names: {_names}")
