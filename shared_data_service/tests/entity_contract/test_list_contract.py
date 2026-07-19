"""List-endpoint contract: pagination bounds and the query whitelists,
derived from each model's q() tags — the same source the repository uses.
"""
from __future__ import annotations

import pytest

from app.modules import ALL_SPECS
from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.repository import derive_query_fields
from tests.entity_contract.conftest import requires_pg, requires_rabbit
from tests.entity_contract.fixtures import FIXTURES

pytestmark = [requires_pg, requires_rabbit]

specs = pytest.mark.parametrize("spec", ALL_SPECS, ids=lambda s: s.name)


@specs
async def test_pagination_bounds_rejected_400(spec, client):
    f = FIXTURES[spec.name]
    assert (await client.get(f.path, params={"limit": 0})).status_code == 400
    assert (await client.get(f.path, params={"limit": 100000})).status_code == 400
    assert (await client.get(f.path, params={"offset": -1})).status_code == 400


@specs
async def test_sort_accepts_tagged_and_always_sortable_fields(spec, client):
    f = FIXTURES[spec.name]
    _, sortable = derive_query_fields(spec.model)
    for field in sorted(sortable):
        r = await client.get(f.path, params={"sort": field})
        assert r.status_code == 200, (field, r.text)
        r = await client.get(f.path, params={"sort": f"-{field}"})
        assert r.status_code == 200, (field, r.text)


@specs
async def test_sort_rejects_untagged_400(spec, client):
    f = FIXTURES[spec.name]
    assert (await client.get(f.path, params={"sort": "no_such"})).status_code == 400


@specs
async def test_filter_accepts_tagged_fields_and_matches(spec, client):
    f = FIXTURES[spec.name]
    body = f.make_valid_create()
    await client.post(f.path, json=body)
    filterable, _ = derive_query_fields(spec.model)
    for field in sorted(filterable):
        assert field in body, f"filterable field {field!r} missing from create fixture"
        r = await client.get(f.path, params={field: body[field]})
        assert r.status_code == 200 and r.json()["total"] == 1, (field, r.text)
        r = await client.get(f.path, params={field: "zz@no.match" if "email" in field else "zz-no-match"})
        assert r.status_code == 200 and r.json()["total"] == 0, field


@specs
async def test_filter_rejects_unknown_at_service_level(spec, container):
    # Unknown HTTP query params are ignored by FastAPI (they are not
    # declared), so the whitelist rejection is a service-level guarantee.
    with pytest.raises(InvalidQueryError):
        await container.services[spec.name].list_page(
            limit=10, offset=0, filters={"no_such": "x"}
        )
