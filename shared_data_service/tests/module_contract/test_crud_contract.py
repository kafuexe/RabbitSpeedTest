"""CRUD behavioral contract — every registered module, real DB, real app.

Parametrized over ALL_SPECS: a new module gets this whole suite for free
(one fixtures entry), and cannot ship without honoring the choreography.
"""
from __future__ import annotations

import uuid

from tests.module_contract.conftest import requires_pg, requires_rabbit
from tests.module_contract.fixtures import FIXTURES, module_specs

pytestmark = [requires_pg, requires_rabbit]


@module_specs
async def test_create_returns_201_and_announces(spec, client):
    f = FIXTURES[spec.name]
    r = await client.post(f.path, json=f.make_valid_create())
    assert r.status_code == 201, r.text
    assert r.json()["version"] == 1


@module_specs
async def test_create_replay_returns_200_and_reannounces(spec, client):
    f = FIXTURES[spec.name]
    body = f.make_valid_create()
    assert (await client.post(f.path, json=body)).status_code == 201
    r = await client.post(f.path, json=body)  # duplicate delivery / retry
    assert r.status_code == 200
    assert r.json()["id"] == body["id"] and r.json()["version"] == 1


@module_specs
async def test_create_contradictory_returns_409(spec, client):
    f = FIXTURES[spec.name]
    await client.post(f.path, json=f.make_valid_create())
    # Same id, different content — derived from the second data fixture so
    # the mutation is module-appropriate, not a hardcoded field name.
    other = f.make_second_valid_data().model_dump(mode="json")
    conflicting = {
        key: other[key] for key in f.make_valid_create() if key in other
    } | {"id": f.make_valid_create()["id"]}
    r = await client.post(f.path, json=conflicting)
    assert r.status_code == 409


@module_specs
async def test_get_missing_returns_404(spec, client):
    f = FIXTURES[spec.name]
    assert (await client.get(f"{f.path}/{uuid.uuid4()}")).status_code == 404


@module_specs
async def test_patch_updates_and_bumps_version(spec, client):
    f = FIXTURES[spec.name]
    body = f.make_valid_create()
    await client.post(f.path, json=body)
    r = await client.patch(f"{f.path}/{body['id']}", json=f.make_valid_update())
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 2
    for key, value in f.make_valid_update().items():
        assert r.json()[key] == value


@module_specs
async def test_patch_expected_version_conflict_returns_409(spec, client):
    f = FIXTURES[spec.name]
    body = f.make_valid_create()
    await client.post(f.path, json=body)
    r = await client.patch(
        f"{f.path}/{body['id']}",
        json={**f.make_valid_update(), "expected_version": 99},
    )
    assert r.status_code == 409
    r = await client.patch(
        f"{f.path}/{body['id']}",
        json={**f.make_valid_update(), "expected_version": 1},
    )
    assert r.status_code == 200 and r.json()["version"] == 2


@module_specs
async def test_empty_patch_returns_400(spec, client):
    f = FIXTURES[spec.name]
    body = f.make_valid_create()
    await client.post(f.path, json=body)
    assert (await client.patch(f"{f.path}/{body['id']}", json={})).status_code == 400


@module_specs
async def test_patch_null_field_means_unchanged(spec, client):
    f = FIXTURES[spec.name]
    body = f.make_valid_create()
    await client.post(f.path, json=body)
    update = f.make_valid_update()
    # A mutable field the valid update does NOT touch, sent as explicit null.
    null_field = next(m for m in spec.mutable_fields if m not in update)
    # An explicit null is not a change: alone it fails the
    # at-least-one-field rule ...
    assert (
        await client.patch(f"{f.path}/{body['id']}", json={null_field: None})
    ).status_code == 400
    # ... and alongside a real change it leaves the field untouched.
    before = (await client.get(f"{f.path}/{body['id']}")).json()
    r = await client.patch(
        f"{f.path}/{body['id']}", json={**update, null_field: None}
    )
    assert r.status_code == 200
    assert r.json()[null_field] == before[null_field]


@module_specs
async def test_invalid_update_cases_return_422(spec, client):
    f = FIXTURES[spec.name]
    body = f.make_valid_create()
    await client.post(f.path, json=body)
    for case in f.make_invalid_update_cases():
        r = await client.patch(f"{f.path}/{body['id']}", json=case)
        assert r.status_code == 422, (case, r.text)
