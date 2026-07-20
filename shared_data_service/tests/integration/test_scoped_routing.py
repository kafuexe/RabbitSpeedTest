"""Scoped (nested) routing: /{project_id}/user is confined to one project;
/users is the top-level unscoped view. Real app wiring, real PostgreSQL."""
from __future__ import annotations

import uuid

import httpx
import pytest

from app.api.app import create_app
from tests.integration.conftest import requires_pg, requires_rabbit

pytestmark = [requires_pg, requires_rabbit]


@pytest.fixture
async def client(container):
    app = create_app(container)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _user(**over) -> dict:
    body = {
        "id": str(uuid.uuid4()),
        "name": "Ada Lovelace",
        "email": "ada@example.com",
        "attributes": {},
    }
    body.update(over)
    return body


async def test_scoped_create_sets_project_id_and_get_is_scoped(client):
    proj = str(uuid.uuid4())
    body = _user()
    r = await client.post(f"/{proj}/user", json=body)
    assert r.status_code == 201, r.text
    assert r.json()["project_id"] == proj  # scope set from the path

    # reachable under its own project
    r = await client.get(f"/{proj}/user/{body['id']}")
    assert r.status_code == 200 and r.json()["id"] == body["id"]

    # NOT reachable under a different project → 404 (not another project's row)
    other = str(uuid.uuid4())
    assert (await client.get(f"/{other}/user/{body['id']}")).status_code == 404

    # but visible on the unscoped top-level route
    assert (await client.get(f"/users/{body['id']}")).status_code == 200


async def test_scoped_list_only_returns_that_projects_users(client):
    p1, p2 = str(uuid.uuid4()), str(uuid.uuid4())
    for _ in range(2):
        assert (await client.post(f"/{p1}/user", json=_user())).status_code == 201
    assert (await client.post(f"/{p2}/user", json=_user())).status_code == 201

    assert (await client.get(f"/{p1}/user")).json()["total"] == 2
    assert (await client.get(f"/{p2}/user")).json()["total"] == 1
    assert (await client.get("/users")).json()["total"] == 3  # unscoped sees all


async def test_scoped_list_ignores_client_project_id_filter(client):
    p1, p2 = str(uuid.uuid4()), str(uuid.uuid4())
    assert (await client.post(f"/{p1}/user", json=_user())).status_code == 201
    assert (await client.post(f"/{p2}/user", json=_user())).status_code == 201
    # a client trying to widen past its scope is ignored — still only p1's row
    r = await client.get(f"/{p1}/user", params={"project_id": p2})
    assert r.json()["total"] == 1


async def test_scoped_update_is_confined_to_scope(client):
    p1, p2 = str(uuid.uuid4()), str(uuid.uuid4())
    body = _user()
    await client.post(f"/{p1}/user", json=body)

    # update under the wrong project → 404
    r = await client.patch(f"/{p2}/user/{body['id']}", json={"name": "Grace"})
    assert r.status_code == 404
    # update under the right project → 200, and project_id is unchanged
    r = await client.patch(f"/{p1}/user/{body['id']}", json={"name": "Grace"})
    assert r.status_code == 200
    assert r.json()["name"] == "Grace" and r.json()["project_id"] == p1


async def test_unscoped_create_leaves_project_id_null(client):
    body = _user()
    r = await client.post("/users", json=body)
    assert r.status_code == 201
    assert r.json()["project_id"] is None  # top-level create is not scoped
