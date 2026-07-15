"""REST API integration tests: real app wiring, real PostgreSQL, in-process
ASGI transport."""
import uuid

import httpx
import pytest

from app.api.app import create_app
from tests.integration.conftest import requires_pg, requires_rabbit

pytestmark = [requires_pg, requires_rabbit]


@pytest.fixture
async def client(container):
    app = create_app(container)  # lifespan not run; container fixture manages it
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def payload(**overrides):
    body = {
        "id": str(uuid.uuid4()),
        "name": "Ada Lovelace",
        "email": "ada@example.com",
        "attributes": {"role": "engineer"},
    }
    body.update(overrides)
    return body


async def test_create_get_roundtrip(client):
    body = payload()
    r = await client.post("/users", json=body)
    assert r.status_code == 201, r.text
    created = r.json()
    assert created["version"] == 1 and created["attributes"] == {"role": "engineer"}

    r = await client.get(f"/users/{body['id']}")
    assert r.status_code == 200 and r.json()["name"] == "Ada Lovelace"


async def test_create_replay_returns_200_not_duplicate(client):
    body = payload()
    assert (await client.post("/users", json=body)).status_code == 201
    r = await client.post("/users", json=body)  # duplicate delivery / retry
    assert r.status_code == 200
    assert r.json()["id"] == body["id"]


async def test_create_conflicting_replay_is_409(client):
    body = payload()
    await client.post("/users", json=body)
    r = await client.post("/users", json={**body, "name": "Somebody Else"})
    assert r.status_code == 409
    assert "correlation_id" in r.json()


async def test_update_versioning_and_conflict(client):
    body = payload()
    await client.post("/users", json=body)

    r = await client.patch(f"/users/{body['id']}", json={"name": "Ada K."})
    assert r.status_code == 200 and r.json()["version"] == 2

    r = await client.patch(
        f"/users/{body['id']}", json={"name": "X", "expected_version": 1}
    )
    assert r.status_code == 409  # concurrent-update guard

    r = await client.patch(
        f"/users/{body['id']}", json={"name": "Ada L.", "expected_version": 2}
    )
    assert r.status_code == 200 and r.json()["version"] == 3


async def test_not_found_and_validation_errors(client):
    assert (await client.get(f"/users/{uuid.uuid4()}")).status_code == 404
    assert (await client.patch(f"/users/{uuid.uuid4()}", json={})).status_code == 400
    r = await client.post("/users", json=payload(email="not-an-email"))
    assert r.status_code == 422  # schema-level validation


async def test_list_pagination_filtering_sorting(client):
    for n in range(5):
        await client.post("/users", json=payload(
            id=str(uuid.uuid4()), name=f"user-{n}", email=f"u{n}@ex.com"))

    r = await client.get("/users", params={"limit": 2, "offset": 0, "sort": "name"})
    body = r.json()
    assert r.status_code == 200 and body["total"] == 5
    assert [u["name"] for u in body["items"]] == ["user-0", "user-1"]

    r = await client.get("/users", params={"email": "u3@ex.com"})
    assert r.json()["total"] == 1

    assert (await client.get("/users", params={"sort": "password"})).status_code == 400
    assert (await client.get("/users", params={"limit": 100000})).status_code == 400


async def test_health_ready_and_correlation(client):
    assert (await client.get("/health")).json() == {"status": "ok"}

    r = await client.get("/ready")
    assert r.status_code == 200
    assert r.json() == {"database": True, "rabbitmq": True}

    r = await client.get("/health", headers={"X-Correlation-ID": "trace-me-123"})
    assert r.headers["X-Correlation-ID"] == "trace-me-123"


async def test_openapi_exposed(client):
    r = await client.get("/openapi.json")
    assert r.status_code == 200
    assert "/users/{user_id}" in r.json()["paths"]
