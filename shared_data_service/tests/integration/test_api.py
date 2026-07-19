"""REST API integration tests: real app wiring, real PostgreSQL, in-process
ASGI transport.

Generic CRUD/list/event behavior lives in tests/entity_contract/ (one
parametrized suite over ALL_SPECS); this file keeps only what is NOT part
of that contract: strict-email specifics, sort/filter result CONTENT, and
the app-level plumbing (health, correlation, OpenAPI exposure)."""
import uuid

from tests.integration.conftest import requires_pg, requires_rabbit

pytestmark = [requires_pg, requires_rabbit]


def payload(**overrides):
    body = {
        "id": str(uuid.uuid4()),
        "name": "Ada Lovelace",
        "email": "ada@example.com",
        "attributes": {"role": "engineer"},
    }
    body.update(overrides)
    return body


async def test_error_body_carries_correlation_id(client):
    body = payload()
    await client.post("/users", json=body)
    r = await client.post("/users", json={**body, "name": "Somebody Else"})
    assert r.status_code == 409
    assert "correlation_id" in r.json()


async def test_strict_email_rejected_at_api_422(client):
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
