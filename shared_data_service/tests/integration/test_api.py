"""REST API integration tests: real app wiring, real PostgreSQL, in-process
ASGI transport.

Generic CRUD/list/event behavior lives in tests/entity_contract/ (one
parametrized suite over ALL_SPECS); this file keeps only what is NOT part
of that contract: strict-email specifics, sort/filter result CONTENT, and
the app-level plumbing (health, correlation, OpenAPI exposure)."""
import uuid

from app.modules.user import User
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
    await client.post("/user", json=body)
    r = await client.post("/user", json={**body, "name": "Somebody Else"})
    assert r.status_code == 409
    assert "correlation_id" in r.json()


async def test_strict_email_rejected_at_api_422(client):
    r = await client.post("/user", json=payload(email="not-an-email"))
    assert r.status_code == 422  # schema-level validation


async def test_read_of_floor_violating_row_does_not_500(container, client):
    # A row written out-of-band (manual SQL, migration backfill, older
    # writer) can violate the business floor. Reads must still serve it —
    # UserOut carries PLAIN types, not UserData's floor, so response
    # validation never re-adjudicates stored data. (Regression guard: an
    # Out model inheriting the floor would 500 here and poison any list
    # page containing the row.)
    rid = uuid.uuid4()
    async with container.session_factory() as session:
        session.add(User(
            id=rid, name="   ", email="legacy-no-at-sign",
            attributes={}, version=1,
        ))
        await session.commit()

    r = await client.get(f"/user/{rid}")
    assert r.status_code == 200, r.text
    assert r.json()["email"] == "legacy-no-at-sign"  # served verbatim

    r = await client.get("/user", params={"limit": 50})
    assert r.status_code == 200 and r.json()["total"] >= 1  # page not poisoned


async def test_list_pagination_filtering_sorting(client):
    for n in range(5):
        await client.post("/user", json=payload(
            id=str(uuid.uuid4()), name=f"user-{n}", email=f"u{n}@ex.com"))

    r = await client.get("/user", params={"limit": 2, "offset": 0, "sort": "name"})
    body = r.json()
    assert r.status_code == 200 and body["total"] == 5
    assert [u["name"] for u in body["items"]] == ["user-0", "user-1"]

    # offset navigates to the next page without overlap; total is stable
    r = await client.get("/user", params={"limit": 2, "offset": 2, "sort": "name"})
    body = r.json()
    assert body["total"] == 5 and body["limit"] == 2 and body["offset"] == 2
    assert [u["name"] for u in body["items"]] == ["user-2", "user-3"]

    # last partial page
    r = await client.get("/user", params={"limit": 2, "offset": 4, "sort": "name"})
    assert [u["name"] for u in r.json()["items"]] == ["user-4"]

    r = await client.get("/user", params={"email": "u3@ex.com"})
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
    assert "/user/{user_id}" in r.json()["paths"]
