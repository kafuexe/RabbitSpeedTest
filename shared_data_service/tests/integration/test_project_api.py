"""Project module integration tests: the same guarantees as the user module,
delivered by the shared VersionedRepository/VersionedEntityService — real app
wiring, real PostgreSQL, in-process ASGI transport."""
import uuid

import httpx
import pytest

from app.api.app import create_app
from app.modules.project.business import ProjectData
from app.modules.shared.service import StateEventItem
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
        "name": "Apollo",
        "description": "Guidance computer rewrite",
        "owner_email": "margaret@example.com",
        "attributes": {"tier": "gold"},
    }
    body.update(overrides)
    return body


async def test_create_get_roundtrip(client):
    body = payload()
    r = await client.post("/projects", json=body)
    assert r.status_code == 201, r.text
    created = r.json()
    assert created["version"] == 1 and created["attributes"] == {"tier": "gold"}

    r = await client.get(f"/projects/{body['id']}")
    assert r.status_code == 200 and r.json()["name"] == "Apollo"


async def test_create_replay_returns_200_conflicting_replay_409(client):
    body = payload()
    assert (await client.post("/projects", json=body)).status_code == 201
    assert (await client.post("/projects", json=body)).status_code == 200
    r = await client.post("/projects", json={**body, "name": "Gemini"})
    assert r.status_code == 409


async def test_update_versioning_and_conflict(client):
    body = payload()
    await client.post("/projects", json=body)

    r = await client.patch(
        f"/projects/{body['id']}", json={"description": "Now with lunar module"}
    )
    assert r.status_code == 200 and r.json()["version"] == 2

    r = await client.patch(
        f"/projects/{body['id']}", json={"name": "X", "expected_version": 1}
    )
    assert r.status_code == 409  # concurrent-update guard


async def test_list_filters_and_rejects_unknown_fields(client):
    await client.post("/projects", json=payload(owner_email="a@example.com"))
    await client.post("/projects", json=payload(owner_email="b@example.com"))

    r = await client.get("/projects", params={"owner_email": "a@example.com"})
    assert r.status_code == 200 and r.json()["total"] == 1

    r = await client.get("/projects", params={"sort": "attributes"})  # not whitelisted
    assert r.status_code == 400


async def test_apply_state_events_dedup_and_version_guard(container):
    """The consumer-path choreography (inbox dedup + version-guarded upsert)
    against real PostgreSQL, driven through the generic apply_state_events."""
    service = container.project_service  # stages no events on this path
    pid = uuid.uuid4()

    def data(version: int, name: str) -> ProjectData:
        return ProjectData(id=pid, name=name, description="",
                           owner_email="p@example.com", attributes={}, version=version)

    await service.apply_state_events([
        StateEventItem("evt-1", "urn:other", data(2, "live")),
    ])
    # duplicate event id → inbox-filtered; stale version → upsert-skipped
    await service.apply_state_events([
        StateEventItem("evt-1", "urn:other", data(9, "dup")),
        StateEventItem("evt-0", "urn:other", data(1, "stale")),
    ])
    stored = await service.get(pid)
    assert stored.name == "live" and stored.version == 2

    # newer version applied
    await service.apply_state_events([
        StateEventItem("evt-2", "urn:other", data(5, "newest")),
    ])
    stored = await service.get(pid)
    assert stored.name == "newest" and stored.version == 5
