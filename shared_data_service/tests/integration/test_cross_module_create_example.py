"""WORKED EXAMPLE (also a passing test): cross-module logic on create.

Question this answers: "when I create an /organization, I also want to run
business logic on the USERS module — e.g. seed a default user." How, given
that every module is isolated and the generic wiring hands each entity only
its own service?

Answer: modules stay isolated; CROSS-module coordination is a
composition-root concern. You subclass the shared `EntityRoutes` for the
"outer" entity, override `create` to call `super().create(...)` (its own
transaction + published event) and then invoke the OTHER module's service,
which you INJECT at construction. The composition root (container/api_app)
is the only place that legitimately knows about two modules, so that is
where the injection happens.

This example uses the existing `project` entity as a stand-in for
`organization` (no `organizations` table exists yet) — the mechanism is
identical; in a real app you write `OrganizationRoutes` over the org spec.

Two correctness points the example demonstrates:

1. NOT one transaction. The org and the user are two separate UnitOfWork
   commits (each service owns its UoW). If atomicity matters, prefer the
   event-driven variant (org.created → a composition-root handler seeds the
   user); it is crash-safe and at-least-once via the inbox. See the module
   docstring note at the bottom.

2. Idempotency REQUIRES a deterministic child id. A client that retries the
   POST replays the org create (→ 200, same row); if the default user's id
   were a fresh uuid4 each time, the retry would create a SECOND user. Deriving
   the id from the org (uuid5) makes the seed a replay-safe no-op.
"""
from __future__ import annotations

import uuid

import httpx
import pytest
from fastapi import FastAPI, Response
from pydantic import BaseModel

from app.modules.project import PROJECT_SPEC, Project, ProjectData, ProjectUpdate
from app.modules.shared.routes import EntityRoutes
from app.modules.shared.service import VersionedEntityService
from app.modules.user import UserData
from tests.integration.conftest import requires_pg, requires_rabbit

pytestmark = [requires_pg, requires_rabbit]

_SEED_NS = uuid.UUID("6f2a1c00-0000-4000-8000-000000000001")


# ---------------------------------------------------------------- the pattern


class OrganizationRoutes(EntityRoutes[Project, ProjectData, ProjectUpdate]):
    """`organization` (here: Project) routes that seed a default user on
    create. The USER service is injected — the module never reaches into a
    sibling; the composition root passes the collaborator in."""

    def __init__(
        self,
        spec,
        service: VersionedEntityService,
        *,
        user_service: VersionedEntityService,
    ) -> None:
        super().__init__(spec, service)
        self._user_service = user_service

    async def create(self, payload: BaseModel, response: Response) -> BaseModel:
        org = await super().create(payload, response)  # org's own txn + event
        # Deterministic child id → seeding is idempotent under client retry.
        default_user_id = uuid.uuid5(_SEED_NS, f"default-user:{org.id}")
        await self._user_service.create(
            UserData(
                id=default_user_id,
                name=f"{org.name} owner",
                email=org.owner_email,
                attributes={"organization_id": str(org.id), "role": "owner"},
            )
        )
        return org


# ------------------------------------------------------------------- the test


@pytest.fixture
async def org_client(container):
    """An app that mounts ONLY the OrganizationRoutes, wired with both the
    project and user services — exactly what the composition root would do."""
    app = FastAPI()
    routes = OrganizationRoutes(
        PROJECT_SPEC,
        container.services["project"],
        user_service=container.services["user"],
    )
    app.include_router(routes.register())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _org_body(org_id: str) -> dict:
    return {
        "id": org_id,
        "name": "Acme",
        "description": "",
        "owner_email": "boss@acme.com",
        "attributes": {},
    }


async def test_org_create_seeds_a_default_user(org_client, container):
    org_id = str(uuid.uuid4())
    r = await org_client.post("/project", json=_org_body(org_id))
    assert r.status_code == 201, r.text

    # the default user now exists, derived deterministically from the org
    default_user_id = uuid.uuid5(_SEED_NS, f"default-user:{org_id}")
    user = await container.services["user"].get(default_user_id)
    assert user.email == "boss@acme.com"
    assert user.attributes["organization_id"] == org_id
    assert user.attributes["role"] == "owner"


async def test_org_create_replay_does_not_duplicate_the_user(org_client, container):
    org_id = str(uuid.uuid4())
    assert (await org_client.post("/project", json=_org_body(org_id))).status_code == 201
    # client retry: org create replays (200), and the deterministic user id
    # makes the seed a replay-safe no-op — not a second user.
    replay = await org_client.post("/project", json=_org_body(org_id))
    assert replay.status_code == 200
    total_users = (await container.services["user"].list_page(limit=50, offset=0)).total
    assert total_users == 1  # exactly one default user, no duplicate


# ---------------------------------------------------------------------------
# The event-driven alternative (recommended when you want decoupling +
# crash-safety, and eventual consistency is acceptable): instead of calling
# the user service inline, publish `organization.created` (the API path
# already does) and register a composition-root handler for it that creates
# the default user. The inbox dedups redelivery; the deterministic user id
# keeps the create idempotent. That handler also lives at the composition
# root — it is the only place that holds both services.
