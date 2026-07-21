"""FULL end-to-end feature test — every routing/filter/pagination/event
feature exercised against the real app, real PostgreSQL, and real RabbitMQ.

Two flows:
  1. test_full_http_features — root + scoped + unscoped CRUD, the whole
     filter-operator matrix, pagination, sorting, optimistic concurrency,
     and idempotent replay, all over HTTP.
  2. test_full_ingress_egress_cycle — an API write publishes a CloudEvent
     (egress), and that exact event, replayed onto the inbound queue, is
     consumed and committed (ingress) — proving the published event format
     round-trips through the broker back into the service.
"""
from __future__ import annotations

import asyncio
import json
import uuid

import httpx
import pytest
from sqlalchemy import text

from app.api.app import create_app
from app.messaging.cloudevents import CloudEvent, now_utc
from hs_rabbit_client import RabbitClient
from tests.integration.conftest import requires_pg, requires_rabbit

pytestmark = [requires_pg, requires_rabbit]

IN_QUEUE = "sds-test.events.in"
OUT_QUEUE = "sds-test.events.out"


@pytest.fixture
async def client(container):
    app = create_app(container)  # lifespan not run; container fixture manages it
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ============================================================ HTTP feature flow


def _names(body: dict) -> list[str]:
    return [u["name"] for u in body["items"]]


async def test_full_http_features(client):
    # ---- root module CRUD: create two projects -----------------------------
    p1, p2 = str(uuid.uuid4()), str(uuid.uuid4())
    for pid, name in ((p1, "Apollo"), (p2, "Gemini")):
        r = await client.post("/project", json={
            "id": pid, "name": name, "description": "d",
            "owner_email": "owner@example.com", "attributes": {}})
        assert r.status_code == 201, r.text
        assert r.json()["version"] == 1

    # root GET + list
    assert (await client.get(f"/project/{p1}")).json()["name"] == "Apollo"
    assert (await client.get("/project")).json()["total"] == 2

    # ---- scoped create: users nested under a project -----------------------
    # Alice, Bob in p1; Carol in p2. Body carries NO project_id — the path does.
    users = {
        "Alice": (p1, str(uuid.uuid4())),
        "Bob":   (p1, str(uuid.uuid4())),
        "Carol": (p2, str(uuid.uuid4())),
    }
    for name, (pid, uid) in users.items():
        r = await client.post(f"/{pid}/user", json={
            "id": uid, "name": name,
            "email": f"{name.lower()}@example.com", "attributes": {}})
        assert r.status_code == 201, r.text
        assert r.json()["project_id"] == pid   # scope set from the path

    # Dave: unscoped create (top-level /users) → project_id stays null
    dave = str(uuid.uuid4())
    r = await client.post("/users", json={
        "id": dave, "name": "Dave", "email": "dave@example.com", "attributes": {}})
    assert r.status_code == 201 and r.json()["project_id"] is None

    # ---- scoping confinement ----------------------------------------------
    assert (await client.get("/users")).json()["total"] == 4          # unscoped: all
    assert (await client.get(f"/{p1}/user")).json()["total"] == 2     # only p1's
    assert (await client.get(f"/{p2}/user")).json()["total"] == 1     # only p2's
    # cross-scope get is a 404; correct-scope get works
    alice = users["Alice"][1]
    assert (await client.get(f"/{p2}/user/{alice}")).status_code == 404
    assert (await client.get(f"/{p1}/user/{alice}")).status_code == 200
    # a client filter can't widen the scope
    r = await client.get(f"/{p1}/user", params={"project_id": p2})
    assert r.json()["total"] == 2  # still p1's rows, the p2 filter is ignored

    # ---- the FULL filter-operator matrix (on /users) -----------------------
    async def names(**params) -> set[str]:
        r = await client.get("/users", params=params)
        assert r.status_code == 200, r.text
        return set(_names(r.json()))

    # exact / iexact
    assert await names(name="Alice") == {"Alice"}
    assert await names(name__exact="Bob") == {"Bob"}
    assert await names(name__iexact="carol") == {"Carol"}
    # contains vs icontains (case sensitivity): lowercase 'a'
    assert await names(name__contains="a") == {"Carol", "Dave"}      # Alice has no lowercase a
    assert await names(name__icontains="a") == {"Alice", "Carol", "Dave"}
    # startswith / istartswith
    assert await names(name__startswith="A") == {"Alice"}
    assert await names(name__istartswith="c") == {"Carol"}
    # endswith / iendswith (on email)
    assert await names(email__endswith="@example.com") == {"Alice", "Bob", "Carol", "Dave"}
    assert await names(email__iendswith="@EXAMPLE.COM") == {"Alice", "Bob", "Carol", "Dave"}
    # comparisons (lexicographic on name: Alice < Bob < Carol < Dave)
    assert await names(name__gt="Bob") == {"Carol", "Dave"}
    assert await names(name__gte="Carol") == {"Carol", "Dave"}
    assert await names(name__lt="Carol") == {"Alice", "Bob"}
    assert await names(name__lte="Bob") == {"Alice", "Bob"}
    # in / not_in
    assert await names(name__in="Alice,Bob") == {"Alice", "Bob"}
    assert await names(name__not_in="Alice") == {"Bob", "Carol", "Dave"}
    # isnull / not_isnull (on the scope column)
    assert await names(project_id__isnull="true") == {"Dave"}          # unscoped only
    assert await names(project_id__isnull="false") == {"Alice", "Bob", "Carol"}
    assert await names(project_id__not_isnull="true") == {"Alice", "Bob", "Carol"}
    # range (inclusive, lexicographic)
    assert await names(name__range="Bob,Carol") == {"Bob", "Carol"}
    # exact filter on the uuid scope column
    assert await names(project_id=p1) == {"Alice", "Bob"}
    # combined filters ⇒ AND
    assert await names(name__icontains="a", project_id__isnull="false") == {"Alice", "Carol"}
    # unknown field / operator ⇒ 400
    assert (await client.get("/users", params={"nope": "x"})).status_code == 400
    assert (await client.get("/users", params={"name__regex": "x"})).status_code == 400

    # ---- pagination + sorting ---------------------------------------------
    r = await client.get("/users", params={"limit": 2, "offset": 0, "sort": "name"})
    body = r.json()
    assert body["total"] == 4 and _names(body) == ["Alice", "Bob"]
    r = await client.get("/users", params={"limit": 2, "offset": 2, "sort": "name"})
    assert _names(r.json()) == ["Carol", "Dave"]           # next page, no overlap
    r = await client.get("/users", params={"sort": "-name"})
    assert _names(r.json()) == ["Dave", "Carol", "Bob", "Alice"]   # descending
    # pagination bounds → 400
    assert (await client.get("/users", params={"limit": 0})).status_code == 400
    assert (await client.get("/users", params={"offset": -1})).status_code == 400
    # sort by a non-whitelisted field → 400
    assert (await client.get("/users", params={"sort": "password"})).status_code == 400

    # ---- optimistic concurrency + idempotent replay ------------------------
    # PATCH Alice under its scope, with the version guard
    r = await client.patch(f"/{p1}/user/{alice}",
                           json={"name": "Alice K.", "expected_version": 1})
    assert r.status_code == 200 and r.json()["version"] == 2
    # stale expected_version → 409
    r = await client.patch(f"/{p1}/user/{alice}",
                           json={"name": "X", "expected_version": 1})
    assert r.status_code == 409
    # idempotent create replay: same body ⇒ 200, same row (via the unscoped route)
    replay_body = {"id": dave, "name": "Dave",
                   "email": "dave@example.com", "attributes": {}}
    assert (await client.post("/users", json=replay_body)).status_code == 200
    assert (await client.get("/users")).json()["total"] == 4     # no duplicate


# ============================================================ event ingress/egress


@pytest.fixture
async def aux():
    c = RabbitClient("amqp://guest:guest@localhost:5672/")
    await c.connect()
    for q in (IN_QUEUE, OUT_QUEUE):
        await c.delete_queue(q)
    yield c
    for q in (IN_QUEUE, OUT_QUEUE):
        await c.delete_queue(q)
    await c.close()


async def _fetch(container, uid: uuid.UUID):
    async with container.engine.connect() as conn:
        return (await conn.execute(
            text("SELECT name, version FROM users WHERE id = :id"), {"id": str(uid)}
        )).first()


async def _wait_for(predicate, timeout: float = 10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if (result := await predicate()):
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met before timeout")


async def test_full_ingress_egress_cycle(container, client, aux):
    """The complete round-trip: an API write EGRESSES a CloudEvent to the
    outbound queue; that exact event, INGRESSED on the inbound queue, is
    consumed and committed — proving SDS's published events are valid inputs
    to the same contract it consumes."""
    # --- EGRESS: capture the user.created the API publishes after commit ---
    captured: list[bytes] = []
    got = asyncio.Event()

    async def collect(body: bytes) -> None:
        captured.append(body)
        got.set()

    out_consumer = await aux.consume(OUT_QUEUE, collect)
    try:
        api_uid = str(uuid.uuid4())
        r = await client.post("/users", json={
            "id": api_uid, "name": "Ingress Egress",
            "email": "ie@example.com", "attributes": {"k": "v"}})
        assert r.status_code == 201, r.text
        await asyncio.wait_for(got.wait(), timeout=10)
    finally:
        await out_consumer.cancel()

    egress = json.loads(captured[0])
    assert egress["type"] == "user.created"
    assert egress["data"]["id"] == api_uid and egress["data"]["version"] == 1

    # --- INGRESS: replay that egress event (re-id'd) onto the inbound queue,
    #     run the real consumer, and assert the row is committed -------------
    consumer_task = asyncio.create_task(container.event_consumer.run())
    try:
        ingress_uid = uuid.uuid4()
        replay = CloudEvent(
            id=f"e2e-{uuid.uuid4()}",
            source="urn:test:e2e-producer",
            type=egress["type"],
            time=now_utc(),
            data={**egress["data"], "id": str(ingress_uid)},  # same shape, new id
        ).to_bytes()
        await aux.publish(IN_QUEUE, replay)

        row = await _wait_for(lambda: _fetch(container, ingress_uid))
        assert row.name == "Ingress Egress" and row.version == 1
    finally:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
