"""End-to-end filter operators over the live API (real PostgreSQL).

User's filterable fields are text (name/email), so this covers the string
lookups end to end; the numeric/date/range/in/isnull operators are covered
against real columns in tests/unit/test_filters.py.
"""
from __future__ import annotations

import uuid

from tests.integration.conftest import requires_pg, requires_rabbit

pytestmark = [requires_pg, requires_rabbit]

_NAMES = ["Ada Lovelace", "Grace Hopper", "Alan Turing"]


async def _seed(client) -> None:
    for i, name in enumerate(_NAMES):
        r = await client.post(
            "/users",
            json={
                "id": str(uuid.uuid4()),
                "name": name,
                "email": f"user{i}@example.com",
                "attributes": {},
            },
        )
        assert r.status_code == 201, r.text


async def _names(client, **params) -> set[str]:
    r = await client.get("/users", params=params)
    assert r.status_code == 200, r.text
    return {u["name"] for u in r.json()["items"]}


async def test_exact_and_icontains(client):
    await _seed(client)
    assert await _names(client, name="Ada Lovelace") == {"Ada Lovelace"}   # bare = exact
    assert await _names(client, name__exact="Ada Lovelace") == {"Ada Lovelace"}
    assert await _names(client, name__icontains="a") == set(_NAMES)        # all contain 'a'
    assert await _names(client, name__contains="Love") == {"Ada Lovelace"}


async def test_startswith_endswith_case_insensitive(client):
    await _seed(client)
    assert await _names(client, name__istartswith="a") == {"Ada Lovelace", "Alan Turing"}
    assert await _names(client, name__startswith="A") == {"Ada Lovelace", "Alan Turing"}
    assert await _names(client, name__iendswith="ER") == {"Grace Hopper"}


async def test_in_operator(client):
    await _seed(client)
    got = await _names(client, name__in="Ada Lovelace,Grace Hopper")
    assert got == {"Ada Lovelace", "Grace Hopper"}


async def test_combined_filters_are_anded(client):
    await _seed(client)
    # icontains 'a' AND email endswith '1@example.com' → only Grace (user1)
    got = await _names(client, name__icontains="a", email__endswith="1@example.com")
    assert got == {"Grace Hopper"}


async def test_unknown_field_or_operator_is_400(client):
    assert (await client.get("/users", params={"password__icontains": "x"})).status_code == 400
    assert (await client.get("/users", params={"name__regex": "x"})).status_code == 400
