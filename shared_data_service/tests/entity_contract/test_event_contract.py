"""Event contract: payload shape stability and the consumer-path
choreography (idempotent, order-safe) — per registered entity, real DB.
"""
from __future__ import annotations

from app.modules.shared.spec import StateEventItem
from tests.entity_contract.conftest import requires_pg, requires_rabbit
from tests.entity_contract.fixtures import FIXTURES, entity_specs

pytestmark = [requires_pg, requires_rabbit]


def _assert_stored_matches(spec, stored, data) -> None:
    for name in spec.mutable_fields:
        assert getattr(stored, name) == getattr(data, name), name


@entity_specs
async def test_event_payload_field_set_equals_data_model_fields(spec, container):
    """THE generic byte-compat guard: whatever a module's event builder
    does, the payload keys must be exactly the Data model's declared fields
    — no server timestamps, no extras. Uses the service's real builder
    (private hook, deliberately: this is the object under contract)."""
    f = FIXTURES[spec.name]
    service = container.services[spec.name]
    entity, _ = await service.create(f.make_valid_data())
    event = service._build_event(spec.created_event_type, entity)
    assert set(event.data.keys()) == set(spec.data.model_fields)


@entity_specs
async def test_out_of_order_apply(spec, container):
    f = FIXTURES[spec.name]
    service = container.services[spec.name]
    newer = f.make_second_valid_data().model_copy(update={"version": 2})
    older = f.make_valid_data()  # version 1
    await service.apply_state_events(
        [StateEventItem("evt-2", "urn:other", newer)]
    )
    await service.apply_state_events(
        [StateEventItem("evt-1", "urn:other", older)]  # late create → stale
    )
    stored = await service.get(older.id)
    assert stored.version == 2
    _assert_stored_matches(spec, stored, newer)


@entity_specs
async def test_duplicate_delivery_is_noop(spec, container):
    f = FIXTURES[spec.name]
    service = container.services[spec.name]
    first = f.make_valid_data()
    await service.apply_state_events([StateEventItem("evt-1", "urn:other", first)])
    impostor = f.make_second_valid_data().model_copy(update={"version": 9})
    await service.apply_state_events(
        [StateEventItem("evt-1", "urn:other", impostor)]  # same event id
    )
    stored = await service.get(first.id)
    assert stored.version == 1
    _assert_stored_matches(spec, stored, first)


@entity_specs
async def test_within_batch_highest_version_wins(spec, container):
    f = FIXTURES[spec.name]
    service = container.services[spec.name]
    v1 = f.make_valid_data()
    v3 = f.make_second_valid_data().model_copy(update={"version": 3})
    v2 = f.make_valid_data().model_copy(update={"version": 2})
    await service.apply_state_events([
        StateEventItem("evt-1", "urn:other", v1),
        StateEventItem("evt-3", "urn:other", v3),
        StateEventItem("evt-2", "urn:other", v2),
    ])
    stored = await service.get(v1.id)
    assert stored.version == 3
    _assert_stored_matches(spec, stored, v3)
