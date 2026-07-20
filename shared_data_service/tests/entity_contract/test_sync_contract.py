"""Sync guards: cross-artifact consistency the type system cannot enforce.
Infra-free — these run everywhere, including CI without a database.
"""
from __future__ import annotations

import dataclasses

from app.modules import ALL_SPECS
from app.modules.shared.repository import derive_query_fields
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import EntitySpec
from tests.entity_contract.fixtures import FIXTURES, entity_specs


@entity_specs
def test_filters_match_filter_tags(spec):
    filterable, _ = derive_query_fields(spec.model)
    assert set(spec.filters.model_fields) == set(filterable)


@entity_specs
def test_mutable_fields_subset_of_data_and_columns(spec):
    for name in spec.mutable_fields:
        assert name in spec.model.__table__.columns, name
        assert name in spec.data.model_fields, name


@entity_specs
def test_create_and_update_fields_subset_of_data(spec):
    assert set(spec.create.model_fields) <= set(spec.data.model_fields)
    update_fields = set(spec.update.model_fields) - {"expected_version"}
    assert update_fields <= set(spec.data.model_fields)


@entity_specs
def test_event_types_derive_from_entity_name(spec):
    assert spec.created_event_type == f"{spec.name}.created"
    assert spec.updated_event_type == f"{spec.name}.updated"


# NOTE: ALL_SPECS↔FIXTURES sync is enforced at COLLECTION time by the
# import-level check in conftest.py — a missing entry fails the whole
# directory, which is stronger than any test could be.


def test_validation_lives_only_in_schemas_or_service_override():
    """Per-entity validation has exactly two homes: the strict Create/Update
    schemas (Pydantic → 422) or a `service_cls` override of `_validate_data`
    (InvalidInputError → 400). Neither runs on the consumer path. A spec-level
    validator mapping (the removed `field_validators`) would run while applying
    events and could freeze a replica — guard against its reintroduction, and
    prove the sanctioned override seam exists.
    """
    spec_fields = {f.name for f in dataclasses.fields(EntitySpec)}
    assert "field_validators" not in spec_fields
    for spec in ALL_SPECS:
        assert not hasattr(spec, "field_validators")
    assert callable(getattr(VersionedEntityService, "_validate_data", None))


@entity_specs
def test_fixture_data_pair_share_id_but_differ(spec):
    f = FIXTURES[spec.name]
    first, second = f.make_valid_data(), f.make_second_valid_data()
    assert first.id == second.id
    assert any(
        getattr(first, name) != getattr(second, name)
        for name in spec.mutable_fields
    )
