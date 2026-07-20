"""Sync guards: cross-artifact consistency the type system cannot enforce.
Infra-free — these run everywhere, including CI without a database.
"""
from __future__ import annotations

from app.modules.shared.repository import derive_query_fields
from tests.module_contract.fixtures import FIXTURES, module_specs


@module_specs
def test_filters_match_filter_tags(spec):
    filterable, _ = derive_query_fields(spec.model)
    assert set(spec.filters.model_fields) == set(filterable)


@module_specs
def test_mutable_fields_subset_of_data_and_columns(spec):
    for name in spec.mutable_fields:
        assert name in spec.model.__table__.columns, name
        assert name in spec.data.model_fields, name


@module_specs
def test_create_and_update_fields_subset_of_data(spec):
    assert set(spec.create.model_fields) <= set(spec.data.model_fields)
    update_fields = set(spec.update.model_fields) - {"expected_version"}
    assert update_fields <= set(spec.data.model_fields)


@module_specs
def test_event_types_derive_from_module_name(spec):
    assert spec.created_event_type == f"{spec.name}.created"
    assert spec.updated_event_type == f"{spec.name}.updated"


# NOTE: ALL_SPECS↔FIXTURES sync is enforced at COLLECTION time by the
# import-level check in conftest.py — a missing entry fails the whole
# directory, which is stronger than any test could be.


@module_specs
def test_fixture_data_pair_share_id_but_differ(spec):
    f = FIXTURES[spec.name]
    first, second = f.make_valid_data(), f.make_second_valid_data()
    assert first.id == second.id
    assert any(
        getattr(first, name) != getattr(second, name)
        for name in spec.mutable_fields
    )
