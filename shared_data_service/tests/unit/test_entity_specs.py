"""Spec-consistency guards: things the type system cannot enforce.

The Filters schemas are hand-declared (so pyright and OpenAPI see real,
named, typed query params) while the query whitelists derive from the ORM
column tags — two sources that MUST agree. This test is the drift guard.
"""
import pytest

from app.modules.project import PROJECT_SPEC
from app.modules.shared.repository import derive_query_fields
from app.modules.user import USER_SPEC

SPECS = [USER_SPEC, PROJECT_SPEC]


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_filters_schema_matches_model_filter_tags(spec):
    filterable, _ = derive_query_fields(spec.model)
    assert set(spec.filters.model_fields) == set(filterable)


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_mutable_fields_exist_on_model_and_data(spec):
    for name in spec.mutable_fields:
        assert name in spec.model.__table__.columns, name
        assert name in spec.data.model_fields, name


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_event_types_derive_from_entity_name(spec):
    assert spec.created_event_type == f"{spec.name}.created"
    assert spec.updated_event_type == f"{spec.name}.updated"
