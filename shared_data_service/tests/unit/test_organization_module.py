"""Organization-specific rules — the ones beyond the generic contract suite.

The parametrized entity_contract/ suite already exercises CRUD, list
whitelists, and event choreography for every spec (organization included via
its fixtures entry). This file pins what is unique to organization: the
strict-at-API / permissive-at-events email split and the 50-char `plan` rule.
Infra-free (pure pydantic), so it runs in the unit suite anywhere.
"""
from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from app.modules.organization import (
    ORGANIZATION_SPEC,
    OrganizationCreate,
    OrganizationData,
    OrganizationUpdate,
)

_ID = uuid.UUID("00000000-0000-0000-0000-00000000f00d")


def test_create_rejects_malformed_email_strictly():
    # API ingress is strict: a domain without a dot is a 422 the client can fix.
    with pytest.raises(ValidationError):
        OrganizationCreate(name="Bell Labs", billing_email="ops@backend")


def test_create_normalizes_strict_email():
    # StrictEmail is EmailStr: it normalizes (lower-cases the domain).
    org = OrganizationCreate(name="Bell Labs", billing_email="Ada@Example.COM")
    assert org.billing_email == "Ada@example.com"


def test_data_email_floor_is_permissive_and_verbatim():
    # The consumer payload floor accepts a minimally-shaped address and stores
    # it verbatim — rejecting it would freeze the replica at the old version.
    org = OrganizationData(id=_ID, name="Bell Labs", billing_email="ops@backend")
    assert org.billing_email == "ops@backend"


def test_data_email_floor_still_requires_at_sign():
    with pytest.raises(ValidationError):
        OrganizationData(id=_ID, name="Bell Labs", billing_email="no-at-sign")


def test_plan_defaults_to_free():
    assert OrganizationCreate(name="X", billing_email="a@b.co").plan == "free"
    assert OrganizationData(id=_ID, name="X", billing_email="a@b.co").plan == "free"


@pytest.mark.parametrize("model", [OrganizationCreate, OrganizationData, OrganizationUpdate])
def test_plan_length_rule_is_single_sourced(model):
    # OrganizationPlan (max_length=50) composed into every schema — the limit
    # cannot drift between API ingress, event payload, and update.
    kwargs = {"name": "X", "billing_email": "a@b.co", "plan": "x" * 51}
    if model is OrganizationData:
        kwargs["id"] = _ID
    if model is OrganizationUpdate:
        kwargs = {"plan": "x" * 51}
    with pytest.raises(ValidationError):
        model(**kwargs)


def test_spec_shape():
    # The declaration the generic machinery consumes.
    assert ORGANIZATION_SPEC.name == "organization"
    assert ORGANIZATION_SPEC.mutable_fields == (
        "name", "billing_email", "plan", "attributes",
    )
    assert ORGANIZATION_SPEC.created_event_type == "organization.created"
    assert ORGANIZATION_SPEC.updated_event_type == "organization.updated"
    # Filters mirror exactly the q(filter=True) tags (also enforced by the
    # sync contract; asserted here for a fast local signal).
    assert set(ORGANIZATION_SPEC.filters.model_fields) == {"name", "billing_email"}
