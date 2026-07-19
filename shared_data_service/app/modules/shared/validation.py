"""Business validation rules shared by API schemas, event payloads, and the
business-layer data models — ONE definition per rule, so the write paths
cannot drift.

Mechanism: the rule functions below are the single rulebook, and the shared
`Annotated` types are the one place where shape constraints (`Field` lengths)
and rules (`AfterValidator`) meet. Models simply declare fields with these
types — no per-model `@field_validator` boilerplate — and pydantic's default
whole-schema validation aggregates every field failure into one
ValidationError. Metadata order matters and is deliberate: `Field`
constraints run first, the rule function after, which is exactly the order
the old `Field(...)` + `@field_validator` pairs ran in (so error shapes are
unchanged).

Storability primitives live in `app.database.storable` (they are facts about
PostgreSQL); they are re-exported here so business-layer code has a single
import point for all validation.

Email policy — a DELIBERATE asymmetry:
- The API ingress is STRICT (`StrictEmail` / `valid_email`, the exact rule
  EmailStr runs): a client submitting a bad address gets a 422 and can
  correct it.
- The consumer path is PERMISSIVE (`FloorEmail`: storable + minimal '@'
  shape, verbatim): consumed events carry FULL-STATE announcements from an
  authoritative producer, and rejecting one over email syntax would
  permanently freeze the replica at the previous version (every later event
  for that user carries the same email). Replication fidelity beats
  re-adjudicating validity; only genuinely unstorable data (NUL/NaN) is
  rejected there.
"""
from __future__ import annotations

from typing import Annotated, Any

from pydantic import AfterValidator, EmailStr, Field
from pydantic.networks import validate_email as _pydantic_validate_email

from app.database.storable import storable_json, storable_text

__all__ = [
    "storable_json", "storable_text", "valid_name", "valid_email",
    "email_floor", "ValidName", "StrictEmail", "FloorEmail", "StorableText",
    "StorableAttributes",
]


# --------------------------------------------------------------- the rules
# Plain functions so the rulebook stays importable and directly testable;
# the Annotated types below are how models consume them.


def valid_name(value: str) -> str:
    """The business floor for a user name: non-blank and storable."""
    if not value.strip():
        raise ValueError("must not be blank")
    return storable_text(value)


def valid_email(value: str) -> str:
    """STRICT email validation — pydantic's own EmailStr rule (its
    validate_email wrapper, which also unwraps 'Name <addr>' pretty forms),
    so the business floor and the API schema can never disagree. Returns the
    normalized address, exactly as EmailStr stores it."""
    storable_text(value)
    return _pydantic_validate_email(value)[1]


def email_floor(value: str) -> str:
    """PERMISSIVE consumer-path floor: storable and minimally email-shaped,
    stored VERBATIM (no normalization — the producer's value is the truth).
    See the module docstring for why the consumer must not be strict."""
    if "@" not in value:
        raise ValueError("must contain '@'")
    return storable_text(value)


# ------------------------------------------------------ the Annotated types
# Declare fields with these; constructing (or, with validate_assignment,
# mutating) a model IS the validation. Field constraints precede the
# AfterValidator so schema checks (lengths) fire before the rule function.

ValidName = Annotated[
    str, Field(min_length=1, max_length=200), AfterValidator(valid_name)
]
"""Non-blank, storable, 1–200 chars — every write path's name rule."""

StrictEmail = Annotated[
    EmailStr, Field(max_length=320), AfterValidator(storable_text)
]
"""API-ingress/business email: EmailStr semantics and normalization (the
exact `valid_email` rule — EmailStr IS pydantic's validate_email) plus the
storability floor. Built on EmailStr rather than AfterValidator(valid_email)
so 422 error messages and the OpenAPI `format: email` stay byte-identical to
the previous `EmailStr = Field(max_length=320)` declaration."""

FloorEmail = Annotated[
    str, Field(min_length=3, max_length=320), AfterValidator(email_floor)
]
"""Consumer-path email floor: storable, contains '@', stored VERBATIM."""

StorableText = Annotated[str, AfterValidator(storable_text)]
"""Text PostgreSQL can store (no NUL); compose lengths per field, e.g.
`Annotated[StorableText, Field(max_length=2000)]`."""

StorableAttributes = Annotated[dict[str, Any], AfterValidator(storable_json)]
"""A JSON object PostgreSQL JSONB can store (no NUL, no NaN/Infinity)."""
