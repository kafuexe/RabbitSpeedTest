"""Business validation rules shared by API schemas, event payloads, and the
business floor — ONE definition per rule, so the write paths cannot drift.

Storability primitives live in `app.database.storable` (they are facts about
PostgreSQL); they are re-exported here so business-layer code has a single
import point for all validation.

Email policy — a DELIBERATE asymmetry:
- The API ingress is STRICT (`valid_email`, the exact rule EmailStr runs):
  a client submitting a bad address gets a 422 and can correct it.
- The consumer path is PERMISSIVE (storable + minimal '@' shape, verbatim):
  consumed events carry FULL-STATE announcements from an authoritative
  producer, and rejecting one over email syntax would permanently freeze the
  replica at the previous version (every later event for that user carries
  the same email). Replication fidelity beats re-adjudicating validity; only
  genuinely unstorable data (NUL/NaN) is rejected there.
"""
from __future__ import annotations

from pydantic.networks import validate_email as _pydantic_validate_email

from app.database.storable import storable_json, storable_text

__all__ = ["storable_json", "storable_text", "valid_name", "valid_email",
           "email_floor"]


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
