"""CloudEvents 1.0 envelope in structured JSON mode.

SimpleClient handlers receive raw bytes only (no AMQP headers), so every
CloudEvents attribute travels inside the JSON body — spec-compliant
"structured content mode".

Attribute size limits match the processed_events inbox columns (String(255));
an oversized id/source is rejected as an invalid envelope (logged + acked)
instead of failing the inbox INSERT on every redelivery.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class InvalidCloudEvent(ValueError):
    """The message body is not a valid CloudEvents 1.0 envelope."""


def validation_error_reason(exc: ValidationError) -> str:
    """Loggable summary of a ValidationError WITHOUT input values.

    str(exc) embeds ``input_value=...`` — the rejected payload (names,
    emails). Logs must never contain PII, so only locations and error
    messages are surfaced.
    """
    return "; ".join(
        f"{'.'.join(str(part) for part in err['loc']) or '<root>'}: {err['msg']}"
        for err in exc.errors(include_url=False, include_input=False)
    )


class CloudEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    specversion: str = "1.0"
    id: str = Field(min_length=1, max_length=255)
    source: str = Field(min_length=1, max_length=255)
    type: str = Field(min_length=1, max_length=255)
    time: datetime | None = None
    datacontenttype: str = "application/json"
    data: dict[str, Any] = Field(default_factory=dict)
    # CloudEvents extension attribute for cross-service tracing.
    correlationid: str | None = Field(default=None, max_length=255)

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, body: bytes) -> "CloudEvent":
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidCloudEvent(f"body is not valid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise InvalidCloudEvent("body is not a JSON object")
        if payload.get("specversion") != "1.0":
            raise InvalidCloudEvent(
                f"unsupported specversion: {payload.get('specversion')!r}"
            )
        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            raise InvalidCloudEvent(validation_error_reason(exc)) from exc


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
