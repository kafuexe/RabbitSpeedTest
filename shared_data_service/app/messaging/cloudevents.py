"""CloudEvents 1.0 envelope in structured JSON mode.

RabbitClient handlers receive raw bytes only (no AMQP headers), so every
CloudEvents attribute travels inside the JSON body — spec-compliant
"structured content mode".

Attribute size limits match the processed_events inbox columns (String(255));
an oversized id/source is rejected as an invalid envelope (logged + acked)
instead of failing the inbox INSERT on every redelivery.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.database.storable import storable_text


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

    # Literal (not a pre-parse check) so an unsupported version is rejected
    # through validation_error_reason like every other field — the raw value
    # never reaches a log. The default serves internal CONSTRUCTION only;
    # from_bytes requires the key to be present (CloudEvents 1.0 does too).
    specversion: Literal["1.0"] = "1.0"
    id: str = Field(min_length=1, max_length=255)
    source: str = Field(min_length=1, max_length=255)
    type: str = Field(min_length=1, max_length=255)
    time: datetime | None = None
    datacontenttype: str = "application/json"
    data: dict[str, Any] = Field(default_factory=dict)
    # CloudEvents extension attribute for cross-service tracing.
    correlationid: str | None = Field(default=None, max_length=255)

    @model_validator(mode="before")
    @classmethod
    def _wire_requires_specversion(cls, data: Any, info: ValidationInfo) -> Any:
        if (
            info.context is not None
            and info.context.get("wire")
            and isinstance(data, dict)
            and "specversion" not in data
        ):
            raise ValueError("specversion is required")
        return data

    @field_validator("id", "source", "type", "correlationid")
    @classmethod
    def _storable(cls, value: str | None) -> str | None:
        # id/source land verbatim in processed_events text columns; a NUL
        # (arrives as the valid JSON escape \\u0000) would fail that INSERT
        # on every redelivery. Reject it as an invalid envelope instead.
        return value if value is None else storable_text(value)

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, body: bytes) -> "CloudEvent":
        # Single pass through pydantic-core's JSON parser; malformed JSON,
        # non-object bodies and bad fields all surface as ValidationError
        # and are summarized WITHOUT input values.
        try:
            return cls.model_validate_json(body, context={"wire": True})
        except ValidationError as exc:
            raise InvalidCloudEvent(validation_error_reason(exc)) from exc


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
