"""Classify database errors as permanent (retry can never succeed) or not.

Why this exists: catching ``sqlalchemy.exc.DataError`` is NOT enough. The
asyncpg dialect leaves most class-22 server errors (NUL in text, invalid
JSONB, value overflow) untranslated — they surface as the generic
``DBAPIError`` — so a class-based check silently never fires and a
deterministically-unstorable write becomes an infinite requeue loop. The
reliable signal is the SQLSTATE carried by the driver exception: class "22"
is "data exception", deterministic by definition.
"""
from __future__ import annotations

from sqlalchemy import exc as sa_exc

_PERMANENT_SQLSTATE_CLASSES = ("22",)  # data exception


def is_permanent_data_error(exc: BaseException) -> bool:
    """True when the database rejected the DATA itself — retrying the same
    input can never succeed. Transient failures (connection loss, timeouts,
    serialization) return False and should be retried."""
    if isinstance(exc, sa_exc.DataError):
        return True
    if isinstance(exc, sa_exc.DBAPIError):
        cause: BaseException | None = exc.orig
        while cause is not None:
            code = getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)
            if code:
                return str(code)[:2] in _PERMANENT_SQLSTATE_CLASSES
            cause = cause.__cause__
    return False
