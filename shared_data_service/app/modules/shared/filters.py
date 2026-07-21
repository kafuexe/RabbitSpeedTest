"""Django-style filter lookups for the generic list path.

Query params are `field__op=value` (bare `field=value` means `exact`), e.g.
`?name__icontains=ada&version__gte=3&email__in=a@x.com,b@x.com`. The field
must be q(filter=True)-tagged (whitelist), the operator must be one of
LOOKUPS, and the value is coerced to the column's Python type — anything
else is a 400 (InvalidQueryError), never raw SQL.

Because the operator lives in the param NAME, these params are dynamic
(read from the request, not a statically-declared model). That is a
deliberate, approved departure from the static-filter rule for the sake of
the Django-style surface; only the list endpoint reads them, and every
field/op/value is validated here before touching SQL.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Mapping

from sqlalchemy import ColumnElement
from sqlalchemy.orm import InstrumentedAttribute

from app.modules.shared.errors import InvalidQueryError

# Operators that require a text column (LIKE/ILIKE).
TEXT_LOOKUPS = frozenset(
    {
        "iexact",
        "contains",
        "icontains",
        "startswith",
        "istartswith",
        "endswith",
        "iendswith",
    }
)
LOOKUPS = TEXT_LOOKUPS | frozenset(
    {
        "exact",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "not_in",
        "isnull",
        "not_isnull",
        "range",
    }
)
DEFAULT_LOOKUP = "exact"


@dataclass(frozen=True)
class FilterClause:
    field: str
    op: str
    value: str  # raw; coerced against the column type in apply_filter


def parse_filter_params(
    raw: Mapping[str, str], *, allowed: frozenset[str]
) -> list[FilterClause]:
    """Parse `field__op` query params into clauses, validating field against
    the whitelist and op against LOOKUPS. Pagination params must already be
    stripped by the caller."""
    clauses: list[FilterClause] = []
    for key, value in raw.items():
        field, _, op = key.partition("__")
        op = op or DEFAULT_LOOKUP
        if field not in allowed:
            raise InvalidQueryError(
                f"cannot filter by {field!r}; allowed: {', '.join(sorted(allowed))}"
            )
        if op not in LOOKUPS:
            raise InvalidQueryError(
                f"unknown filter operator {op!r} on {field!r}; "
                f"allowed: {', '.join(sorted(LOOKUPS))}"
            )
        clauses.append(FilterClause(field=field, op=op, value=value))
    return clauses


_LIKE_ESCAPE = str.maketrans({"\\": r"\\", "%": r"\%", "_": r"\_"})


def _like(value: str) -> str:
    """Escape LIKE metacharacters so a user value matches literally."""
    return value.translate(_LIKE_ESCAPE)


_TRUTHY = frozenset({"true", "1", "yes", ""})

_COERCERS: dict[type, Callable[[str], Any]] = {
    uuid.UUID: uuid.UUID,
    bool: lambda v: v.strip().lower() in _TRUTHY,
    datetime: datetime.fromisoformat,
    date: date.fromisoformat,
    int: int,
    float: float,
}


def _coerce(value: str, python_type: type) -> Any:
    coercer = _COERCERS.get(python_type)
    if coercer is None:
        return value  # str (and any other) pass through
    try:
        return coercer(value)
    except (ValueError, TypeError) as exc:
        raise InvalidQueryError(
            f"invalid value {value!r} for this field: {exc}"
        ) from exc


# Text pattern per operator (before LIKE/ILIKE); `i*` variants ⇒ ILIKE.
_PATTERNS: dict[str, Callable[[str], str]] = {
    "iexact": lambda v: _like(v),
    "contains": lambda v: f"%{_like(v)}%",
    "icontains": lambda v: f"%{_like(v)}%",
    "startswith": lambda v: f"{_like(v)}%",
    "istartswith": lambda v: f"{_like(v)}%",
    "endswith": lambda v: f"%{_like(v)}",
    "iendswith": lambda v: f"%{_like(v)}",
}
_COMPARATORS: dict[str, Callable[[Any, Any], ColumnElement[bool]]] = {
    "exact": lambda c, x: c == x,
    "gt": lambda c, x: c > x,
    "gte": lambda c, x: c >= x,
    "lt": lambda c, x: c < x,
    "lte": lambda c, x: c <= x,
}


def apply_filter(
    column: InstrumentedAttribute[Any], clause: FilterClause
) -> ColumnElement[bool]:
    """Build the SQLAlchemy WHERE expression for one clause."""
    op, v = clause.op, clause.value
    py = column.type.python_type

    if op in TEXT_LOOKUPS and py is not str:
        raise InvalidQueryError(
            f"operator {op!r} needs a text field, not {clause.field!r}"
        )

    if op in _PATTERNS:
        pattern = _PATTERNS[op](v)
        return column.ilike(pattern) if op.startswith("i") else column.like(pattern)
    if op in _COMPARATORS:
        return _COMPARATORS[op](column, _coerce(v, py))
    if op == "in":
        return column.in_([_coerce(x, py) for x in v.split(",")])
    if op == "not_in":
        return column.notin_([_coerce(x, py) for x in v.split(",")])
    if op == "isnull":
        return column.is_(None) if v.strip().lower() in _TRUTHY else column.isnot(None)
    if op == "not_isnull":
        return column.isnot(None) if v.strip().lower() in _TRUTHY else column.is_(None)
    if op == "range":
        parts = v.split(",")
        if len(parts) != 2:
            raise InvalidQueryError(
                f"range needs two comma-separated values, got {v!r}"
            )
        return column.between(_coerce(parts[0], py), _coerce(parts[1], py))
    raise InvalidQueryError(f"unknown operator {op!r}")  # pragma: no cover
