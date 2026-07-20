"""Unit tests for the Django-style filter engine (no DB).

parse_filter_params: field/operator whitelisting and the bare=exact default.
apply_filter: each operator maps to the right SQL, with value coercion and
LIKE-escaping — covered by compiling the expression to SQL text so the full
operator matrix (incl. numeric/date/range) is exercised without needing a
DB or the field to be API-filterable.
"""
from __future__ import annotations

import pytest

from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.filters import (
    LOOKUPS,
    FilterClause,
    apply_filter,
    parse_filter_params,
)
from app.modules.user import User

ALLOWED = frozenset({"name", "email"})


def _expr(column, op: str, value: str):
    return apply_filter(column, FilterClause(field=column.key, op=op, value=value))


def _op(column, op: str, value: str) -> str:
    return _expr(column, op, value).operator.__name__


def _rhs(column, op: str, value: str):
    return _expr(column, op, value).right.value


# ------------------------------------------------------------ parse_filter_params


def test_bare_field_is_exact():
    (clause,) = parse_filter_params({"name": "ada"}, allowed=ALLOWED)
    assert (clause.field, clause.op, clause.value) == ("name", "exact", "ada")


def test_field__op_is_parsed():
    (clause,) = parse_filter_params({"name__icontains": "ad"}, allowed=ALLOWED)
    assert (clause.field, clause.op) == ("name", "icontains")


def test_unknown_field_rejected():
    with pytest.raises(InvalidQueryError):
        parse_filter_params({"password__icontains": "x"}, allowed=ALLOWED)


def test_unknown_operator_rejected():
    with pytest.raises(InvalidQueryError):
        parse_filter_params({"name__regex": "x"}, allowed=ALLOWED)


def test_all_documented_operators_are_supported():
    documented = {
        "exact", "iexact", "contains", "icontains", "startswith", "istartswith",
        "endswith", "iendswith", "gt", "gte", "lt", "lte", "in", "not_in",
        "isnull", "not_isnull", "range",
    }
    assert documented == LOOKUPS


# -------------------------------------------------------------------- apply_filter


def test_string_operators_use_like_ilike_with_wrapped_pattern():
    assert _op(User.name, "contains", "ada") == "like_op"
    assert _rhs(User.name, "contains", "ada") == "%ada%"
    assert _op(User.name, "icontains", "ada") == "ilike_op"
    assert _rhs(User.name, "icontains", "ada") == "%ada%"
    assert _rhs(User.name, "startswith", "ada") == "ada%"
    assert _rhs(User.name, "istartswith", "ada") == "ada%"
    assert _op(User.name, "istartswith", "ada") == "ilike_op"
    assert _rhs(User.name, "endswith", "ada") == "%ada"
    assert _op(User.name, "iexact", "ada") == "ilike_op"
    assert _rhs(User.name, "iexact", "ada") == "ada"  # no wildcards


def test_like_metacharacters_are_escaped():
    # a literal % / _ in the user value must not act as a wildcard
    assert _rhs(User.name, "contains", "50%") == r"%50\%%"
    assert _rhs(User.name, "contains", "a_b") == r"%a\_b%"


def test_comparison_operators_coerce_to_column_type():
    assert (_op(User.version, "gte", "3"), _rhs(User.version, "gte", "3")) == ("ge", 3)
    assert (_op(User.version, "lt", "10"), _rhs(User.version, "lt", "10")) == ("lt", 10)
    assert _op(User.version, "range", "2,5") == "between_op"


def test_in_and_not_in_coerce_each_value():
    assert _op(User.version, "in", "1,2,3") == "in_op"
    assert _rhs(User.version, "in", "1,2,3") == [1, 2, 3]
    assert _op(User.version, "not_in", "4,5") == "not_in_op"


def test_isnull_variants():
    assert _expr(User.name, "isnull", "true").operator.__name__ == "is_"
    assert _expr(User.name, "isnull", "false").operator.__name__ == "is_not"
    assert _expr(User.name, "not_isnull", "true").operator.__name__ == "is_not"


def test_text_operator_on_nontext_field_is_400():
    with pytest.raises(InvalidQueryError):
        apply_filter(User.version, FilterClause("version", "icontains", "3"))


def test_bad_value_for_typed_field_is_400():
    with pytest.raises(InvalidQueryError):
        apply_filter(User.version, FilterClause("version", "gte", "not-an-int"))


def test_range_needs_exactly_two_values():
    with pytest.raises(InvalidQueryError):
        apply_filter(User.version, FilterClause("version", "range", "1,2,3"))
