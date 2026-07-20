import pytest

from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.query import (
    SortSpec,
    make_page_request,
    parse_sort,
)

ALLOWED = frozenset({"name", "created_at"})
DEFAULT = SortSpec(field="created_at", descending=True)


def test_parse_sort_default_and_directions():
    assert parse_sort(None, allowed=ALLOWED, default=DEFAULT) == DEFAULT
    assert parse_sort("name", allowed=ALLOWED, default=DEFAULT) == SortSpec("name", False)
    assert parse_sort("-name", allowed=ALLOWED, default=DEFAULT) == SortSpec("name", True)


def test_parse_sort_rejects_unknown_field():
    with pytest.raises(InvalidQueryError):
        parse_sort("password", allowed=ALLOWED, default=DEFAULT)


def test_page_request_bounds():
    assert make_page_request(10, 0, max_limit=100).limit == 10
    with pytest.raises(InvalidQueryError):
        make_page_request(0, 0, max_limit=100)
    with pytest.raises(InvalidQueryError):
        make_page_request(101, 0, max_limit=100)  # large pagination rejected
    with pytest.raises(InvalidQueryError):
        make_page_request(10, -1, max_limit=100)


# Filter whitelisting/operators moved to app/modules/shared/filters.py —
# see tests/unit/test_filters.py.
