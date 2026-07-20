"""Generic DAL for versioned entities — CRUD only, no business rules.

There are no per-module repository subclasses: a repository is configured
by the ORM model it serves, `VersionedRepository(model, session)`, and its
query whitelists are derived from the model's column tags
(`mapped_column(info=q(filter=..., sort=...))` — see modules/shared/spec.py).
`id`, `version`, `created_at`, `updated_at` are always sortable.

Machinery every module gets:

- `insert_if_absent`   — idempotent create in one round trip
- `get_for_update`     — row lock so concurrent API updates serialize
- `upsert_if_newer_many` — atomic bulk upsert with the version guard as a
  SQL `WHERE`, evaluated by PostgreSQL, no locks
- `list`               — whitelisted filter/sort/paginate

Instances join the caller's session; the Unit of Work owns commit/rollback.
"""
from __future__ import annotations

import uuid
from functools import lru_cache
from typing import Any, Generic, Sequence, TypeVar

from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.filters import apply_filter
from app.modules.shared.query import ListQuery

M = TypeVar("M")

ALWAYS_SORTABLE = ("id", "version", "created_at", "updated_at")


@lru_cache(maxsize=None)
def query_columns(model: type[Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """(filterable, sortable) column maps derived from the model's `q()`
    tags — the single source of truth for both the repository's SQL and the
    service's allowed-field sets. Cached per model: repositories are
    constructed per unit of work, and the reflection pass must not be paid
    on every request/batch. Treat the returned dicts as read-only."""
    filterable: dict[str, Any] = {}
    sortable: dict[str, Any] = {}
    for column in model.__table__.columns:
        attr = getattr(model, column.key)
        if column.info.get("filter"):
            filterable[column.key] = attr
        if column.info.get("sort") or column.key in ALWAYS_SORTABLE:
            sortable[column.key] = attr
    return filterable, sortable


def derive_query_fields(model: type[Any]) -> tuple[frozenset[str], frozenset[str]]:
    """(filterable, sortable) field-name sets for whitelist checks."""
    filterable, sortable = query_columns(model)
    return frozenset(filterable), frozenset(sortable)


class VersionedRepository(Generic[M]):
    """Data access for one versioned table. The model must have an `id`
    primary key and a `version` column (the optimistic-concurrency /
    event-ordering anchor)."""

    def __init__(self, model: type[M], session: AsyncSession) -> None:
        self._model: type[Any] = model
        self._session = session
        # Shallow copies: the cached maps are shared process-wide, and a
        # per-instance mutation (test fake, tenant-scoped narrowing) must
        # never leak into every other repository.
        filterable, sortable = query_columns(model)
        self.filterable_columns = dict(filterable)
        self.sortable_columns = dict(sortable)

    def _row_values(self, module: M) -> dict[str, Any]:
        """The column values a write carries: every mapped column the
        application owns — i.e. not maintained by the server
        (created_at/updated_at style server defaults / onupdate)."""
        return {
            column.key: getattr(module, column.key)
            for column in self._model.__table__.columns
            if column.server_default is None and column.onupdate is None
        }

    async def get(self, module_id: uuid.UUID) -> M | None:
        return await self._session.get(self._model, module_id)

    async def get_for_update(self, module_id: uuid.UUID) -> M | None:
        """Row-locked read: serializes concurrent updates of the same row."""
        stmt = (
            select(self._model)
            .where(self._model.id == module_id)
            .with_for_update()
        )
        return await self._session.scalar(stmt)

    async def insert_if_absent(self, module: M) -> M | None:
        """Idempotent insert keyed on id, one round trip: returns the freshly
        inserted row (server defaults populated via RETURNING), or None if a
        row with that id already exists."""
        stmt = (
            pg_insert(self._model)
            .values(**self._row_values(module))
            .on_conflict_do_nothing(index_elements=[self._model.id])
            .returning(self._model)
        )
        return await self._session.scalar(stmt)

    async def upsert_if_newer_many(self, entities: Sequence[M]) -> None:
        """Atomic bulk upsert with a version guard, one statement: inserts
        missing rows, overwrites rows whose stored version is older, silently
        skips stale writes. No row locks needed — the guard is a WHERE clause
        evaluated by PostgreSQL. Callers must ensure ids are unique."""
        if not entities:
            return
        rows = [self._row_values(e) for e in entities]
        stmt = pg_insert(self._model).values(rows)
        set_ = {key: getattr(stmt.excluded, key) for key in rows[0] if key != "id"}
        set_["updated_at"] = func.now()
        stmt = stmt.on_conflict_do_update(
            index_elements=[self._model.id],
            set_=set_,
            where=self._model.version < stmt.excluded.version,
        )
        await self._session.execute(stmt)

    async def list(self, query: ListQuery) -> tuple[list[M], int]:
        # Two queries on purpose: a window count (count().over()) would drag
        # the ENTIRE filtered set through a WindowAgg before LIMIT — measured
        # 2.7-5.6x slower at 200k rows (kills index early-termination and
        # parallel scan) to save one sub-ms round trip.
        stmt: Select[tuple[M]] = select(self._model)
        for clause in query.filters:
            column = self.filterable_columns.get(clause.field)
            if column is None:
                raise InvalidQueryError(f"cannot filter by {clause.field!r}")
            stmt = stmt.where(apply_filter(column, clause))

        total = await self._session.scalar(
            select(func.count()).select_from(stmt.subquery())
        )

        sort_column = self.sortable_columns.get(query.sort.field)
        if sort_column is None:
            raise InvalidQueryError(f"cannot sort by {query.sort.field!r}")
        order = sort_column.desc() if query.sort.descending else sort_column.asc()
        # Tie-break on id for a deterministic page order.
        stmt = stmt.order_by(order, self._model.id.asc())
        stmt = stmt.limit(query.page.limit).offset(query.page.offset)

        rows = (await self._session.scalars(stmt)).all()
        return list(rows), int(total or 0)
