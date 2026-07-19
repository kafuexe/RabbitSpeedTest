"""Generic DAL for versioned entities — CRUD only, no business rules.

A module's repository subclasses `VersionedRepository`, declares its model
and query whitelists, and inherits the machinery every module needs:

- `insert_if_absent`   — idempotent create in one round trip
- `get_for_update`     — row lock so concurrent API updates serialize
- `upsert_if_newer_many` — atomic bulk upsert with the version guard as a
  SQL `WHERE`, evaluated by PostgreSQL, no locks
- `list`               — whitelisted filter/sort/paginate

Every method is an ordinary override point for modules that need different
SQL. Instances join the caller's session; the Unit of Work owns
commit/rollback.
"""
from __future__ import annotations

import uuid
from typing import Any, ClassVar, Generic, Sequence, TypeVar

from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.query import ListQuery

M = TypeVar("M")


class VersionedRepository(Generic[M]):
    """Data access for one versioned table.

    Subclasses declare:
    - `model`              — the ORM class; must have an `id` primary key and
                             a `version` column (the optimistic-concurrency /
                             event-ordering anchor)
    - `filterable_columns` / `sortable_columns` — the query whitelists, the
      single source of truth the business layer derives its allowed-field
      sets from
    """

    model: ClassVar[type[Any]]
    filterable_columns: ClassVar[dict[str, InstrumentedAttribute]]
    sortable_columns: ClassVar[dict[str, InstrumentedAttribute]]

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @classmethod
    def _row_values(cls, entity: M) -> dict[str, Any]:
        """The column values a write carries: every mapped column the
        application owns — i.e. not maintained by the server
        (created_at/updated_at style server defaults / onupdate)."""
        return {
            column.key: getattr(entity, column.key)
            for column in cls.model.__table__.columns
            if column.server_default is None and column.onupdate is None
        }

    async def get(self, entity_id: uuid.UUID) -> M | None:
        return await self._session.get(self.model, entity_id)

    async def get_for_update(self, entity_id: uuid.UUID) -> M | None:
        """Row-locked read: serializes concurrent updates of the same row."""
        stmt = select(self.model).where(self.model.id == entity_id).with_for_update()
        return await self._session.scalar(stmt)

    async def insert_if_absent(self, entity: M) -> M | None:
        """Idempotent insert keyed on id, one round trip: returns the freshly
        inserted row (server defaults populated via RETURNING), or None if a
        row with that id already exists."""
        stmt = (
            pg_insert(self.model)
            .values(**self._row_values(entity))
            .on_conflict_do_nothing(index_elements=[self.model.id])
            .returning(self.model)
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
        stmt = pg_insert(self.model).values(rows)
        set_ = {key: getattr(stmt.excluded, key) for key in rows[0] if key != "id"}
        set_["updated_at"] = func.now()
        stmt = stmt.on_conflict_do_update(
            index_elements=[self.model.id],
            set_=set_,
            where=self.model.version < stmt.excluded.version,
        )
        await self._session.execute(stmt)

    async def list(self, query: ListQuery) -> tuple[list[M], int]:
        # Two queries on purpose: a window count (count().over()) would drag
        # the ENTIRE filtered set through a WindowAgg before LIMIT — measured
        # 2.7-5.6x slower at 200k rows (kills index early-termination and
        # parallel scan) to save one sub-ms round trip.
        stmt: Select[tuple[M]] = select(self.model)
        for key, value in query.filters.items():
            column = self.filterable_columns.get(key)
            if column is None:
                raise InvalidQueryError(f"cannot filter by {key!r}")
            stmt = stmt.where(column == value)

        total = await self._session.scalar(
            select(func.count()).select_from(stmt.subquery())
        )

        sort_column = self.sortable_columns.get(query.sort.field)
        if sort_column is None:
            raise InvalidQueryError(f"cannot sort by {query.sort.field!r}")
        order = sort_column.desc() if query.sort.descending else sort_column.asc()
        # Tie-break on id for a deterministic page order.
        stmt = stmt.order_by(order, self.model.id.asc())
        stmt = stmt.limit(query.page.limit).offset(query.page.offset)

        rows = (await self._session.scalars(stmt)).all()
        return list(rows), int(total or 0)
