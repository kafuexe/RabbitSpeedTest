"""User DAL — CRUD only, no business rules. Joins the caller's session; the
Unit of Work owns commit/rollback."""
from __future__ import annotations

import uuid
from typing import Sequence

from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.query import ListQuery
from app.modules.user.model import User

# Single source of truth for query whitelists; the business layer derives
# its allowed-field sets from these keys.
FILTERABLE_COLUMNS: dict[str, InstrumentedAttribute] = {
    "name": User.name,
    "email": User.email,
}
SORTABLE_COLUMNS: dict[str, InstrumentedAttribute] = {
    "id": User.id,
    "name": User.name,
    "email": User.email,
    "version": User.version,
    "created_at": User.created_at,
    "updated_at": User.updated_at,
}


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, user_id: uuid.UUID) -> User | None:
        return await self._session.get(User, user_id)

    async def get_for_update(self, user_id: uuid.UUID) -> User | None:
        """Row-locked read: serializes concurrent updates of the same user."""
        stmt = select(User).where(User.id == user_id).with_for_update()
        return await self._session.scalar(stmt)

    async def insert_if_absent(self, user: User) -> User | None:
        """Idempotent insert keyed on id, one round trip: returns the freshly
        inserted row (server defaults populated via RETURNING), or None if a
        row with that id already exists."""
        stmt = (
            pg_insert(User)
            .values(
                id=user.id,
                name=user.name,
                email=user.email,
                attributes=user.attributes,
                version=user.version,
            )
            .on_conflict_do_nothing(index_elements=[User.id])
            .returning(User)
        )
        return await self._session.scalar(stmt)

    async def upsert_if_newer_many(self, users: Sequence[User]) -> None:
        """Atomic bulk upsert with a version guard, one statement: inserts
        missing rows, overwrites rows whose stored version is older, silently
        skips stale writes. No row locks needed — the guard is a WHERE clause
        evaluated by PostgreSQL. Callers must ensure ids are unique."""
        if not users:
            return
        stmt = pg_insert(User).values([
            {
                "id": u.id,
                "name": u.name,
                "email": u.email,
                "attributes": u.attributes,
                "version": u.version,
            }
            for u in users
        ])
        stmt = stmt.on_conflict_do_update(
            index_elements=[User.id],
            set_={
                "name": stmt.excluded.name,
                "email": stmt.excluded.email,
                "attributes": stmt.excluded.attributes,
                "version": stmt.excluded.version,
                "updated_at": func.now(),
            },
            where=User.version < stmt.excluded.version,
        )
        await self._session.execute(stmt)

    async def list(self, query: ListQuery) -> tuple[list[User], int]:
        # Two queries on purpose: a window count (count().over()) would drag
        # the ENTIRE filtered set through a WindowAgg before LIMIT — measured
        # 2.7-5.6x slower at 200k rows (kills index early-termination and
        # parallel scan) to save one sub-ms round trip.
        stmt: Select[tuple[User]] = select(User)
        for key, value in query.filters.items():
            column = FILTERABLE_COLUMNS.get(key)
            if column is None:
                raise InvalidQueryError(f"cannot filter by {key!r}")
            stmt = stmt.where(column == value)

        total = await self._session.scalar(
            select(func.count()).select_from(stmt.subquery())
        )

        sort_column = SORTABLE_COLUMNS.get(query.sort.field)
        if sort_column is None:
            raise InvalidQueryError(f"cannot sort by {query.sort.field!r}")
        order = sort_column.desc() if query.sort.descending else sort_column.asc()
        # Tie-break on id for a deterministic page order.
        stmt = stmt.order_by(order, User.id.asc())
        stmt = stmt.limit(query.page.limit).offset(query.page.offset)

        rows = (await self._session.scalars(stmt)).all()
        return list(rows), int(total or 0)
