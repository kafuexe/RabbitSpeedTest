"""Declarative base with a stable naming convention (needed by Alembic)."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, MetaData, Uuid, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class VersionedBase(Base):
    """Base for every module's table: the exact columns the versioned
    repository contract requires (and ModuleSpec.__post_init__ enforces) —
    the uuid primary key, the optimistic-concurrency `version`, and the
    server-maintained timestamps. Subclasses add `__tablename__` and their
    business columns. `eager_defaults` fetches the server defaults via
    RETURNING at flush time so instances stay complete after the session
    closes."""

    __abstract__ = True
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
