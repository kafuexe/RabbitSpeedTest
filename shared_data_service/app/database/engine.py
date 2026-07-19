"""Async engine and session factory construction (wired by bootstrap only)."""
from __future__ import annotations

import ssl

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config.settings import Settings


def _ssl_context(ca_file: str) -> ssl.SSLContext | None:
    # Loading here (at startup) makes an unreadable/garbage CA bundle fail
    # the process immediately instead of at the first connection attempt.
    return ssl.create_default_context(cafile=ca_file) if ca_file else None


def create_engine(settings: Settings) -> AsyncEngine:
    connect_args = {}
    ctx = _ssl_context(settings.db_ca_file)
    if ctx is not None:
        connect_args["ssl"] = ctx
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
