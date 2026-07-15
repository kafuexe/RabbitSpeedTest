"""Unit of Work: one transaction per API request / consumed message.

Owns the only commit/rollback in the system. Domain events are STAGED during
the transaction and handed to the injected EventPublisher strictly after a
successful commit — rollback discards them. This is the Outbox seam: an
OutboxPublisher that inserts rows instead of publishing would slot in here
with zero business-layer changes.

Consumer idempotency (`mark_event_processed`) is delivery infrastructure, so
it lives here rather than in any module's repository.
"""
from __future__ import annotations

import logging
from types import TracebackType
from typing import Callable, Protocol, Sequence

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database.inbox import ProcessedEvent
from app.messaging.cloudevents import CloudEvent
from app.messaging.protocols import EventPublisher

logger = logging.getLogger(__name__)


class UnitOfWork(Protocol):
    session: AsyncSession

    def stage_event(self, event: CloudEvent) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def mark_events_processed(
        self, pairs: Sequence[tuple[str, str]]
    ) -> set[tuple[str, str]]: ...
    async def __aenter__(self) -> "UnitOfWork": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]


class SqlAlchemyUnitOfWork:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        event_publisher: EventPublisher,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = event_publisher
        self._staged: list[CloudEvent] = []
        self._committed = False
        self.session: AsyncSession = None  # type: ignore[assignment]  # set in __aenter__

    async def __aenter__(self) -> "SqlAlchemyUnitOfWork":
        self.session = self._session_factory()
        self._staged = []
        self._committed = False
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if not self._committed:
                if exc_type is None:
                    # Clean read-only exit: detach instances BEFORE the
                    # rollback expires them, so business code can return ORM
                    # objects from read paths without a magic commit.
                    self.session.expunge_all()
                await self.session.rollback()
        finally:
            await self.session.close()

    def stage_event(self, event: CloudEvent) -> None:
        self._staged.append(event)

    async def commit(self) -> None:
        try:
            await self.session.commit()
        except Exception:
            if self._staged:
                # Ambiguous outcome: the database may have committed even
                # though the ack was lost. If it did, these events are gone
                # (nothing will re-stage them) — log loudly enough to page.
                logger.critical(
                    "commit failed with staged events; if the commit actually "
                    "applied, these events were NOT published",
                    extra={
                        "event_ids": [e.id for e in self._staged],
                        "event_types": sorted({e.type for e in self._staged}),
                    },
                )
            raise
        self._committed = True
        staged, self._staged = self._staged, []
        for event in staged:
            try:
                await self._publisher.publish_event(event)
            except Exception:
                # Commit already succeeded; the write must not be undone and
                # the caller must not see an error. Log loudly — this exact
                # gap is what the future Outbox implementation closes.
                logger.exception(
                    "event publish failed after commit",
                    extra={"event_id": event.id, "event_type": event.type},
                )

    async def rollback(self) -> None:
        await self.session.rollback()
        self._staged.clear()

    async def mark_events_processed(
        self, pairs: Sequence[tuple[str, str]]
    ) -> set[tuple[str, str]]:
        """Bulk inbox insert; returns the pairs that were NEW (everything
        else is a duplicate delivery). One statement for the whole batch."""
        unique = list(dict.fromkeys(pairs))
        stmt = (
            pg_insert(ProcessedEvent)
            .values([{"source": s, "event_id": e} for s, e in unique])
            .on_conflict_do_nothing(
                index_elements=[ProcessedEvent.source, ProcessedEvent.event_id]
            )
            .returning(ProcessedEvent.source, ProcessedEvent.event_id)
        )
        result = await self.session.execute(stmt)
        return {(row.source, row.event_id) for row in result}
