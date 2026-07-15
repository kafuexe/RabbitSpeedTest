"""In-memory fakes implementing the service's ports, for mock tests.

FakeUnitOfWork mirrors the real UoW contract: staged events reach the
publisher only on commit; rollback (or missing commit) discards them; inbox
marks are TRANSACTIONAL — they join the shared inbox only on commit, exactly
like the real processed_events insert, so retry-after-failure tests behave
like the real system.

FakeUserRepository stores copies (like a database) so post-call mutation of
the caller's instance never leaks into the "stored" row, and upsert bumps
updated_at like the real ON CONFLICT SET does.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from app.messaging.cloudevents import CloudEvent
from app.modules.shared.query import ListQuery
from app.modules.user.model import User


class FakeEventPublisher:
    def __init__(self) -> None:
        self.published: list[CloudEvent] = []

    async def publish_event(self, event: CloudEvent) -> None:
        self.published.append(event)


class FakeMessageBus:
    """MessagePublisher + MessageConsumer port fake."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []

    async def publish(self, queue: str, body: bytes) -> None:
        self.messages.append((queue, body))

    async def consume(self, queue, handler):  # pragma: no cover - not used
        raise NotImplementedError


def _copy(user: User) -> User:
    clone = User(
        id=user.id,
        name=user.name,
        email=user.email,
        attributes=dict(user.attributes),
        version=user.version,
    )
    clone.created_at = user.created_at
    clone.updated_at = user.updated_at
    return clone


class FakeUserRepository:
    def __init__(self, store: dict[uuid.UUID, User]) -> None:
        self.store = store

    async def get(self, user_id: uuid.UUID) -> User | None:
        return self.store.get(user_id)

    async def get_for_update(self, user_id: uuid.UUID) -> User | None:
        return self.store.get(user_id)

    async def insert_if_absent(self, user: User) -> User | None:
        if user.id in self.store:
            return None
        stored = _copy(user)
        self._stamp(stored)
        self.store[user.id] = stored
        return stored

    async def upsert_if_newer_many(self, users) -> None:
        now = datetime.now(timezone.utc)
        for user in users:
            current = self.store.get(user.id)
            if current is None:
                stored = _copy(user)
                self._stamp(stored)
                self.store[user.id] = stored
            elif user.version > current.version:
                current.name = user.name
                current.email = user.email
                current.attributes = dict(user.attributes)
                current.version = user.version
                current.updated_at = now  # real upsert sets updated_at=now()

    async def list(self, query: ListQuery) -> tuple[list[User], int]:
        users = list(self.store.values())
        for key, value in query.filters.items():
            users = [u for u in users if getattr(u, key) == value]
        users.sort(
            key=lambda u: ((v := getattr(u, query.sort.field)) is None, v),
            reverse=query.sort.descending,
        )
        total = len(users)
        page = users[query.page.offset : query.page.offset + query.page.limit]
        return page, total

    @staticmethod
    def _stamp(user: User) -> None:
        now = datetime.now(timezone.utc)
        if user.created_at is None:
            user.created_at = now
        user.updated_at = now


class FakeUnitOfWork:
    def __init__(self, publisher: FakeEventPublisher, inbox: set[tuple[str, str]]) -> None:
        self._publisher = publisher
        self._inbox = inbox
        self._pending_inbox: set[tuple[str, str]] = set()
        self.session = None
        self.staged: list[CloudEvent] = []
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> "FakeUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self.committed:
            await self.rollback()

    def stage_event(self, event: CloudEvent) -> None:
        self.staged.append(event)

    async def commit(self) -> None:
        self.committed = True
        self._inbox.update(self._pending_inbox)  # inbox joins the transaction
        self._pending_inbox.clear()
        staged, self.staged = self.staged, []
        for event in staged:
            await self._publisher.publish_event(event)

    async def rollback(self) -> None:
        self.rolled_back = True
        self.staged.clear()
        self._pending_inbox.clear()

    async def mark_events_processed(self, pairs) -> set[tuple[str, str]]:
        seen = self._inbox | self._pending_inbox
        fresh = {p for p in pairs if p not in seen}
        self._pending_inbox.update(fresh)
        return fresh


class FakeWorld:
    """Everything a UserService needs, wired to in-memory state."""

    def __init__(self) -> None:
        self.publisher = FakeEventPublisher()
        self.inbox: set[tuple[str, str]] = set()
        self.store: dict[uuid.UUID, User] = {}
        self.uows: list[FakeUnitOfWork] = []

    def uow_factory(self) -> FakeUnitOfWork:
        uow = FakeUnitOfWork(self.publisher, self.inbox)
        self.uows.append(uow)
        return uow

    def repo_factory(self, session) -> FakeUserRepository:
        return FakeUserRepository(self.store)
