"""User business service.

Framework-free: no FastAPI, no RabbitMQ imports. Both the API and the
consumer call these methods; which EventPublisher the injected UnitOfWork
carries (real vs null) is the composition root's decision — that is how
"publish after commit" and "consumer never republishes" are both enforced
without a single `if` in here.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Protocol, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    from app.messaging.cloudevents import CloudEvent

from app.database.unit_of_work import UnitOfWorkFactory
from app.modules.shared.errors import ConflictError, InvalidInputError, NotFoundError
from app.modules.shared.query import (
    ListQuery,
    Page,
    SortSpec,
    build_filters,
    make_page_request,
    parse_sort,
)
from app.modules.user.model import User
from app.modules.user.repository import FILTERABLE_COLUMNS, SORTABLE_COLUMNS

logger = logging.getLogger(__name__)

SORTABLE_FIELDS = frozenset(SORTABLE_COLUMNS)
FILTERABLE_FIELDS = frozenset(FILTERABLE_COLUMNS)
DEFAULT_SORT = SortSpec(field="created_at", descending=True)


@dataclass(frozen=True)
class UserData:
    """Full desired state of a user (create payload / event payload)."""

    id: uuid.UUID
    name: str
    email: str
    attributes: dict[str, Any]
    version: int = 1


@dataclass(frozen=True)
class UserChanges:
    """Partial update; None means "leave unchanged"."""

    name: str | None = None
    email: str | None = None
    attributes: dict[str, Any] | None = None


@dataclass(frozen=True)
class UserEventItem:
    """One consumed event: identity for dedup + the state it announces."""

    event_id: str
    source: str
    data: UserData


class UserRepositoryPort(Protocol):
    async def get(self, user_id: uuid.UUID) -> User | None: ...
    async def get_for_update(self, user_id: uuid.UUID) -> User | None: ...
    async def insert_if_absent(self, user: User) -> User | None: ...
    async def upsert_if_newer_many(self, users: Sequence[User]) -> None: ...
    async def list(self, query: ListQuery) -> tuple[list[User], int]: ...


class UserService:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        repo_factory: Callable[[AsyncSession], UserRepositoryPort],
        *,
        event_source: str,
        max_page_size: int,
    ) -> None:
        self._uow_factory = uow_factory
        self._repo_factory = repo_factory
        self._event_source = event_source
        self._max_page_size = max_page_size

    # ------------------------------------------------------------- API path

    async def create_user(self, data: UserData) -> tuple[User, bool]:
        """Idempotent create. Returns (user, created). Replaying the same id
        with identical content returns the stored row AND re-announces its
        state event — so a create whose first attempt died in the ambiguous
        commit window still gets its user.created published on retry.
        Contradictory content for an existing id is a conflict."""
        self._validate_name(data.name)
        self._validate_email(data.email)
        async with self._uow_factory() as uow:
            repo = self._repo_factory(uow.session)
            user = await repo.insert_if_absent(
                User(
                    id=data.id,
                    name=data.name,
                    email=data.email,
                    attributes=dict(data.attributes),
                    version=1,
                )
            )
            if user is None:
                existing = await repo.get(data.id)
                if existing is None:  # pragma: no cover - momentary race window
                    raise ConflictError(f"user {data.id} is being created concurrently")
                if (existing.name, existing.email, existing.attributes) != (
                    data.name, data.email, data.attributes,
                ):
                    raise ConflictError(
                        f"user {data.id} already exists with different content"
                    )
                # Re-announce: consumers dedup by version, so the duplicate
                # event is harmless, but a previously lost one is recovered.
                uow.stage_event(self._state_event("user.created", existing))
                await uow.commit()
                logger.info("create replayed", extra={"user_id": str(data.id)})
                return existing, False

            uow.stage_event(self._state_event("user.created", user))
            await uow.commit()
            logger.info("user created", extra={"user_id": str(data.id)})
            return user, True

    async def update_user(
        self,
        user_id: uuid.UUID,
        changes: UserChanges,
        *,
        expected_version: int | None = None,
    ) -> User:
        if changes.name is None and changes.email is None and changes.attributes is None:
            raise InvalidInputError("update must change at least one field")
        if changes.name is not None:
            self._validate_name(changes.name)
        if changes.email is not None:
            self._validate_email(changes.email)
        async with self._uow_factory() as uow:
            repo = self._repo_factory(uow.session)
            user = await repo.get_for_update(user_id)
            if user is None:
                raise NotFoundError(f"user {user_id} not found")
            if expected_version is not None and user.version != expected_version:
                raise ConflictError(
                    f"version conflict: expected {expected_version}, is {user.version}"
                )
            if changes.name is not None:
                user.name = changes.name
            if changes.email is not None:
                user.email = changes.email
            if changes.attributes is not None:
                user.attributes = dict(changes.attributes)
            user.version += 1
            uow.stage_event(self._state_event("user.updated", user))
            await uow.commit()
            logger.info(
                "user updated",
                extra={"user_id": str(user_id), "version": user.version},
            )
            return user

    async def get_user(self, user_id: uuid.UUID) -> User:
        async with self._uow_factory() as uow:
            user = await self._repo_factory(uow.session).get(user_id)
            if user is None:
                raise NotFoundError(f"user {user_id} not found")
            return user

    async def list_users(
        self,
        *,
        limit: int,
        offset: int,
        sort: str | None = None,
        name: str | None = None,
        email: str | None = None,
    ) -> Page[User]:
        query = ListQuery(
            page=make_page_request(limit, offset, max_limit=self._max_page_size),
            sort=parse_sort(sort, allowed=SORTABLE_FIELDS, default=DEFAULT_SORT),
            filters=build_filters(
                {"name": name, "email": email}, allowed=FILTERABLE_FIELDS
            ),
        )
        async with self._uow_factory() as uow:
            items, total = await self._repo_factory(uow.session).list(query)
            return Page(items=items, total=total, limit=query.page.limit,
                        offset=query.page.offset)

    # -------------------------------------------------------- consumer path

    async def apply_user_events(self, items: Sequence[UserEventItem]) -> None:
        """Apply a batch of externally-announced user states in ONE
        transaction. Idempotent and order-safe, atomically:

        - duplicate delivery   → bulk inbox insert filters it out
        - within-batch races   → highest version per user wins
        - update before create → row upserted from the event's full state
        - stale/out-of-order   → version guard in the upsert skips it
        The inbox rows commit with the data, so redeliveries stay no-ops;
        nothing is acked (the batcher resolves submits) until this commits.
        """
        async with self._uow_factory() as uow:
            fresh = await uow.mark_events_processed(
                [(i.source, i.event_id) for i in items]
            )
            winners: dict[uuid.UUID, UserData] = {}
            for item in items:
                if (item.source, item.event_id) not in fresh:
                    continue
                current = winners.get(item.data.id)
                if current is None or item.data.version > current.version:
                    winners[item.data.id] = item.data
            if winners:
                repo = self._repo_factory(uow.session)
                await repo.upsert_if_newer_many([
                    User(
                        id=d.id,
                        name=d.name,
                        email=d.email,
                        attributes=dict(d.attributes),
                        version=d.version,
                    )
                    for d in winners.values()
                ])
            await uow.commit()
            logger.info(
                "user events applied",
                extra={
                    "batch": len(items),
                    "fresh": len(fresh),
                    "duplicates": len(items) - len(fresh),
                    "users_written": len(winners),
                },
            )
            # Per-event traceability without paying for it on the hot path.
            logger.debug(
                "user events applied (detail)",
                extra={"event_ids": [i.event_id for i in items]},
            )

    # ------------------------------------------------------------- internal
    # These are the business floor for user data, enforced on every write
    # path (API via create/update; the consumer path enforces the same rules
    # in UserEventData so nothing the API would reject gets stored).

    def _validate_name(self, name: str) -> None:
        if not name.strip():
            raise InvalidInputError("name must not be empty")

    def _validate_email(self, email: str) -> None:
        if "@" not in email:
            raise InvalidInputError("email must be a valid address")

    def _state_event(self, event_type: str, user: User) -> "CloudEvent":
        # Local import keeps business.py free of a hard edge on the event
        # module at import time while events.py imports UserData from here.
        from app.modules.user.events import build_user_event

        return build_user_event(event_type, user, source=self._event_source)
