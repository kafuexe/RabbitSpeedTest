"""Generic business service for versioned entities.

The choreography every module shares — idempotent create with replay
re-announce, optimistic update, batched idempotent event application — is
written ONCE here. A module subclasses `VersionedEntityService`, declares
its identity (entity name, event types, query whitelists) and implements
three small hooks; everything else is inherited and remains overridable.

Validation is NOT choreography: the `*Data`/`*Changes` types are pydantic
models declared with the shared Annotated types (modules/shared/validation),
so every instance handed to these methods is valid by construction — there
are no validation calls here.

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
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Generic,
    Mapping,
    Protocol,
    Sequence,
    TypeVar,
)

from pydantic import BaseModel
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

logger = logging.getLogger(__name__)


class StateData(Protocol):
    """Full desired state of an entity — the minimum the generic choreography
    needs from a module's `*Data` model (a pydantic BaseModel, valid by
    construction)."""

    id: uuid.UUID
    version: int


M = TypeVar("M")  # ORM model
D = TypeVar("D", bound=StateData)  # full-state payload (create/event)
C = TypeVar("C", bound=BaseModel)  # partial-update model; None = unchanged


@dataclass(frozen=True)
class StateEventItem(Generic[D]):
    """One consumed event: identity for dedup + the state it announces."""

    event_id: str
    source: str
    data: D


class VersionedRepositoryPort(Protocol[M]):
    async def get(self, entity_id: uuid.UUID) -> M | None: ...
    async def get_for_update(self, entity_id: uuid.UUID) -> M | None: ...
    async def insert_if_absent(self, entity: M) -> M | None: ...
    async def upsert_if_newer_many(self, entities: Sequence[M]) -> None: ...
    async def list(self, query: ListQuery) -> tuple[list[M], int]: ...


class VersionedEntityService(Generic[M, D, C]):
    """Subclasses declare the class attributes below and implement the
    hooks at the bottom of this class. Public methods are the shared
    choreography; override one only when a module genuinely diverges."""

    entity_name: ClassVar[str]
    created_event_type: ClassVar[str]
    updated_event_type: ClassVar[str]
    default_sort: ClassVar[SortSpec]
    sortable_fields: ClassVar[frozenset[str]]
    filterable_fields: ClassVar[frozenset[str]]

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        repo_factory: Callable[[AsyncSession], VersionedRepositoryPort[M]],
        *,
        event_source: str,
        max_page_size: int,
    ) -> None:
        self._uow_factory = uow_factory
        self._repo_factory = repo_factory
        self._event_source = event_source
        self._max_page_size = max_page_size

    # ------------------------------------------------------------- API path

    async def create(self, data: D) -> tuple[M, bool]:
        """Idempotent create. Returns (entity, created). Replaying the same id
        with identical content returns the stored row AND re-announces its
        state event — so a create whose first attempt died in the ambiguous
        commit window still gets its created-event published on retry.
        Contradictory content for an existing id is a conflict."""
        async with self._uow_factory() as uow:
            repo = self._repo_factory(uow.session)
            entity = self._new_entity(data)
            entity.version = 1  # creates always start at 1, whatever the payload says
            created = await repo.insert_if_absent(entity)
            if created is None:
                existing = await repo.get(data.id)
                if existing is None:  # pragma: no cover - momentary race window
                    raise ConflictError(
                        f"{self.entity_name} {data.id} is being created concurrently"
                    )
                if not self._content_matches(existing, data):
                    raise ConflictError(
                        f"{self.entity_name} {data.id} already exists with "
                        "different content"
                    )
                # Re-announce: consumers dedup by version, so the duplicate
                # event is harmless, but a previously lost one is recovered.
                uow.stage_event(self._build_event(self.created_event_type, existing))
                await uow.commit()
                logger.info(
                    "create replayed",
                    extra={"entity": self.entity_name, "entity_id": str(data.id)},
                )
                return existing, False

            uow.stage_event(self._build_event(self.created_event_type, created))
            await uow.commit()
            logger.info(
                "%s created", self.entity_name, extra={"entity_id": str(data.id)}
            )
            return created, True

    async def update(
        self,
        entity_id: uuid.UUID,
        changes: C,
        *,
        expected_version: int | None = None,
    ) -> M:
        if all(
            getattr(changes, name) is None for name in type(changes).model_fields
        ):
            raise InvalidInputError("update must change at least one field")
        async with self._uow_factory() as uow:
            repo = self._repo_factory(uow.session)
            entity = await repo.get_for_update(entity_id)
            if entity is None:
                raise NotFoundError(f"{self.entity_name} {entity_id} not found")
            if expected_version is not None and entity.version != expected_version:
                raise ConflictError(
                    f"version conflict: expected {expected_version}, "
                    f"is {entity.version}"
                )
            self._apply_changes(entity, changes)
            entity.version += 1
            uow.stage_event(self._build_event(self.updated_event_type, entity))
            await uow.commit()
            logger.info(
                "%s updated",
                self.entity_name,
                extra={"entity_id": str(entity_id), "version": entity.version},
            )
            return entity

    async def get(self, entity_id: uuid.UUID) -> M:
        async with self._uow_factory() as uow:
            entity = await self._repo_factory(uow.session).get(entity_id)
            if entity is None:
                raise NotFoundError(f"{self.entity_name} {entity_id} not found")
            return entity

    async def list_page(
        self,
        *,
        limit: int,
        offset: int,
        sort: str | None = None,
        filters: Mapping[str, str | None] | None = None,
    ) -> Page[M]:
        query = ListQuery(
            page=make_page_request(limit, offset, max_limit=self._max_page_size),
            sort=parse_sort(sort, allowed=self.sortable_fields,
                            default=self.default_sort),
            filters=build_filters(filters or {}, allowed=self.filterable_fields),
        )
        async with self._uow_factory() as uow:
            items, total = await self._repo_factory(uow.session).list(query)
            return Page(items=items, total=total, limit=query.page.limit,
                        offset=query.page.offset)

    # -------------------------------------------------------- consumer path

    async def apply_state_events(self, items: Sequence[StateEventItem[D]]) -> None:
        """Apply a batch of externally-announced entity states in ONE
        transaction. Idempotent and order-safe, atomically:

        - duplicate delivery   → bulk inbox insert filters it out
        - within-batch races   → highest version per entity wins
        - update before create → row upserted from the event's full state
        - stale/out-of-order   → version guard in the upsert skips it
        The inbox rows commit with the data, so redeliveries stay no-ops;
        nothing is acked (the batcher resolves submits) until this commits.
        """
        async with self._uow_factory() as uow:
            fresh = await uow.mark_events_processed(
                [(i.source, i.event_id) for i in items]
            )
            winners: dict[uuid.UUID, D] = {}
            for item in items:
                if (item.source, item.event_id) not in fresh:
                    continue
                current = winners.get(item.data.id)
                if current is None or item.data.version > current.version:
                    winners[item.data.id] = item.data
            if winners:
                repo = self._repo_factory(uow.session)
                await repo.upsert_if_newer_many(
                    [self._new_entity(d) for d in winners.values()]
                )
            await uow.commit()
            logger.info(
                "%s events applied",
                self.entity_name,
                extra={
                    "batch": len(items),
                    "fresh": len(fresh),
                    "duplicates": len(items) - len(fresh),
                    "written": len(winners),
                },
            )
            # Per-event traceability without paying for it on the hot path:
            # logging only defers formatting, not argument construction, so
            # the id list must be guarded, not just passed to debug().
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "%s events applied (detail)",
                    self.entity_name,
                    extra={"event_ids": [i.event_id for i in items]},
                )

    # ----------------------------------------------------------- the hooks
    # There are no validation hooks: `*Data`/`*Changes` are pydantic models
    # built from the SHARED Annotated types in modules/shared/validation.py
    # (the same rules the API schemas and event payloads run), so anything
    # that reaches these methods is already valid and no write path can
    # drift.

    def _new_entity(self, data: D) -> M:
        """Build an ORM instance from full state, honoring `data.version`
        (the consumer path upserts at the announced version)."""
        raise NotImplementedError

    def _content_matches(self, entity: M, data: D) -> bool:
        """Replay equality: is the stored row the same content this create
        announces? (Versions/timestamps are not content.)"""
        raise NotImplementedError

    def _build_event(self, event_type: str, entity: M) -> "CloudEvent":
        """Full-state event announcing this entity, from `self._event_source`."""
        raise NotImplementedError

    def _apply_changes(self, entity: M, changes: C) -> None:
        """Copy every non-None changes field onto the entity. The default
        assumes changes field names match model attributes; override when
        they don't."""
        for name in type(changes).model_fields:
            value = getattr(changes, name)
            if value is None:
                continue
            setattr(entity, name, dict(value) if isinstance(value, dict) else value)
