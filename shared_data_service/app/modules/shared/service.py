"""Generic business service for versioned entities.

The choreography every module shares — idempotent create with replay
re-announce, optimistic update, batched idempotent event application — is
written ONCE here, and since the hooks now have generic default
implementations driven by the module's EntitySpec, no subclass is required:
the default service is instantiated from the spec alone. A module that
genuinely diverges subclasses `VersionedEntityService`, overrides a hook
(each default is `super()`-callable), and passes itself as
`spec.service_cls`.

Validation is NOT choreography: the `*Data`/`*Update` types are pydantic
models declared with the shared Annotated types (modules/shared/validation),
so every instance handed to these methods is valid by construction. The
spec's optional `field_validators` mapping exists for rules that cannot be
an Annotated type; it defaults to empty.

Framework-free: no FastAPI, no RabbitMQ imports. Both the API and the
consumer call these methods; which EventPublisher the injected UnitOfWork
carries (real vs null) is the composition root's decision — that is how
"publish after commit" and "consumer never republishes" are both enforced
without a single `if` in here.
"""
from __future__ import annotations

import logging
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Mapping,
    Protocol,
    Sequence,
    cast,
)

from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    from app.messaging.cloudevents import CloudEvent

from app.database.unit_of_work import UnitOfWorkFactory
from app.modules.shared.errors import ConflictError, InvalidInputError, NotFoundError
from app.modules.shared.events import build_state_event
from app.modules.shared.filters import parse_filter_params
from app.modules.shared.query import (
    ListQuery,
    PageResult,
    make_page_request,
    parse_sort,
)
from app.modules.shared.repository import VersionedRepository, derive_query_fields
from app.modules.shared.spec import (
    D,
    EntitySpec,
    M,
    StateData,
    StateEventItem,
    U,
    VersionedEntity,
)

__all__ = [
    "StateData",
    "StateEventItem",
    "VersionedEntityService",
    "VersionedRepositoryPort",
]

logger = logging.getLogger(__name__)


class VersionedRepositoryPort(Protocol[M]):
    """The DAL surface the choreography needs — satisfied by the generic
    VersionedRepository and by in-memory fakes in tests."""

    async def get(self, entity_id: uuid.UUID) -> M | None: ...
    async def get_for_update(self, entity_id: uuid.UUID) -> M | None: ...
    async def insert_if_absent(self, entity: M) -> M | None: ...
    async def upsert_if_newer_many(self, entities: Sequence[M]) -> None: ...
    async def list(self, query: ListQuery) -> tuple[list[M], int]: ...


class VersionedEntityService(Generic[M, D, U]):
    """Instantiated from an EntitySpec alone; subclassing is an extension
    point, not a requirement. Public methods are the shared choreography;
    the underscore hooks are the sanctioned override seams."""

    def __init__(
        self,
        spec: EntitySpec[M, D, U],
        uow_factory: UnitOfWorkFactory,
        *,
        event_source: str,
        max_page_size: int,
        repo_factory: Callable[[AsyncSession], VersionedRepositoryPort[M]] | None = None,
    ) -> None:
        self._spec = spec
        self._uow_factory = uow_factory
        self._event_source = event_source
        self._max_page_size = max_page_size
        self._repo: Callable[[AsyncSession], VersionedRepositoryPort[M]] = (
            repo_factory
            if repo_factory is not None
            else lambda session: VersionedRepository(spec.model, session)
        )
        self._filterable_fields, self._sortable_fields = derive_query_fields(
            spec.model
        )

    @property
    def spec(self) -> EntitySpec[M, D, U]:
        return self._spec

    # ------------------------------------------------------------- API path

    async def create(self, data: D) -> tuple[M, bool]:
        """Idempotent create. Returns (entity, created). Replaying the same id
        with identical content returns the stored row AND re-announces its
        state event — so a create whose first attempt died in the ambiguous
        commit window still gets its created-event published on retry.
        Contradictory content for an existing id is a conflict."""
        name = self._spec.name
        async with self._uow_factory() as uow:
            repo = self._repo(uow.session)
            entity = self._new_entity(data)
            # Creates always start at 1, whatever the payload says.
            cast(VersionedEntity, entity).version = 1
            created = await repo.insert_if_absent(entity)
            if created is None:
                existing = await repo.get(data.id)
                if existing is None:  # pragma: no cover - momentary race window
                    raise ConflictError(
                        f"{name} {data.id} is being created concurrently"
                    )
                if not self._content_matches(existing, data):
                    raise ConflictError(
                        f"{name} {data.id} already exists with different content"
                    )
                # Re-announce: consumers dedup by version, so the duplicate
                # event is harmless, but a previously lost one is recovered.
                uow.stage_event(
                    self._build_event(self._spec.created_event_type, existing)
                )
                await uow.commit()
                logger.info(
                    "create replayed",
                    extra={"entity": name, "entity_id": str(data.id)},
                )
                return existing, False

            uow.stage_event(
                self._build_event(self._spec.created_event_type, created)
            )
            await uow.commit()
            logger.info("%s created", name, extra={"entity_id": str(data.id)})
            return created, True

    async def update(
        self,
        entity_id: uuid.UUID,
        changes: U,
        *,
        expected_version: int | None = None,
    ) -> M:
        """Sent-field semantics: a field counts as changed iff the client
        actually sent it (`model_fields_set`), it is a mutable field, and
        its value is not None (an explicit null still means "unchanged", as
        it always has on this API)."""
        if expected_version is None:
            # Update schemas carry the guard too (VersionedUpdate); honor it
            # for direct callers so the version check cannot be silently
            # dropped by forgetting the keyword argument.
            expected_version = getattr(changes, "expected_version", None)
        if not self._sent_fields(changes):
            raise InvalidInputError("update must change at least one field")
        async with self._uow_factory() as uow:
            repo = self._repo(uow.session)
            entity = await repo.get_for_update(entity_id)
            if entity is None:
                raise NotFoundError(f"{self._spec.name} {entity_id} not found")
            versioned = cast(VersionedEntity, entity)
            if expected_version is not None and versioned.version != expected_version:
                raise ConflictError(
                    f"version conflict: expected {expected_version}, "
                    f"is {versioned.version}"
                )
            self._apply_changes(entity, changes)
            versioned.version += 1
            uow.stage_event(
                self._build_event(self._spec.updated_event_type, entity)
            )
            await uow.commit()
            logger.info(
                "%s updated",
                self._spec.name,
                extra={"entity_id": str(entity_id), "version": versioned.version},
            )
            return entity

    async def get(self, entity_id: uuid.UUID) -> M:
        async with self._uow_factory() as uow:
            entity = await self._repo(uow.session).get(entity_id)
            if entity is None:
                raise NotFoundError(f"{self._spec.name} {entity_id} not found")
            return entity

    async def list_page(
        self,
        *,
        limit: int,
        offset: int,
        sort: str | None = None,
        filters: Mapping[str, str] | None = None,
    ) -> PageResult[M]:
        """`filters` is the raw `field__op=value` mapping (pagination params
        already stripped); it is parsed and whitelisted here."""
        query = ListQuery(
            page=make_page_request(limit, offset, max_limit=self._max_page_size),
            sort=parse_sort(sort, allowed=self._sortable_fields,
                            default=self._spec.default_sort),
            filters=parse_filter_params(
                filters or {}, allowed=self._filterable_fields
            ),
        )
        async with self._uow_factory() as uow:
            items, total = await self._repo(uow.session).list(query)
            return PageResult(items=items, total=total, limit=query.page.limit,
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
                repo = self._repo(uow.session)
                await repo.upsert_if_newer_many(
                    [self._new_entity(d) for d in winners.values()]
                )
            await uow.commit()
            logger.info(
                "%s events applied",
                self._spec.name,
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
                    self._spec.name,
                    extra={"event_ids": [i.event_id for i in items]},
                )

    # ----------------------------------------------------------- the hooks
    # Generic defaults driven by the spec; ordinary methods, so a custom
    # service_cls can override any of them and still call super().

    def _validated(self, name: str, value: Any) -> Any:
        """Run the spec's extra per-field rule, if one is declared."""
        validator = self._spec.field_validators.get(name)
        return value if validator is None else validator(value)

    @staticmethod
    def _own_copy(value: Any) -> Any:
        """Detach mutable payload values (dicts) from the source model."""
        if isinstance(value, dict):
            return dict(cast("dict[str, Any]", value))
        return value

    def _sent_fields(self, changes: U) -> list[str]:
        return [
            name
            for name in self._spec.mutable_fields
            if name in changes.model_fields_set
            and getattr(changes, name) is not None
        ]

    def _new_entity(self, data: D) -> M:
        """Build an ORM instance from full state, honoring `data.version`
        (the consumer path upserts at the announced version)."""
        values: dict[str, Any] = {}
        for name in self._spec.mutable_fields:
            values[name] = self._own_copy(self._validated(name, getattr(data, name)))
        factory = cast(Callable[..., M], self._spec.model)
        return factory(id=data.id, version=data.version, **values)

    def _content_matches(self, entity: M, data: D) -> bool:
        """Replay equality: is the stored row the same content this create
        announces? (Versions/timestamps are not content.)"""
        return all(
            getattr(entity, name) == getattr(data, name)
            for name in self._spec.mutable_fields
        )

    def _build_event(self, event_type: str, entity: M) -> "CloudEvent":
        """Full-state event announcing this entity. The Data model's
        `extra="ignore"` is what keeps server-maintained columns
        (created_at/updated_at) out of the payload — structurally, not via
        an exclude list."""
        payload = self._spec.data.model_validate(entity, from_attributes=True)
        return build_state_event(event_type, payload, source=self._event_source)

    def _apply_changes(self, entity: M, changes: U) -> None:
        """Copy every sent field onto the entity. Field names match model
        attributes by design (mutable_fields); override when they don't."""
        for name in self._sent_fields(changes):
            value = self._own_copy(self._validated(name, getattr(changes, name)))
            setattr(entity, name, value)
