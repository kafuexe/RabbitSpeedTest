# Worked Example: Adding a Module

This chapter builds a complete second module — **`project`** — from scratch.
Every code block is full and copy-paste runnable. When you finish, the service
has a `/projects` REST API, publishes `project.created` / `project.updated`
CloudEvents, consumes those same event types idempotently and in order, and
batches consumed writes — the same guarantees the `user` module has, because
it is built from the same parts.

**Prerequisites:** working dev environment ([Setup](02-setup.md)), and you've
skimmed the [Architecture Tour](03-architecture-tour.md) — this chapter shows
*what to type*; that one explains *why it's shaped this way*.

## The map: what you will touch

A module is **six code files in one new directory** (plus an empty
`__init__.py`) and **four wiring edits**. Nothing else in the *application*
code changes — tests come in [Testing](06-testing.md), and the
[Maintenance Contract](08-maintenance.md) adds one line to `README.md`'s
layout tree.

```
app/modules/project/          ← NEW directory, the whole module
  __init__.py                   empty marker
  model.py                      ORM table
  repository.py                 data access (CRUD, no rules)
  business.py                   the rules — API and consumer both call this
  schemas.py                    REST DTOs (strict validation)
  events.py                     event contract (permissive validation)
  router.py                     FastAPI endpoints (thin)

app/bootstrap/container.py    ← EDIT: wire the module into both object graphs
app/api/app.py                ← EDIT: mount the router
alembic/env.py                ← EDIT: one import so autogenerate sees the model
alembic/versions/…            ← NEW migration (generated)
```

!!! note "Why this boundary"
    Dependency rules ([tour](03-architecture-tour.md)): `router → business →
    repository → model`, and the consumer edge also calls `business`. The
    business layer imports neither FastAPI nor RabbitMQ. No file in
    `app/modules/project/` may import from `app/modules/user/` — modules talk
    to each other through events, never through each other's repositories.

## Step 0 — Design decisions (make these first)

For `project` we choose:

| Decision | Choice | Where it lands |
|---|---|---|
| Fields | `name`, `description`, `owner_email`, free-form `attributes` | model, schemas, events |
| Identity | client-supplied UUID (replay-safe create) | schemas, business |
| Filterable | `name`, `owner_email` | repository whitelists |
| Sortable | `id`, `name`, `owner_email`, `version`, `created_at`, `updated_at` | repository whitelists |
| Event types | `project.created`, `project.updated` — full state + version | events.py |
| Strict vs permissive email | strict at API ingress, permissive floor on the consumer path | schemas vs events |

That last row is the one people get wrong. The rule of the house
([reliability model](04-reliability-model.md)): **the API is strict** (a
client can fix a 422), **the consumer is permissive** (rejecting a full-state
event freezes the replica at the old version forever, because rejected
payloads are acked away). The consumer floor rejects only what could never be
stored — NUL bytes, NaN/Infinity, values wider than their columns — plus a
minimal shape floor (non-blank name, an `@` in the email, `version >= 1`).

Every validation rule you'll use comes from
`app/modules/shared/validation.py` — one definition per rule, shared by the
API schemas, the event payloads, and the business floor, so no write path can
drift:

| Function | Rule | Use it in |
|---|---|---|
| `valid_name(v)` | non-blank + NUL-free | API schemas, event payloads, business floor |
| `valid_email(v)` | strict — exactly pydantic's `EmailStr` rule, returns the normalized address | business floor (API ingress) |
| `email_floor(v)` | permissive — NUL-free + contains `@`, value kept verbatim | event payloads only |
| `storable_text(v)` | no NUL bytes | free-text fields (e.g. `description`) |
| `storable_json(v)` | no NUL / NaN / Infinity anywhere in the tree (keys included) | `attributes`-style JSONB fields |

## Step 1 — `model.py`: the table

`version` is not decoration — it is the optimistic-concurrency anchor **and**
the event-ordering anchor. Every update increments it; every event carries it;
the consumer drops anything stale by comparing it.

```python title="app/modules/project/model.py"
"""Project ORM model. `version` is the optimistic-concurrency / event-ordering
anchor: every successful update increments it, and inbound events carrying a
version <= the stored one are stale."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Integer, String, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base


class Project(Base):
    __tablename__ = "projects"
    # Fetch server-generated columns (created_at/updated_at) via RETURNING at
    # flush time, so instances stay complete after the session closes.
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    owner_email: Mapped[str] = mapped_column(String(320), nullable=False, index=True)
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
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
```

Also create the empty package marker:

```python title="app/modules/project/__init__.py"
```

## Step 2 — `repository.py`: data access, no rules

Three non-obvious methods carry the module's guarantees, so read the
docstrings as you paste:

- `insert_if_absent` — **idempotent create** in one round trip
  (`ON CONFLICT DO NOTHING … RETURNING`).
- `get_for_update` — **row lock** so concurrent API updates serialize.
- `upsert_if_newer_many` — the consumer's **atomic bulk upsert with the
  version guard as a SQL `WHERE`**, evaluated by PostgreSQL, no locks.

```python title="app/modules/project/repository.py"
"""Project DAL — CRUD only, no business rules. Joins the caller's session;
the Unit of Work owns commit/rollback."""
from __future__ import annotations

import uuid
from typing import Sequence

from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app.modules.shared.errors import InvalidQueryError
from app.modules.shared.query import ListQuery
from app.modules.project.model import Project

# Single source of truth for query whitelists; the business layer derives
# its allowed-field sets from these keys.
FILTERABLE_COLUMNS: dict[str, InstrumentedAttribute] = {
    "name": Project.name,
    "owner_email": Project.owner_email,
}
SORTABLE_COLUMNS: dict[str, InstrumentedAttribute] = {
    "id": Project.id,
    "name": Project.name,
    "owner_email": Project.owner_email,
    "version": Project.version,
    "created_at": Project.created_at,
    "updated_at": Project.updated_at,
}


class ProjectRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, project_id: uuid.UUID) -> Project | None:
        return await self._session.get(Project, project_id)

    async def get_for_update(self, project_id: uuid.UUID) -> Project | None:
        """Row-locked read: serializes concurrent updates of the same project."""
        stmt = select(Project).where(Project.id == project_id).with_for_update()
        return await self._session.scalar(stmt)

    async def insert_if_absent(self, project: Project) -> Project | None:
        """Idempotent insert keyed on id, one round trip: returns the freshly
        inserted row (server defaults populated via RETURNING), or None if a
        row with that id already exists."""
        stmt = (
            pg_insert(Project)
            .values(
                id=project.id,
                name=project.name,
                description=project.description,
                owner_email=project.owner_email,
                attributes=project.attributes,
                version=project.version,
            )
            .on_conflict_do_nothing(index_elements=[Project.id])
            .returning(Project)
        )
        return await self._session.scalar(stmt)

    async def upsert_if_newer_many(self, projects: Sequence[Project]) -> None:
        """Atomic bulk upsert with a version guard, one statement: inserts
        missing rows, overwrites rows whose stored version is older, silently
        skips stale writes. No row locks needed — the guard is a WHERE clause
        evaluated by PostgreSQL. Callers must ensure ids are unique."""
        if not projects:
            return
        stmt = pg_insert(Project).values([
            {
                "id": p.id,
                "name": p.name,
                "description": p.description,
                "owner_email": p.owner_email,
                "attributes": p.attributes,
                "version": p.version,
            }
            for p in projects
        ])
        stmt = stmt.on_conflict_do_update(
            index_elements=[Project.id],
            set_={
                "name": stmt.excluded.name,
                "description": stmt.excluded.description,
                "owner_email": stmt.excluded.owner_email,
                "attributes": stmt.excluded.attributes,
                "version": stmt.excluded.version,
                "updated_at": func.now(),
            },
            where=Project.version < stmt.excluded.version,
        )
        await self._session.execute(stmt)

    async def list(self, query: ListQuery) -> tuple[list[Project], int]:
        # Two queries on purpose: a window count would drag the ENTIRE
        # filtered set through a WindowAgg before LIMIT (measured 2.7-5.6x
        # slower at 200k rows in the user module benchmark).
        stmt: Select[tuple[Project]] = select(Project)
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
        stmt = stmt.order_by(order, Project.id.asc())
        stmt = stmt.limit(query.page.limit).offset(query.page.offset)

        rows = (await self._session.scalars(stmt)).all()
        return list(rows), int(total or 0)
```

## Step 3 — `business.py`: the rules

This is the heart of the module. Two things make it work in both worlds:

1. **It is framework-free** — no FastAPI, no RabbitMQ imports. The API edge
   and the consumer edge both call these same methods.
2. **It never decides whether events get published.** It *stages* events on
   the injected UnitOfWork; whether that UoW carries a real publisher (API
   graph) or a null one (consumer graph) is the composition root's choice.
   That is how "publish after commit" and "the consumer never republishes"
   are both enforced without a single `if` here.

```python title="app/modules/project/business.py"
"""Project business service.

Framework-free: no FastAPI, no RabbitMQ imports. Both the API and the
consumer call these methods; which EventPublisher the injected UnitOfWork
carries (real vs null) is the composition root's decision.
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
from app.modules.shared.validation import storable_text, valid_email, valid_name
from app.modules.project.model import Project
from app.modules.project.repository import FILTERABLE_COLUMNS, SORTABLE_COLUMNS

logger = logging.getLogger(__name__)

SORTABLE_FIELDS = frozenset(SORTABLE_COLUMNS)
FILTERABLE_FIELDS = frozenset(FILTERABLE_COLUMNS)
DEFAULT_SORT = SortSpec(field="created_at", descending=True)


@dataclass(frozen=True)
class ProjectData:
    """Full desired state of a project (create payload / event payload)."""

    id: uuid.UUID
    name: str
    description: str
    owner_email: str
    attributes: dict[str, Any]
    version: int = 1


@dataclass(frozen=True)
class ProjectChanges:
    """Partial update; None means "leave unchanged"."""

    name: str | None = None
    description: str | None = None
    owner_email: str | None = None
    attributes: dict[str, Any] | None = None


@dataclass(frozen=True)
class ProjectEventItem:
    """One consumed event: identity for dedup + the state it announces."""

    event_id: str
    source: str
    data: ProjectData


class ProjectRepositoryPort(Protocol):
    async def get(self, project_id: uuid.UUID) -> Project | None: ...
    async def get_for_update(self, project_id: uuid.UUID) -> Project | None: ...
    async def insert_if_absent(self, project: Project) -> Project | None: ...
    async def upsert_if_newer_many(self, projects: Sequence[Project]) -> None: ...
    async def list(self, query: ListQuery) -> tuple[list[Project], int]: ...


class ProjectService:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        repo_factory: Callable[[AsyncSession], ProjectRepositoryPort],
        *,
        event_source: str,
        max_page_size: int,
    ) -> None:
        self._uow_factory = uow_factory
        self._repo_factory = repo_factory
        self._event_source = event_source
        self._max_page_size = max_page_size

    # ------------------------------------------------------------- API path

    async def create_project(self, data: ProjectData) -> tuple[Project, bool]:
        """Idempotent create. Returns (project, created). Replaying the same
        id with identical content returns the stored row AND re-announces its
        state event — so a create whose first attempt died in the ambiguous
        commit window still gets its project.created published on retry.
        Contradictory content for an existing id is a conflict."""
        self._validate_name(data.name)
        self._validate_description(data.description)
        self._validate_email(data.owner_email)
        async with self._uow_factory() as uow:
            repo = self._repo_factory(uow.session)
            project = await repo.insert_if_absent(
                Project(
                    id=data.id,
                    name=data.name,
                    description=data.description,
                    owner_email=data.owner_email,
                    attributes=dict(data.attributes),
                    version=1,
                )
            )
            if project is None:
                existing = await repo.get(data.id)
                if existing is None:  # pragma: no cover - momentary race window
                    raise ConflictError(
                        f"project {data.id} is being created concurrently"
                    )
                if (
                    existing.name,
                    existing.description,
                    existing.owner_email,
                    existing.attributes,
                ) != (data.name, data.description, data.owner_email, data.attributes):
                    raise ConflictError(
                        f"project {data.id} already exists with different content"
                    )
                # Re-announce: consumers dedup by version, so the duplicate
                # event is harmless, but a previously lost one is recovered.
                uow.stage_event(self._state_event("project.created", existing))
                await uow.commit()
                logger.info("create replayed", extra={"project_id": str(data.id)})
                return existing, False

            uow.stage_event(self._state_event("project.created", project))
            await uow.commit()
            logger.info("project created", extra={"project_id": str(data.id)})
            return project, True

    async def update_project(
        self,
        project_id: uuid.UUID,
        changes: ProjectChanges,
        *,
        expected_version: int | None = None,
    ) -> Project:
        if (
            changes.name is None
            and changes.description is None
            and changes.owner_email is None
            and changes.attributes is None
        ):
            raise InvalidInputError("update must change at least one field")
        if changes.name is not None:
            self._validate_name(changes.name)
        if changes.description is not None:
            self._validate_description(changes.description)
        if changes.owner_email is not None:
            self._validate_email(changes.owner_email)
        async with self._uow_factory() as uow:
            repo = self._repo_factory(uow.session)
            project = await repo.get_for_update(project_id)
            if project is None:
                raise NotFoundError(f"project {project_id} not found")
            if expected_version is not None and project.version != expected_version:
                raise ConflictError(
                    f"version conflict: expected {expected_version}, "
                    f"is {project.version}"
                )
            if changes.name is not None:
                project.name = changes.name
            if changes.description is not None:
                project.description = changes.description
            if changes.owner_email is not None:
                project.owner_email = changes.owner_email
            if changes.attributes is not None:
                project.attributes = dict(changes.attributes)
            project.version += 1
            uow.stage_event(self._state_event("project.updated", project))
            await uow.commit()
            logger.info(
                "project updated",
                extra={"project_id": str(project_id), "version": project.version},
            )
            return project

    async def get_project(self, project_id: uuid.UUID) -> Project:
        async with self._uow_factory() as uow:
            project = await self._repo_factory(uow.session).get(project_id)
            if project is None:
                raise NotFoundError(f"project {project_id} not found")
            return project

    async def list_projects(
        self,
        *,
        limit: int,
        offset: int,
        sort: str | None = None,
        name: str | None = None,
        owner_email: str | None = None,
    ) -> Page[Project]:
        query = ListQuery(
            page=make_page_request(limit, offset, max_limit=self._max_page_size),
            sort=parse_sort(sort, allowed=SORTABLE_FIELDS, default=DEFAULT_SORT),
            filters=build_filters(
                {"name": name, "owner_email": owner_email},
                allowed=FILTERABLE_FIELDS,
            ),
        )
        async with self._uow_factory() as uow:
            items, total = await self._repo_factory(uow.session).list(query)
            return Page(items=items, total=total, limit=query.page.limit,
                        offset=query.page.offset)

    # -------------------------------------------------------- consumer path

    async def apply_project_events(self, items: Sequence[ProjectEventItem]) -> None:
        """Apply a batch of externally-announced project states in ONE
        transaction. Idempotent and order-safe, atomically:

        - duplicate delivery   → bulk inbox insert filters it out
        - within-batch races   → highest version per project wins
        - update before create → row upserted from the event's full state
        - stale/out-of-order   → version guard in the upsert skips it
        The inbox rows commit with the data, so redeliveries stay no-ops;
        nothing is acked (the batcher resolves submits) until this commits.
        """
        async with self._uow_factory() as uow:
            fresh = await uow.mark_events_processed(
                [(i.source, i.event_id) for i in items]
            )
            winners: dict[uuid.UUID, ProjectData] = {}
            for item in items:
                if (item.source, item.event_id) not in fresh:
                    continue
                current = winners.get(item.data.id)
                if current is None or item.data.version > current.version:
                    winners[item.data.id] = item.data
            if winners:
                repo = self._repo_factory(uow.session)
                await repo.upsert_if_newer_many([
                    Project(
                        id=d.id,
                        name=d.name,
                        description=d.description,
                        owner_email=d.owner_email,
                        attributes=dict(d.attributes),
                        version=d.version,
                    )
                    for d in winners.values()
                ])
            await uow.commit()
            logger.info(
                "project events applied",
                extra={
                    "batch": len(items),
                    "fresh": len(fresh),
                    "duplicates": len(items) - len(fresh),
                    "projects_written": len(winners),
                },
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "project events applied (detail)",
                    extra={"event_ids": [i.event_id for i in items]},
                )

    # ------------------------------------------------------------- internal
    # The business floor delegates to the SHARED rules (the same functions
    # the API schemas and ProjectEventData run), so no write path can drift.

    def _validate_name(self, name: str) -> None:
        try:
            valid_name(name)
        except ValueError as exc:
            raise InvalidInputError(f"name {exc}") from None

    def _validate_description(self, description: str) -> None:
        try:
            storable_text(description)
        except ValueError as exc:
            raise InvalidInputError(f"description {exc}") from None

    def _validate_email(self, email: str) -> None:
        try:
            valid_email(email)
        except ValueError as exc:
            raise InvalidInputError(f"owner_email invalid: {exc}") from None

    def _state_event(self, event_type: str, project: Project) -> "CloudEvent":
        # Local import keeps business.py free of a hard edge on the event
        # module at import time while events.py imports ProjectData from here.
        from app.modules.project.events import build_project_event

        return build_project_event(event_type, project, source=self._event_source)
```

!!! warning "Do not skip the validation helpers"
    They delegate to `app/modules/shared/validation.py` — the *same*
    functions the API schemas and the event payload schema run. One
    definition per rule means no write path can drift: a value valid in the
    schema is valid at the business floor, and vice versa.

## Step 4 — `schemas.py`: the strict edge

API DTOs. Strict on purpose: a client submitting bad data gets a 422 and can
correct it. `EmailStr` here is *exactly* the same rule as `valid_email` in the
business layer, so the two can never disagree.

```python title="app/modules/project/schemas.py"
"""API DTOs for the project module (Pydantic v2)."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from app.modules.shared.validation import storable_json, storable_text, valid_name


class ProjectCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)
    owner_email: EmailStr = Field(max_length=320)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name_valid(cls, value: str) -> str:
        return valid_name(value)

    @field_validator("description")
    @classmethod
    def _description_storable(cls, value: str) -> str:
        return storable_text(value)

    @field_validator("attributes")
    @classmethod
    def _attributes_storable(cls, value: dict[str, Any]) -> dict[str, Any]:
        return storable_json(value)


class ProjectUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    owner_email: EmailStr | None = Field(default=None, max_length=320)
    attributes: dict[str, Any] | None = None
    expected_version: int | None = Field(default=None, ge=1)

    @field_validator("name")
    @classmethod
    def _name_valid(cls, value: str | None) -> str | None:
        return None if value is None else valid_name(value)

    @field_validator("description")
    @classmethod
    def _description_storable(cls, value: str | None) -> str | None:
        return None if value is None else storable_text(value)

    @field_validator("attributes")
    @classmethod
    def _attributes_storable(
        cls, value: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        return None if value is None else storable_json(value)


class ProjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: str
    owner_email: str
    attributes: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime


class ProjectPageOut(BaseModel):
    items: list[ProjectOut]
    total: int
    limit: int
    offset: int
```

## Step 5 — `events.py`: the permissive edge

The event contract: type constants, the payload schema, the outbound builder,
and inbound handler registration. Note what the handlers **don't** do — no
try/except, no ack/nack decisions. They validate (a `ValidationError`
propagates) and submit to the batcher. The dispatch layer
(`app/messaging/consumer.py`) owns the permanent-vs-transient classification,
so every module is poison-safe by construction.

```python title="app/modules/project/events.py"
"""Project event contract: types, payload schema, builders, registration.

Both project.created and project.updated carry the project's FULL state plus
its version, which is what makes out-of-order handling possible: the consumer
can upsert from any event and drop anything stale.
"""
from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.logging.correlation import get_correlation_id
from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent, now_utc
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.validation import (
    email_floor,
    storable_json,
    storable_text,
    valid_name,
)
from app.modules.project.business import ProjectData, ProjectEventItem
from app.modules.project.model import Project

PROJECT_CREATED = "project.created"
PROJECT_UPDATED = "project.updated"


class ProjectEventData(BaseModel):
    """Payload carried in the CloudEvent `data` attribute.

    DELIBERATELY more permissive than the API schemas: events are FULL-STATE
    announcements from an authoritative producer, and rejecting one (the
    dispatch layer acks rejected payloads away) would freeze the replica at
    the previous version forever. So only what can never be stored (NUL/NaN)
    or is not minimally shaped is rejected, and values are stored VERBATIM.
    Strict validation belongs at the API ingress, where the client can
    correct a 422. See modules/shared/validation.py.
    """

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)
    owner_email: str = Field(min_length=3, max_length=320)
    attributes: dict[str, Any] = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)

    @field_validator("name")
    @classmethod
    def _name_floor(cls, value: str) -> str:
        return valid_name(value)

    @field_validator("description")
    @classmethod
    def _description_floor(cls, value: str) -> str:
        return storable_text(value)

    @field_validator("owner_email")
    @classmethod
    def _email_floor(cls, value: str) -> str:
        return email_floor(value)

    @field_validator("attributes")
    @classmethod
    def _attributes_storable(cls, value: dict[str, Any]) -> dict[str, Any]:
        return storable_json(value)


def build_project_event(
    event_type: str, project: Project, *, source: str
) -> CloudEvent:
    data = ProjectEventData(
        id=project.id,
        name=project.name,
        description=project.description,
        owner_email=project.owner_email,
        attributes=project.attributes,
        version=project.version,
    )
    return CloudEvent(
        id=str(uuid.uuid4()),
        source=source,
        type=event_type,
        time=now_utc(),
        data=data.model_dump(mode="json"),
        correlationid=get_correlation_id(),
    )


def register_project_event_handlers(
    registry: EventHandlerRegistry, batcher: Batcher[ProjectEventItem]
) -> None:
    """Handlers validate (ValidationError propagates — EventConsumer's
    dispatch classifies it permanent and acks) and submit to the greedy
    batcher; submit() returns — and the message is acked — only once the
    item's batch has committed. This module owns no ack/nack policy."""

    async def apply_state_event(event: CloudEvent) -> None:
        payload = ProjectEventData.model_validate(event.data)
        await batcher.submit(
            ProjectEventItem(
                event_id=event.id,
                source=event.source,
                data=ProjectData(
                    id=payload.id,
                    name=payload.name,
                    description=payload.description,
                    owner_email=payload.owner_email,
                    attributes=payload.attributes,
                    version=payload.version,
                ),
            )
        )

    registry.register(PROJECT_CREATED, apply_state_event)
    registry.register(PROJECT_UPDATED, apply_state_event)
```

## Step 6 — `router.py`: the thin edge

Translation only: schema in, service call, schema out. If you find yourself
writing an `if` about domain state here, it belongs in `business.py`.

```python title="app/modules/project/router.py"
"""Project REST endpoints — thin translation only: schema in, service call,
schema out. No business logic, no repository access."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Query, Response, status

from app.modules.project.business import (
    ProjectChanges,
    ProjectData,
    ProjectService,
)
from app.modules.project.schemas import (
    ProjectCreate,
    ProjectOut,
    ProjectPageOut,
    ProjectUpdate,
)


def build_project_router(service: ProjectService) -> APIRouter:
    router = APIRouter(prefix="/projects", tags=["projects"])

    @router.post("", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
    async def create_project(payload: ProjectCreate, response: Response) -> ProjectOut:
        project, created = await service.create_project(
            ProjectData(
                id=payload.id or uuid.uuid4(),
                name=payload.name,
                description=payload.description,
                owner_email=str(payload.owner_email),
                attributes=payload.attributes,
            )
        )
        if not created:
            response.status_code = status.HTTP_200_OK
        return ProjectOut.model_validate(project)

    @router.get("/{project_id}", response_model=ProjectOut)
    async def get_project(project_id: uuid.UUID) -> ProjectOut:
        return ProjectOut.model_validate(await service.get_project(project_id))

    @router.patch("/{project_id}", response_model=ProjectOut)
    async def update_project(
        project_id: uuid.UUID, payload: ProjectUpdate
    ) -> ProjectOut:
        project = await service.update_project(
            project_id,
            ProjectChanges(
                name=payload.name,
                description=payload.description,
                owner_email=(
                    str(payload.owner_email)
                    if payload.owner_email is not None
                    else None
                ),
                attributes=payload.attributes,
            ),
            expected_version=payload.expected_version,
        )
        return ProjectOut.model_validate(project)

    @router.get("", response_model=ProjectPageOut)
    async def list_projects(
        limit: int = Query(default=50),
        offset: int = Query(default=0),
        sort: str | None = Query(default=None, description="field or -field"),
        name: str | None = Query(default=None),
        owner_email: str | None = Query(default=None),
    ) -> ProjectPageOut:
        page = await service.list_projects(
            limit=limit, offset=offset, sort=sort, name=name,
            owner_email=owner_email,
        )
        return ProjectPageOut(
            items=[ProjectOut.model_validate(p) for p in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )

    return router
```

## Step 7 — Wiring: the composition root and friends

Three files change. This is the subtlest part of the whole exercise, because
the container builds **two object graphs** from one codebase:

- **API graph** — UnitOfWork carries `QueueEventPublisher`: committed events
  are published to the outbound queue.
- **Consumer graph** — UnitOfWork carries `NullEventPublisher`: the consumer
  can never republish, *by construction*, not by discipline.

Your new service must be wired into **both**.

### 7a. `app/bootstrap/container.py`

Add the imports:

```python
from app.modules.project.business import ProjectService
from app.modules.project.events import register_project_event_handlers
from app.modules.project.repository import ProjectRepository
```

In `__init__`, right after `self.user_service = UserService(...)` — same
`api_uow_factory`, so project events publish after commit exactly like user
events:

```python
        self.project_service = ProjectService(
            api_uow_factory,
            ProjectRepository,
            event_source=settings.event_source,
            max_page_size=settings.max_page_size,
        )
```

In `_build_consumer_graph`, after the user batcher is registered and **before**
`self.event_consumer = EventConsumer(...)` (the consumer must see a fully
populated registry):

```python
        consumer_project_service = ProjectService(
            consumer_uow_factory,
            ProjectRepository,
            event_source=self.settings.event_source,
            max_page_size=self.settings.max_page_size,
        )
        self.project_batcher = Batcher(
            consumer_project_service.apply_project_events,
            max_batch=self.settings.consumer_batch_size,
        )
        register_project_event_handlers(self.registry, self.project_batcher)
```

In `start()`, widen the restart check — a closed batcher silently nacks
everything forever while looking healthy, which is exactly why this check
exists:

```python
        if self.user_batcher.closed or self.project_batcher.closed:
            self._build_consumer_graph()
```

In `stop()`, close your batcher alongside the user one (order still matters:
consumer cancelled first, then batchers — pending items nack+requeue while
the channel is still open — then bus, then engine):

```python
        await self.user_batcher.close()
        await self.project_batcher.close()
```

!!! note "Each module gets its own batcher"
    A batch is one database transaction for **one** business method
    (`apply_project_events`). Sharing a batcher across modules would mix
    item types into one call. The registry is shared; batchers are per-module.

### 7b. `app/api/app.py`

```python
from app.modules.project.router import build_project_router
```

and next to the existing user router line:

```python
    app.include_router(build_project_router(container.project_service))
```

### 7c. `alembic/env.py`

Autogenerate only sees tables whose model modules are imported. Add yours next
to the existing imports (aliased — `model` is already taken by the user
import):

```python
from app.modules.project import model as project_model  # noqa: F401
```

## Step 8 — The migration

Generate, **review**, then apply (from `shared_data_service/`, like every
command in this guide):

```bash
.venv/bin/python -m alembic revision --autogenerate -m "projects table"
```

Open the generated file in `alembic/versions/` and check it against this
expected shape — autogenerate is a draft, not a decision. It must create
*only* the `projects` table (if it tries to touch `users` or
`processed_events`, your env.py import is wrong):

```python
def upgrade() -> None:
    op.create_table('projects',
    sa.Column('id', sa.Uuid(), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('description', sa.String(length=2000), nullable=False),
    sa.Column('owner_email', sa.String(length=320), nullable=False),
    sa.Column('attributes', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_projects'))
    )
    op.create_index(op.f('ix_projects_owner_email'), 'projects', ['owner_email'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_projects_owner_email'), table_name='projects')
    op.drop_table('projects')
```

Apply it:

```bash
.venv/bin/python -m alembic upgrade head
```

## Step 9 — Prove it works

Start the service in `both` mode (`.venv/bin/python main.py`), then walk the
guarantees end to end.

**1. Idempotent create — run this twice.** First run: `201`. Second run,
byte-identical: `200` with the same stored row (and the `project.created`
event is re-announced — that's the crash-recovery path, see
[reliability](04-reliability-model.md)).

```bash
curl -si -X POST http://127.0.0.1:8080/projects \
  -H 'content-type: application/json' \
  -d '{
    "id": "11111111-1111-1111-1111-111111111111",
    "name": "Apollo",
    "description": "Guidance computer rewrite",
    "owner_email": "margaret@example.com"
  }' | head -1
```

**2. Contradictory replay → 409.** Same id, different name:

```bash
curl -si -X POST http://127.0.0.1:8080/projects \
  -H 'content-type: application/json' \
  -d '{"id": "11111111-1111-1111-1111-111111111111",
       "name": "Gemini", "owner_email": "margaret@example.com"}' | head -1
```

**3. Optimistic update.** Succeeds (version 1 → 2); run it again unchanged and
the stale `expected_version` gets a `409`:

```bash
curl -si -X PATCH \
  http://127.0.0.1:8080/projects/11111111-1111-1111-1111-111111111111 \
  -H 'content-type: application/json' \
  -d '{"description": "Now with lunar module", "expected_version": 1}' | head -1
```

**4. Read and list:**

```bash
curl -s http://127.0.0.1:8080/projects/11111111-1111-1111-1111-111111111111
curl -s 'http://127.0.0.1:8080/projects?sort=-created_at&owner_email=margaret@example.com'
```

**5. The consumer path.** Publish a `project.created` event from a fake
external producer into the inbound queue, and watch the service apply it.
This script is a throwaway verification tool — delete it when you're done
(it is not part of the module):

```python title="scripts/publish_test_project_event.py"
"""Publish one project CloudEvent to the inbound queue.

EVENT_ID is a constant on purpose: re-running the script unchanged publishes
a true duplicate delivery, which is experiment 5a below.
"""
import asyncio
import json

from app.config.settings import Settings
from app.messaging.simple_client import SimpleClientAdapter

EVENT_ID = "e0000000-0000-0000-0000-000000000001"
EVENT_TYPE = "project.created"
VERSION = 1


async def main() -> None:
    settings = Settings()
    bus = SimpleClientAdapter(
        settings.amqp_url, prefetch=1, persistent=settings.persistent_messages
    )
    await bus.connect()
    event = {
        "specversion": "1.0",
        "id": EVENT_ID,
        "source": "urn:example:other-service",
        "type": EVENT_TYPE,
        "data": {
            "id": "22222222-2222-2222-2222-222222222222",
            "name": "Borealis",
            "description": "arrived via events",
            "owner_email": "producer@example.com",
            "attributes": {"tier": "gold"},
            "version": VERSION,
        },
    }
    await bus.publish(settings.consume_queues[0], json.dumps(event).encode())
    await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
.venv/bin/python scripts/publish_test_project_event.py
```

The service log shows `project events applied` with `batch: 1, fresh: 1`, and
the row is now readable over the API:

```bash
curl -s http://127.0.0.1:8080/projects/22222222-2222-2222-2222-222222222222
```

**5a. The inbox.** Run the script again, **unchanged** — same `EVENT_ID`, so
it is a genuine duplicate delivery. The log shows
`duplicates: 1, projects_written: 0`: the inbox filtered it before it ever
reached the table.

**5b. The version guard.** First move the row ahead of the event:

```bash
curl -s -X PATCH \
  http://127.0.0.1:8080/projects/22222222-2222-2222-2222-222222222222 \
  -H 'content-type: application/json' \
  -d '{"description": "updated via API"}'
```

The row is now at version 2. In the script, change `EVENT_ID` to any new
value (so the inbox lets it through), `EVENT_TYPE` to `"project.updated"`,
and leave `VERSION = 1` — a stale, out-of-order event. Run it: the log shows
`fresh: 1` but the upsert's `WHERE stored.version < new.version` skips the
write. Confirm nothing moved backwards:

```bash
curl -s http://127.0.0.1:8080/projects/22222222-2222-2222-2222-222222222222
# → still version 2, description "updated via API"
```

Those two experiments *are* the reliability model, observed live.

**6. Crucially — the consumer did not republish.** Check the outbound queue
(`shared-data.events.out` in the management UI at http://localhost:15672 —
Queues → the queue → Get Messages): the API experiments above put events
there, but consuming events produced **no additional ones**. That's the
`NullEventPublisher` in the consumer graph doing its job. For a sharper
probe, run with `SDS_LOG_LEVEL=DEBUG`: any event the consumer graph tried to
publish would log `event suppressed (consumer path never republishes)` — and
in this walkthrough even that line is absent, because the consumer path
stages no events at all.

## Definition of done — checklist

Copy this into your PR description and check every box:

- [ ] `app/modules/project/` contains all six files + `__init__.py`
- [ ] No import from any other module's internals (`app/modules/user/…`)
- [ ] Repository: `insert_if_absent`, `get_for_update`,
      `upsert_if_newer_many` (version guard in `WHERE`), whitelisted
      filter/sort columns
- [ ] Business: validation delegates to `modules/shared/validation.py`;
      events staged on the UoW, never published directly
- [ ] Events: full-state payload with `version`; strict API schema,
      permissive event floor; handlers registered for both event types
- [ ] Container: service in the **API graph**, service + dedicated batcher +
      handler registration in the **consumer graph**, batcher closed in
      `stop()`, restart check widened in `start()`
- [ ] Router mounted in `app/api/app.py`
- [ ] Model imported in `alembic/env.py`; migration generated, **reviewed**,
      applied; `downgrade()` works
- [ ] Verified live: create twice (201 → 200), contradictory create (409),
      stale `expected_version` (409), event consumed and applied, duplicate
      event deduped, stale version dropped, nothing republished
- [ ] Tests written for the module — next chapter: [Testing](06-testing.md)
- [ ] Onboarding updated if you changed any pattern this guide teaches
      ([Maintenance Contract](08-maintenance.md))
