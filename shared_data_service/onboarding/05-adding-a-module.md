# Worked Example: Adding a Module

This chapter builds a complete second module — **`project`** — from scratch.
(The finished module now ships in `app/modules/project/`, so you can follow
along typing it yourself or diff each step against the real thing.)
Every code block is full and copy-paste runnable. When you finish, the service
has a `/projects` REST API, publishes `project.created` / `project.updated`
CloudEvents, consumes those same event types idempotently and in order, and
batches consumed writes — the same guarantees the `user` module has, because
it is built from the same parts.

**Prerequisites:** working dev environment ([Setup](02-setup.md)), and you've
skimmed the [Architecture Tour](03-architecture-tour.md) — this chapter shows
*what to type*; that one explains *why it's shaped this way*.

## The shape: generic machinery, thin modules

Everything that is the same for every module lives in
`app/modules/shared/` and is **inherited, not copied**:

| Shared unit | What it owns |
|---|---|
| `shared/repository.py` — `VersionedRepository` | idempotent `insert_if_absent`, row-locked `get_for_update`, `upsert_if_newer_many` with the version guard as a SQL `WHERE`, whitelisted filter/sort/paginate `list` |
| `shared/service.py` — `VersionedEntityService` | the whole choreography: idempotent create with replay re-announce, optimistic update, batched idempotent `apply_state_events`, staged events (publish-after-commit) |
| `shared/events.py` | CloudEvent envelope building, full-state handler registration (ack/nack policy stays in the dispatch layer) |

A module declares only what is genuinely its own: the table, the query
whitelists, the data shapes (which — declared with the shared validation
types — *are* the validation floor), replay equality, and the event
contract. Every inherited method is an ordinary Python method — if your
module genuinely diverges, **override it**; the base classes are defaults,
not a framework.

## The map: what you will touch

A module is **six code files in one new directory** (plus an empty
`__init__.py`) and **three wiring edits**. Nothing else in the *application*
code changes — tests come in [Testing](06-testing.md), and the
[Maintenance Contract](08-maintenance.md) adds one line to `README.md`'s
layout tree.

```
app/modules/project/          ← NEW directory, the whole module
  __init__.py                   empty marker
  model.py                      ORM table
  repository.py                 whitelists — machinery inherited
  business.py                   data shapes + hooks — choreography inherited
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
API schemas, the event payloads, and the business-layer data models, so no
write path can drift. You never write a `@field_validator`: the module
exports **shared `Annotated` types** that combine the rule function
(`AfterValidator`) with the shape constraints (`Field` lengths) in one
declaration. Declaring a field with one of these types *is* the validation —
pydantic validates the whole schema and aggregates every field failure into
one `ValidationError`:

| Annotated type | Rule + shape | Use it in |
|---|---|---|
| `ValidName` | non-blank + NUL-free, 1–200 chars | API schemas, event payloads, business models |
| `StrictEmail` | strict — pydantic's `EmailStr` rule (normalized) + storability, ≤ 320 chars | API schemas + business models (API ingress) |
| `FloorEmail` | permissive — NUL-free + contains `@`, value kept verbatim, 3–320 chars | event payloads only |
| `StorableText` | no NUL bytes; compose a length per field: `Annotated[StorableText, Field(max_length=2000)]` | free-text fields (e.g. `description`) |
| `StorableAttributes` | no NUL / NaN / Infinity anywhere in the tree (keys included) | `attributes`-style JSONB fields |

The underlying rule functions (`valid_name`, `valid_email`, `email_floor`,
`storable_text`, `storable_json`) remain importable for programmatic checks,
but models should always use the Annotated types.

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

!!! note "What `VersionedRepository` expects from a model"
    An `id` primary key, a `version` column, and server-maintained
    `created_at`/`updated_at`. Write payloads are derived from the table
    automatically: every column *without* a server default / `onupdate` is
    carried by inserts and upserts. A model that needs something else
    overrides `_row_values` in its repository.

## Step 2 — `repository.py`: declare the whitelists

The machinery — idempotent insert, row locks, the version-guarded bulk
upsert, the two-query list — is inherited. You declare the model and the
query whitelists, the single source of truth the business layer derives its
allowed-field sets from. Anything outside them is rejected with a 400, never
passed to SQL.

```python title="app/modules/project/repository.py"
"""Project DAL — declares the table and its query whitelists; every mechanism
(idempotent insert, row-locked read, version-guarded bulk upsert, whitelisted
list) is inherited from the shared VersionedRepository."""
from __future__ import annotations

from app.modules.shared.repository import VersionedRepository
from app.modules.project.model import Project


class ProjectRepository(VersionedRepository[Project]):
    model = Project
    filterable_columns = {
        "name": Project.name,
        "owner_email": Project.owner_email,
    }
    sortable_columns = {
        "id": Project.id,
        "name": Project.name,
        "owner_email": Project.owner_email,
        "version": Project.version,
        "created_at": Project.created_at,
        "updated_at": Project.updated_at,
    }
```

Read the base class once (`app/modules/shared/repository.py`) — the
docstrings on `insert_if_absent`, `get_for_update` and `upsert_if_newer_many`
*are* the module's storage guarantees.

## Step 3 — `business.py`: data shapes + the hooks

The heart of the module — but the choreography (idempotent create with
replay re-announce, optimistic update, batched idempotent event application)
is inherited from `VersionedEntityService`. You supply:

- the **data shapes** (`ProjectData`, `ProjectChanges`) — pydantic models
  declared with the shared Annotated types, so an instance is **valid by
  construction** and `validate_assignment=True` re-validates on mutation;
  there are no manual validation calls anywhere in the business layer,
- the **identity declarations** (entity name, event types, default sort,
  allowed fields),
- and three **hooks**: build an entity, compare replay content, and build
  the state event.

Two properties still hold, now by construction: the service is
framework-free, and it never decides whether events get published — it stages
them on the injected UnitOfWork, and the composition root decides which
publisher (real vs null) that UoW carries.

```python title="app/modules/project/business.py"
"""Project business rules. The choreography lives in the shared
VersionedEntityService; this module supplies only what is project-specific:
the data shapes (which ARE the validation floor), replay equality, and
event building.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel, ConfigDict, Field

from app.modules.shared.query import SortSpec
from app.modules.shared.service import StateEventItem, VersionedEntityService
from app.modules.shared.validation import (
    StorableAttributes,
    StorableText,
    StrictEmail,
    ValidName,
)
from app.modules.project.model import Project
from app.modules.project.repository import ProjectRepository

if TYPE_CHECKING:
    from app.messaging.cloudevents import CloudEvent

# The project description rule+shape in ONE place: storable text, max 2000.
# API schemas and the event payload import this, so the limit cannot drift.
ProjectDescription = Annotated[StorableText, Field(max_length=2000)]


class ProjectData(BaseModel):
    """Full desired state of a project (create payload / event payload).
    Valid by construction; assignment re-validates."""

    model_config = ConfigDict(validate_assignment=True)

    id: uuid.UUID
    name: ValidName
    description: ProjectDescription
    owner_email: StrictEmail
    attributes: StorableAttributes
    version: int = 1


class ProjectChanges(BaseModel):
    """Partial update; None means "leave unchanged". Non-None fields are
    validated by the same shared types the full state uses."""

    model_config = ConfigDict(validate_assignment=True)

    name: ValidName | None = None
    description: ProjectDescription | None = None
    owner_email: StrictEmail | None = None
    attributes: StorableAttributes | None = None


ProjectEventItem = StateEventItem[ProjectData]


class ProjectService(VersionedEntityService[Project, ProjectData, ProjectChanges]):
    entity_name = "project"
    created_event_type = "project.created"
    updated_event_type = "project.updated"
    default_sort = SortSpec(field="created_at", descending=True)
    sortable_fields = frozenset(ProjectRepository.sortable_columns)
    filterable_fields = frozenset(ProjectRepository.filterable_columns)

    def _new_entity(self, data: ProjectData) -> Project:
        return Project(
            id=data.id,
            name=data.name,
            description=data.description,
            owner_email=data.owner_email,
            attributes=dict(data.attributes),
            version=data.version,
        )

    def _content_matches(self, entity: Project, data: ProjectData) -> bool:
        return (
            entity.name, entity.description, entity.owner_email,
            entity.attributes,
        ) == (data.name, data.description, data.owner_email, data.attributes)

    def _build_event(self, event_type: str, entity: Project) -> "CloudEvent":
        # Local import keeps business.py free of a hard edge on the event
        # module at import time while events.py imports ProjectData from here.
        from app.modules.project.events import build_project_event

        return build_project_event(event_type, entity, source=self._event_source)
```

!!! note "The hooks, and when to override more"
    `_apply_changes` (copy non-None change fields onto the entity) has a
    generic default that assumes change-field names match model attributes —
    true here, so we don't write it. If your module's update rules are
    richer (cross-field checks, derived columns), override `_apply_changes`
    or, for genuinely divergent flows, the public `create`/`update`/
    `apply_state_events` themselves — they are ordinary methods, not sealed
    framework internals.

!!! warning "Do not re-type field rules — use the shared Annotated types"
    `ValidName`, `StrictEmail`, `StorableAttributes` (and your module's own
    compositions like `ProjectDescription`) come from
    `app/modules/shared/validation.py` — the *same* types the API schemas
    run. One definition per rule means no write path can drift: a value
    valid in the schema is valid in the business model, and vice versa.
    There is no validation code to call — constructing (or assigning to) a
    `ProjectData`/`ProjectChanges` IS the business floor, and invalid input
    raises `pydantic.ValidationError` at the call site.

## Step 4 — `schemas.py`: the strict edge

API DTOs. Strict on purpose: a client submitting bad data gets a 422 and can
correct it. The fields are the *same* Annotated types the business models
declare (`StrictEmail` is exactly pydantic's `EmailStr` rule plus
storability), so the schema and the business floor can never disagree — and
because these are plain type declarations, pydantic validates the whole
schema and reports every bad field in one 422.

```python title="app/modules/project/schemas.py"
"""API DTOs for the project module (Pydantic v2). Fields are declared with
the shared Annotated types from modules/shared/validation.py (plus the
module's own ProjectDescription) — rule + shape constraints in one
declaration, whole-schema validation by default."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.modules.project.business import ProjectDescription
from app.modules.shared.validation import (
    StorableAttributes,
    StrictEmail,
    ValidName,
)


class ProjectCreate(BaseModel):
    # Client-supplied id makes create replay-safe; omitted → server generates
    # one (that request is then not replayable by design).
    id: uuid.UUID | None = None
    name: ValidName
    description: ProjectDescription = ""
    owner_email: StrictEmail
    attributes: StorableAttributes = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    name: ValidName | None = None
    description: ProjectDescription | None = None
    owner_email: StrictEmail | None = None
    attributes: StorableAttributes | None = None
    expected_version: int | None = Field(default=None, ge=1)


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
and inbound handler registration. The envelope and the handler mechanics come
from `modules/shared/events.py`; note what the module **doesn't** own — no
try/except, no ack/nack decisions. Handlers validate (a `ValidationError`
propagates) and submit to the batcher; the dispatch layer
(`app/messaging/consumer.py`) owns the permanent-vs-transient classification,
so every module is poison-safe by construction.

```python title="app/modules/project/events.py"
"""Project event contract: types, payload schema, builder, registration.

Both project.created and project.updated carry the project's FULL state plus
its version, which is what makes out-of-order handling possible: the consumer
can upsert from any event and drop anything stale.
"""
from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field

from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.events import build_state_event, register_state_event_handlers
from app.modules.shared.validation import FloorEmail, StorableAttributes, ValidName
from app.modules.project.business import (
    ProjectData,
    ProjectDescription,
    ProjectEventItem,
)
from app.modules.project.model import Project

PROJECT_CREATED = "project.created"
PROJECT_UPDATED = "project.updated"


class ProjectEventData(BaseModel):
    """Payload carried in the CloudEvent `data` attribute.

    DELIBERATELY more permissive than the API schemas: events are FULL-STATE
    announcements from an authoritative producer, and rejecting one (the
    dispatch layer acks rejected payloads away) would freeze the replica at
    the previous version forever. So the email field is FloorEmail, not
    StrictEmail: only what can never be stored (NUL/NaN) or is not minimally
    shaped is rejected, and values are stored VERBATIM. Strict validation
    belongs at the API ingress, where the client can correct a 422. See
    modules/shared/validation.py.
    """

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    name: ValidName
    description: ProjectDescription = ""
    owner_email: FloorEmail
    attributes: StorableAttributes = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)


def build_project_event(
    event_type: str, project: Project, *, source: str
) -> CloudEvent:
    payload = ProjectEventData(
        id=project.id,
        name=project.name,
        description=project.description,
        owner_email=project.owner_email,
        attributes=project.attributes,
        version=project.version,
    )
    return build_state_event(event_type, payload, source=source)


def register_project_event_handlers(
    registry: EventHandlerRegistry, batcher: Batcher[ProjectEventItem]
) -> None:
    register_state_event_handlers(
        registry,
        batcher,
        event_types=(PROJECT_CREATED, PROJECT_UPDATED),
        payload_model=ProjectEventData,
        data_type=ProjectData,
    )
```

## Step 6 — `router.py`: the thin edge

Translation only: schema in, service call, schema out. The service methods
are the generic ones — `create`, `get`, `update`, `list_page`. If you find
yourself writing an `if` about domain state here, it belongs in `business.py`.

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
        project, created = await service.create(
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
        return ProjectOut.model_validate(await service.get(project_id))

    @router.patch("/{project_id}", response_model=ProjectOut)
    async def update_project(
        project_id: uuid.UUID, payload: ProjectUpdate
    ) -> ProjectOut:
        project = await service.update(
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
        page = await service.list_page(
            limit=limit, offset=offset, sort=sort,
            filters={"name": name, "owner_email": owner_email},
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

Three files change. The container builds **two object graphs** from one
codebase:

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
populated registry). Appending to `self._batchers` is what enrolls your
batcher in the restart check in `start()` and the close loop in `stop()` —
no further container edits needed:

```python
        consumer_project_service = ProjectService(
            consumer_uow_factory,
            ProjectRepository,
            event_source=self.settings.event_source,
            max_page_size=self.settings.max_page_size,
        )
        self.project_batcher = Batcher(
            consumer_project_service.apply_state_events,
            max_batch=self.settings.consumer_batch_size,
        )
        register_project_event_handlers(self.registry, self.project_batcher)
        self._batchers.append(self.project_batcher)
```

!!! note "Each module gets its own batcher"
    A batch is one database transaction for **one** business method
    (`apply_state_events` of one service). Sharing a batcher across modules
    would mix item types into one call. The registry is shared; batchers are
    per-module.

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
from app.messaging.rabbit_client_adapter import RabbitClientAdapter

EVENT_ID = "e0000000-0000-0000-0000-000000000001"
EVENT_TYPE = "project.created"
VERSION = 1


async def main() -> None:
    settings = Settings()
    bus = RabbitClientAdapter(
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
`duplicates: 1, written: 0`: the inbox filtered it before it ever
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
- [ ] Repository subclasses `VersionedRepository`: model + whitelisted
      filter/sort columns declared; no machinery copied
- [ ] Business subclasses `VersionedEntityService`: identity attributes +
      the three hooks; data shapes are pydantic models declared with the
      shared Annotated types from `modules/shared/validation.py`
      (`validate_assignment=True`, no manual validation calls); events
      staged on the UoW, never published directly
- [ ] Events: full-state payload with `version`; strict API schema,
      permissive event floor; registration via
      `register_state_event_handlers` for both event types
- [ ] Container: service in the **API graph**; service + dedicated batcher +
      handler registration in the **consumer graph**; batcher appended to
      `self._batchers` (that alone covers the restart check and shutdown)
- [ ] Router mounted in `app/api/app.py`
- [ ] Model imported in `alembic/env.py`; migration generated, **reviewed**,
      applied; `downgrade()` works
- [ ] Verified live: create twice (201 → 200), contradictory create (409),
      stale `expected_version` (409), event consumed and applied, duplicate
      event deduped, stale version dropped, nothing republished
- [ ] Tests written for the module — next chapter: [Testing](06-testing.md)
- [ ] Onboarding updated if you changed any pattern this guide teaches
      ([Maintenance Contract](08-maintenance.md))
