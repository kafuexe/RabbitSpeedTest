# Worked Example: Adding a Module

This chapter adds a complete entity — a hypothetical **`task`** — from
scratch. The recipe is deliberately short:

1. **one module file** — `app/modules/task.py`
2. **one line** in the entity registry — `ALL_SPECS` in `app/modules/__init__.py`
3. **one fixtures entry** — `tests/entity_contract/fixtures.py`
4. a generated database migration

That's it. Container wiring, router mounting, event registration, and the
behavioral test suite all iterate the registry — none of them are edited per
entity. The two shipped entities (`app/modules/user.py`,
`app/modules/project.py`) are the living template; diff anything below
against them when in doubt.

**Prerequisites:** working dev environment ([Setup](02-setup.md)), and you've
skimmed the [Architecture Tour](03-architecture-tour.md) — this chapter shows
*what to type*; that one explains *why it's shaped this way*.

## The shape: generic machinery, one-file modules

Everything that is the same for every entity lives in `app/modules/shared/`
and is **driven by your spec, not copied**:

| Shared unit | What it owns |
|---|---|
| `shared/spec.py` — `EntitySpec` + `q()` | the declaration an entity makes: its classes, `mutable_fields`, and the extension seams. `q(filter=..., sort=...)` tags columns — the single source of the query whitelists |
| `shared/repository.py` — `VersionedRepository` | idempotent `insert_if_absent`, row-locked `get_for_update`, `upsert_if_newer_many` with the version guard as a SQL `WHERE`, whitelisted filter/sort/paginate `list`. Instance-configured from your model — **no per-entity subclass** |
| `shared/service.py` — `VersionedEntityService` | the whole choreography: idempotent create with replay re-announce, optimistic update, batched idempotent `apply_state_events`, staged events (publish-after-commit). Instantiated from the spec alone; every hook has a generic default driven by `spec.mutable_fields` |
| `shared/routing.py` | the shared endpoint **bodies** (`create_and_respond`, `update_and_respond`, `list_and_respond`) and the `VersionedUpdate` base your Update schema inherits |
| `shared/schemas.py` — `Page[ItemT]` | the generic page envelope; you subclass it one line for a stable OpenAPI name |
| `shared/events.py` | CloudEvent envelope building and the generic created/updated handler registration (ack/nack policy stays in the dispatch layer) |
| `shared/wiring.py` | `build_entity_service` / `build_entity_consumer` — what the container loop calls per spec |

A module declares only what is genuinely its own: the table, the data
shapes, the strict API schemas, the response/filter models, the thin route
declarations, and one `EntitySpec` tying them together.

!!! note "Why route signatures are hand-written"
    FastAPI resolves parameter annotations at decoration time, so payload
    and filter parameters must be **concrete classes** to be visible to
    pyright and the OpenAPI schema — a TypeVar there is exactly the
    dynamic-signature trick this codebase bans. So each entity writes ~40
    lines of pure declarations, and every body is one call into
    `shared/routing.py`.

## Step 0 — Design decisions (make these first)

For `task` we choose:

| Decision | Choice | Where it lands |
|---|---|---|
| Fields | `name`, `details`, `assignee_email`, free-form `attributes` | the module file |
| Identity | client-supplied UUID (replay-safe create) | `TaskCreate.id` |
| Filterable / sortable | `name`, `assignee_email` (plus the always-sortable `id`, `version`, `created_at`, `updated_at`) | `q()` tags on the model columns |
| Event types | derived: `task.created` / `task.updated` — full state + version | `spec.name` |
| Strict vs permissive email | strict in `TaskCreate`/`TaskUpdate`, permissive floor in `TaskData` | the module file |

That last row is the one people get wrong, and it is now **structural**. The
rule of the house ([reliability model](04-reliability-model.md)): **the API
is strict** (a client can fix a 422), **the consumer is permissive**
(rejecting a full-state event freezes the replica at the old version
forever, because rejected payloads are acked away). Since the `Data` model
IS the event payload, the split is simply: strict types in
`Create`/`Update`, floor types in `Data`.

Every validation rule comes from `app/modules/shared/validation.py` — one
definition per rule. You never write a `@field_validator`; declaring a field
with a shared `Annotated` type *is* the validation, and pydantic aggregates
every field failure into one `ValidationError`:

| Annotated type | Rule + shape | Use it in |
|---|---|---|
| `ValidName` | non-blank + NUL-free, 1–200 chars | Data, Create, Update |
| `StrictEmail` | strict — pydantic's `EmailStr` rule (normalized) + storability, ≤ 320 chars | Create, Update (API ingress only) |
| `FloorEmail` | permissive — NUL-free + contains `@`, value kept verbatim, 3–320 chars | Data (business model / event payload) |
| `StorableText` | no NUL bytes; compose a length per field: `Annotated[StorableText, Field(max_length=2000)]` | free-text fields (e.g. `details`) |
| `StorableAttributes` | no NUL / NaN / Infinity anywhere in the tree (keys included) | `attributes`-style JSONB fields |

## Step 1 — The module file

One file, six sections, in a fixed order (storage → data → API schemas →
responses/filters → routes → spec). This is `app/modules/user.py` with the
names changed — read that file alongside this skeleton:

```python title="app/modules/task.py"
"""Task module — the whole entity in one file."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, Integer, String, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base
from app.modules.shared.routing import (
    VersionedUpdate, create_and_respond, list_and_respond, update_and_respond,
)
from app.modules.shared.schemas import Page
from app.modules.shared.service import VersionedEntityService
from app.modules.shared.spec import EntitySpec, StateEventItem, q
from app.modules.shared.validation import (
    FloorEmail, StorableAttributes, StorableText, StrictEmail, ValidName,
)

TaskDetails = Annotated[StorableText, Field(max_length=2000)]

# ------------------------------------------------------------------ storage

class Task(Base):
    __tablename__ = "tasks"
    __mapper_args__ = {"eager_defaults": True}  # RETURNING server defaults

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(
        String(200), nullable=False, info=q(filter=True, sort=True))
    details: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    assignee_email: Mapped[str] = mapped_column(
        String(320), nullable=False, index=True, info=q(filter=True, sort=True))
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
        onupdate=func.now(), nullable=False)

# ------------------------------- full state: business model + event payload

class TaskData(BaseModel):
    """Full desired state; ALSO the CloudEvent payload. Permissive floor —
    strictness is the API schemas' job. extra="ignore" keeps
    created_at/updated_at out of events built from ORM rows."""

    model_config = ConfigDict(extra="ignore", validate_assignment=True)

    id: uuid.UUID
    name: ValidName
    details: TaskDetails = ""
    assignee_email: FloorEmail
    attributes: StorableAttributes = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)

# ------------------------------------------------ strict API-ingress schemas

class TaskCreate(BaseModel):
    id: uuid.UUID | None = None          # client id ⇒ replay-safe create
    name: ValidName
    details: TaskDetails = ""
    assignee_email: StrictEmail
    attributes: StorableAttributes = Field(default_factory=dict)

class TaskUpdate(VersionedUpdate):
    # Sent-field semantics via model_fields_set; None means unchanged.
    name: ValidName | None = None
    details: TaskDetails | None = None
    assignee_email: StrictEmail | None = None
    attributes: StorableAttributes | None = None

class TaskOut(TaskData):
    # Redeclare defaulted fields without defaults so responses keep them
    # required (see UserOut for the pyright note).
    model_config = ConfigDict(from_attributes=True)

    details: str = Field(...)  # pyright: ignore[reportGeneralTypeIssues]
    attributes: dict[str, Any] = Field(...)  # pyright: ignore[reportGeneralTypeIssues]
    version: int = Field(...)  # pyright: ignore[reportGeneralTypeIssues]
    created_at: datetime
    updated_at: datetime

class TaskPageOut(Page[TaskOut]):
    pass  # explicit subclass ⇒ stable OpenAPI schema name

class TaskFilters(BaseModel):
    """Mirrors the q(filter=True) tags — the contract suite enforces it."""
    name: str | None = None
    assignee_email: str | None = None

TaskService = VersionedEntityService[Task, TaskData, TaskUpdate]
TaskEventItem = StateEventItem[TaskData]

# ------------------------------------------------------------------- routes

def build_task_router(service: TaskService) -> APIRouter:
    router = APIRouter(prefix="/tasks", tags=["tasks"])

    @router.post("", response_model=TaskOut, status_code=status.HTTP_201_CREATED)
    async def create_task(payload: TaskCreate, response: Response) -> TaskOut:
        return await create_and_respond(
            TASK_SPEC, service, payload, response, out=TaskOut)

    @router.get("/{task_id}", response_model=TaskOut)
    async def get_task(task_id: uuid.UUID) -> TaskOut:
        return TaskOut.model_validate(await service.get(task_id))

    @router.patch("/{task_id}", response_model=TaskOut)
    async def update_task(task_id: uuid.UUID, payload: TaskUpdate) -> TaskOut:
        return await update_and_respond(service, task_id, payload, out=TaskOut)

    @router.get("", response_model=TaskPageOut)
    async def list_tasks(
        limit: int = Query(default=50),
        offset: int = Query(default=0),
        sort: str | None = Query(default=None, description="field or -field"),
        name: str | None = Query(default=None),
        assignee_email: str | None = Query(default=None),
    ) -> TaskPageOut:
        return await list_and_respond(
            service, limit=limit, offset=offset, sort=sort,
            filters=TaskFilters(name=name, assignee_email=assignee_email),
            out=TaskOut, page_out=TaskPageOut)

    return router

# ---------------------------------------------------------------------- spec

TASK_SPEC = EntitySpec(
    name="task",                       # ⇒ task.created / task.updated
    model=Task,
    data=TaskData,
    create=TaskCreate,
    update=TaskUpdate,
    out=TaskOut,
    filters=TaskFilters,
    mutable_fields=("name", "details", "assignee_email", "attributes"),
    router_factory=build_task_router,
)
```

Things worth noticing:

- **No service, repository, or handler class.** The generic
  `VersionedEntityService` and `VersionedRepository` are configured entirely
  by the spec: `mutable_fields` drives entity construction, replay equality,
  and update application; the `q()` tags drive the query whitelists (`id`,
  `version`, `created_at`, `updated_at` are always sortable).
- **No event constants and no event payload class.** Event type names derive
  from `spec.name`; `TaskData` IS the payload, and its `extra="ignore"`
  keeps server timestamps out of events structurally.
- **Update semantics.** The service applies exactly the mutable fields the
  client actually sent (`model_fields_set`) whose value is not None — an
  explicit `null` still means "leave unchanged", and an empty PATCH is a
  400. `expected_version` comes from the inherited `VersionedUpdate`.

## Step 2 — Register the spec

```python title="app/modules/__init__.py (the entity registry)"
from app.modules.task import TASK_SPEC

ALL_SPECS: tuple[EntitySpec[Any, Any, Any], ...] = (
    USER_SPEC, PROJECT_SPEC, TASK_SPEC,
)
```

This is **the only wiring edit in the application**. The container builds
`services[spec.name]` and one consumer batcher per spec by looping
`ALL_SPECS`; `app/api/app.py` mounts `spec.router_factory(...)` in tuple
order; `alembic/env.py` imports the registry, so autogenerate sees your
table too. Import-time asserts catch duplicate names and copy-paste class
mistakes before the first request.

## Step 3 — The contract-suite fixtures entry

`tests/entity_contract/` is a parametrized behavioral contract that runs
against **every** spec in `ALL_SPECS` on real PostgreSQL: CRUD (201 /
replay-200 / contradictory-409 / 404 / patch semantics / empty-patch-400 /
null-means-unchanged), list whitelists derived from your `q()` tags, event
choreography (out-of-order, duplicate delivery, highest-version-wins), and
sync guards (e.g. your `Filters` model must match your `q(filter=True)`
tags). A spec without a fixtures entry **fails collection** — you cannot
forget this step.

```python title="tests/entity_contract/fixtures.py (add one entry)"
_TASK_ID = uuid.UUID("00000000-0000-0000-0000-0000000000aa")

FIXTURES["task"] = EntityFixtures(
    path="/tasks",
    make_valid_data=lambda: TaskData(
        id=_TASK_ID, name="Ship it", details="v1",
        assignee_email="ada@example.com", attributes={"p": 1}),
    make_second_valid_data=lambda: TaskData(   # same id, different content
        id=_TASK_ID, name="Rewrite it", details="v2",
        assignee_email="grace@example.com", attributes={"p": 2}),
    make_valid_create=lambda: {
        "id": str(_TASK_ID), "name": "Ship it", "details": "v1",
        "assignee_email": "ada@example.com", "attributes": {"p": 1}},
    make_valid_update=lambda: {"details": "now with tests"},
    make_invalid_update_cases=lambda: [
        {"name": "   "},                      # blank-after-strip
        {"assignee_email": "ops@backend"},    # API email stays strict
    ],
)
```

(The real file uses a literal dict — add your entry alongside `"user"` and
`"project"`.)

Also add your table to the TRUNCATE in `tests/integration/conftest.py`
(`TRUNCATE users, projects, tasks, processed_events`) so tests start clean.

## Step 4 — The migration

Nothing to edit in `alembic/env.py` — it imports the registry. Generate,
**review**, apply (from `shared_data_service/`):

```bash
.venv/bin/python -m alembic revision --autogenerate -m "tasks table"
# review alembic/versions/<new file> — it must create ONLY the tasks table
.venv/bin/python -m alembic upgrade head
```

## Step 5 — Prove it works

**First, the contract suite** — your entity appears in every parametrized
test automatically:

```bash
.venv/bin/python -m pytest tests/entity_contract -q
# ...::test_create_replay_returns_200_and_reannounces[task] PASSED  etc.
```

That suite *is* the reliability model exercised live: idempotent create and
re-announce, contradictory replay 409, optimistic versioning, whitelists,
inbox dedup, version-guarded upserts, highest-version-wins.

**Then, if you want to see it with your own eyes**, start the service in
`both` mode (`.venv/bin/python main.py`) and run the classic experiments —
they work identically for any entity (swap the path and body):

1. **Idempotent create** — POST the same body twice: `201`, then `200` with
   the same stored row (and the `task.created` event re-announced — the
   crash-recovery path).
2. **Contradictory replay** — same id, different name → `409`.
3. **Optimistic update** — PATCH with `"expected_version": 1`: succeeds
   (version 1 → 2); run it again unchanged → `409`.
4. **The consumer path** — publish a `task.created` CloudEvent into the
   inbound queue (any AMQP client; see `tests/integration/test_messaging.py`
   for a working producer). The log shows `task events applied` with
   `batch: 1, fresh: 1`. Publish the *same event id* again →
   `duplicates: 1, written: 0` (the inbox). Publish a **stale version** →
   `fresh: 1` but the row does not move backwards (the version guard).
5. **The consumer did not republish** — the outbound queue gained nothing
   from step 4: that is the `NullEventPublisher` in the consumer graph.

## Extension points (when your entity isn't plain CRUD)

The generic machinery is a default, not a cage. Three seams, all declared on
the spec:

**Custom behavior — `service_cls`.** Subclass the generic service, override
or extend the hooks (they are ordinary methods with generic defaults —
`_new_entity`, `_content_matches`, `_build_event`, `_apply_changes` — and
`super()` works), add new verbs:

```python
class OrderService(VersionedEntityService[Order, OrderData, OrderUpdate]):
    async def cancel(self, order_id: uuid.UUID) -> Order: ...

    def _content_matches(self, entity: Order, data: OrderData) -> bool:
        # tighten replay equality, then defer to the generic rule
        return entity.currency == data.currency and super()._content_matches(entity, data)

ORDER_SPEC = EntitySpec(..., service_cls=OrderService,
                        field_validators={"currency": valid_iso4217})
```

(`field_validators` is the seam for a per-field rule that cannot live in an
Annotated type; it runs on create over all mutable fields and on update over
the fields actually sent.)

**Extra routes.** Your module owns its router factory — add endpoints beyond
CRUD right there. One wrinkle: the spec types `router_factory` over the
*base* service (callables are contravariant in parameters), so a module with
a custom `service_cls` casts once inside its own factory:

```python
def build_order_router(service: VersionedEntityService[Order, OrderData, OrderUpdate]) -> APIRouter:
    order_service = cast(OrderService, service)  # the wiring built exactly this class
    router = APIRouter(prefix="/orders", tags=["orders"])
    # ...the four CRUD declarations...

    @router.post("/{order_id}/cancel", response_model=OrderOut)
    async def cancel_order(order_id: uuid.UUID) -> OrderOut:
        return OrderOut.model_validate(await order_service.cancel(order_id))

    return router
```

**Extra / different event handling.** Two optional spec fields, consumed by
`build_entity_consumer` so the container loop never grows special cases:
`extra_event_handlers` registers handlers for event types beyond
created/updated (e.g. `order.cancelled`); `register_events` *replaces* the
generic created/updated registration entirely, for a module whose
consumption contract genuinely differs.

## Definition of done — checklist

- [ ] `app/modules/<entity>.py` follows the section order of
      `app/modules/user.py`; no imports from sibling entity modules
- [ ] Strict types (`StrictEmail`, …) only in Create/Update; the floor in
      Data; Data has `extra="ignore"` + `validate_assignment=True`
- [ ] Columns tagged with `q()`; `Filters` model mirrors the filter tags
- [ ] Spec registered in `ALL_SPECS` (the only wiring edit)
- [ ] Fixtures entry added; table added to the conftest TRUNCATE
- [ ] Migration generated, **reviewed**, applied; `downgrade()` works
- [ ] `pytest tests/entity_contract` green with your entity's id in every
      parametrized test; `uvx pyright` clean
- [ ] Entity-specific rules (beyond the contract) get their own tests —
      next chapter: [Testing](06-testing.md)
- [ ] Onboarding updated if you changed any pattern this guide teaches
      ([Maintenance Contract](08-maintenance.md))
