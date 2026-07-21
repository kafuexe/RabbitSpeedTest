# Worked Example: Adding a Module

This chapter adds a complete module — a hypothetical **`task`** — from
scratch. The recipe is deliberately short:

1. **one module file** — `app/modules/task.py`
2. **one line** in the module registry — `ALL_SPECS` in `app/modules/__init__.py`
3. **one fixtures entry** — `tests/module_contract/fixtures.py`
4. a generated database migration

That's it. Container wiring, router mounting, event registration, and the
behavioral test suite all iterate the registry — none of them are edited per
module. The two shipped entities (`app/modules/user.py`,
`app/modules/project.py`) are the living template; diff anything below
against them when in doubt.

**Prerequisites:** working dev environment ([Setup](02-setup.md)), and you've
skimmed the [Architecture Tour](03-architecture-tour.md) — this chapter shows
*what to type*; that one explains *why it's shaped this way*.

## The shape: generic machinery, one-file modules

Everything that is the same for every module lives in `app/modules/shared/`
and is **driven by your spec, not copied**:

| Shared unit | What it owns |
|---|---|
| `shared/spec.py` — `ModuleSpec` + `q()` | the declaration a module makes: its classes, `mutable_fields`, and the extension seams (`service_cls`, `routes_cls`, `scope_parent`/`also_unscoped`). `q(filter=..., sort=...)` tags columns — the single source of the query whitelists |
| `shared/repository.py` — `VersionedRepository` | idempotent `insert_if_absent`, row-locked `get_for_update`, `upsert_if_newer_many` with the version guard as a SQL `WHERE`, whitelisted filter/sort/paginate `list`. Instance-configured from your model — **no per-module subclass** |
| `shared/service.py` — `VersionedModuleService` | the whole choreography: idempotent create with replay re-announce, optimistic update, batched idempotent `apply_state_events`, staged events (publish-after-commit). Instantiated from the spec alone; every hook has a generic default driven by `spec.mutable_fields` |
| `shared/routes.py` — `ModuleRoutes`, `ScopedModuleRoutes` | generates the four CRUD routes for any spec: a LOGIC layer (`create`/`get_one`/`update`/`list` — the override surface) and a SIGNATURE layer that hands FastAPI concrete per-module annotations. `ScopedModuleRoutes` nests them under a parent (`/{project_id}/user`) — see [Nested routing](#nested-scoped-routing). No route code lives in the module file |
| `shared/schemas.py` — `Page[ItemT]`, `Pagination`, `VersionedUpdate` | the generic page envelope (subclass one line for a stable OpenAPI name), the shared `limit`/`offset`/`sort` query surface your `<Module>ListParams` composes with your filters, and the `VersionedUpdate` base your Update schema inherits |
| `shared/filters.py` — `parse_filter_params`, `apply_filter` | the Django-style `field__op` filter engine every list route uses (whitelisting, type coercion, LIKE-escaping) — see [Filtering the list endpoint](#filtering-the-list-endpoint) |
| `shared/events.py` | CloudEvent envelope building and the generic created/updated handler registration (ack/nack policy stays in the dispatch layer) |
| `shared/wiring.py` | `build_module_service` / `build_module_consumer` — what the container loop calls per spec |

A module declares only what is genuinely its own: the table, the data
shapes, the strict API schemas, the response/filter models, and one
`ModuleSpec` tying them together. It ends at the spec — the routes come from
the shared `ModuleRoutes`.

!!! note "How routes are generated"
    FastAPI resolves parameter annotations at decoration time, so payload
    and filter parameters must be **concrete classes** to be visible to
    pyright and the OpenAPI schema. `ModuleRoutes` (`shared/routes.py`)
    supplies them dynamically — its signature-layer factories annotate the
    inner endpoints with `self.spec.create` etc. (which FastAPI evaluates
    eagerly to the real class). That is why `routes.py` deliberately has **no**
    `from __future__ import annotations`, and why the only `# type: ignore`
    in the codebase sits on its three dynamic-annotation lines. Your module
    file writes no route code at all.

## Step 0 — Design decisions (make these first)

For `task` we choose:

| Decision | Choice | Where it lands |
|---|---|---|
| Fields | `name`, `details`, `assignee_email`, free-form `attributes` | the module file |
| Identity | client-supplied UUID (replay-safe create) | `TaskCreate.id` |
| Filterable / sortable | `name`, `assignee_email` (plus the always-sortable `id`, `version`, `created_at`, `updated_at`) | `q()` tags on the model columns |
| Event types | derived: `task.created` / `task.updated` — full state + version | `spec.name` |
| Strict vs permissive email | strict in `TaskCreate`/`TaskUpdate`, permissive floor in `TaskData` | the module file |
| Root or scoped? | `task` is a **root** at `/task`. To nest under a parent instead (like `user` under `project`), set `scope_parent` + a scope column — see [Nested routing](#nested-scoped-routing) | `ModuleSpec.scope_parent` |

The list endpoint gets Django-style filter operators for free — see
[Filtering the list endpoint](#filtering-the-list-endpoint).

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
"""Task module — the whole module in one file."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, Integer, String, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base
from app.modules.shared.schemas import Page, Pagination, VersionedUpdate
from app.modules.shared.service import VersionedModuleService
from app.modules.shared.spec import ModuleSpec, q
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

class TaskOut(BaseModel):
    # Plain field types on PURPOSE, NOT TaskData's floor types: a response
    # model re-validates the stored row, so inheriting the floor would 500
    # on any out-of-band row that violates it — and plain types keep the
    # response schema free of the floor's min/max-length annotations.
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    details: str
    assignee_email: str
    attributes: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime

class TaskPageOut(Page[TaskOut]):
    pass  # explicit subclass ⇒ stable OpenAPI schema name

class TaskFilters(BaseModel):
    """Mirrors the q(filter=True) tags — the contract suite enforces it.
    Kept PURE (filters only) so it is TASK_SPEC.filters."""
    name: str | None = None
    assignee_email: str | None = None

class TaskListParams(TaskFilters, Pagination):
    """The list endpoint's flattened query model: filters + the shared
    Pagination surface (limit/offset/sort). FastAPI flattens exactly ONE
    query-param model per endpoint, so the two compose here."""

TaskService = VersionedModuleService[Task, TaskData, TaskUpdate]

# ---------------------------------------------------------------------- spec
# The module ends here — no route code. The four CRUD routes at /task,
# /task/{task_id} are generated by the shared ModuleRoutes from TASK_SPEC.

TASK_SPEC = ModuleSpec(
    name="task",                       # ⇒ task.created / task.updated, /task
    model=Task,
    data=TaskData,
    create=TaskCreate,
    update=TaskUpdate,
    out=TaskOut,
    filters=TaskFilters,
    page_out=TaskPageOut,
    list_params=TaskListParams,
    mutable_fields=("name", "details", "assignee_email", "attributes"),
)
```

Things worth noticing:

- **No service, repository, or handler class.** The generic
  `VersionedModuleService` and `VersionedRepository` are configured entirely
  by the spec: `mutable_fields` drives module construction, replay equality,
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

```python title="app/modules/__init__.py (the module registry)"
from app.modules.task import TASK_SPEC

ALL_SPECS: tuple[ModuleSpec[Any, Any, Any], ...] = (
    USER_SPEC, PROJECT_SPEC, TASK_SPEC,
)
```

This is **the only wiring edit in the application**. The container builds
`services[spec.name]` and one consumer batcher per spec by looping
`ALL_SPECS`; `app/api/app.py` mounts `(spec.routes_cls or ModuleRoutes)(spec,
service).register()` in tuple order; `alembic/env.py` imports the registry,
so autogenerate sees your table too. Import-time asserts catch duplicate
names and copy-paste class mistakes before the first request.

## Step 3 — The contract-suite fixtures entry

`tests/module_contract/` is a parametrized behavioral contract that runs
against **every** spec in `ALL_SPECS` on real PostgreSQL: CRUD (201 /
replay-200 / contradictory-409 / 404 / patch semantics / empty-patch-400 /
null-means-unchanged), list whitelists derived from your `q()` tags, event
choreography (out-of-order, duplicate delivery, highest-version-wins), and
sync guards (e.g. your `Filters` model must match your `q(filter=True)`
tags). A spec without a fixtures entry **fails collection** — you cannot
forget this step.

```python title="tests/module_contract/fixtures.py (add one entry)"
_TASK_ID = uuid.UUID("00000000-0000-0000-0000-0000000000aa")

FIXTURES["task"] = ModuleFixtures(
    path="/task",
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

The integration/contract fixtures TRUNCATE every registered module's table
automatically — the list is derived from `ALL_SPECS` in
`tests/integration/conftest.py`, so registering your spec is all it takes.

## Step 4 — The migration

Nothing to edit in `alembic/env.py` — it imports the registry. Generate,
**review**, apply (from `shared_data_service/`):

```bash
.venv/bin/python -m alembic revision --autogenerate -m "tasks table"
# review alembic/versions/<new file> — it must create ONLY the tasks table
.venv/bin/python -m alembic upgrade head
```

## Step 5 — Prove it works

**First, the contract suite** — your module appears in every parametrized
test automatically:

```bash
.venv/bin/python -m pytest tests/module_contract -q
# ...::test_create_replay_returns_200_and_reannounces[task] PASSED  etc.
```

That suite *is* the reliability model exercised live: idempotent create and
re-announce, contradictory replay 409, optimistic versioning, whitelists,
inbox dedup, version-guarded upserts, highest-version-wins.

**Then, if you want to see it with your own eyes**, start the service in
`both` mode (`.venv/bin/python main.py`) and run the classic experiments —
they work identically for any module (swap the path and body):

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

## Filtering the list endpoint

Every module's `GET` list route accepts Django-style lookups as query
params — `field__op=value`, where a bare `field=value` means `exact`. The
field must be `q(filter=True)`-tagged and the operator must be one of the
supported lookups; anything else is a `400` (`InvalidQueryError`), never
raw SQL. The engine lives in `app/modules/shared/filters.py`.

`task` tags `name` and `assignee_email` as `q(filter=True)`, so those are
the fields you can filter on:

```bash
GET /task?name__icontains=ship               # case-insensitive substring
GET /task?assignee_email=ada@example.com     # bare ⇒ exact
GET /task?name__istartswith=sh               # prefix
GET /task?assignee_email__in=a@x.com,b@x.com # comma-separated membership
GET /task?name__icontains=ship&assignee_email__endswith=@acme.com   # two params ⇒ AND
```

| Operator | Meaning | Type |
|---|---|---|
| `exact` (default) | equals | any |
| `iexact` | case-insensitive equals | text only |
| `contains` / `icontains` | substring / case-insensitive | text only |
| `startswith` / `istartswith` | prefix / case-insensitive | text only |
| `endswith` / `iendswith` | suffix / case-insensitive | text only |
| `gt` / `gte` / `lt` / `lte` | comparisons | any comparable |
| `in` / `not_in` | membership | any; comma-separated values |
| `isnull` / `not_isnull` | `IS [NOT] NULL` | any; value `true`/`false` |
| `range` | `BETWEEN lo AND hi` | any comparable; two comma-separated |

The comparison, `range`, and membership operators work on any filter-tagged
column of the matching type — tag an `int` or timestamp column
`q(filter=True)` and `priority__gte=3` or `due_at__range=2026-01-01,2026-12-31`
just work (only `name`/`assignee_email` are tagged in this example, so those
particular queries would 400 here with "cannot filter by …").

Three guarantees you get for free: the value is **coerced to the column's
Python type** (a non-numeric value on an int column is a 400, not a SQL
error); `%` and `_` in `*contains`/`*startswith`/`*endswith` values are
**escaped** so they match literally, never as wildcards; and a text-only
operator on a non-text column (e.g. `iexact`/`icontains` on an int) is a
400. Nothing to wire — the list route documents the available fields and
operators in its OpenAPI description automatically.

## Nested (scoped) routing

By default a module is a **root** at `/{name}` (like `project`). A module
can instead be **scoped under a parent** — its routes nest under the
parent's id and every operation is confined to that parent. `user` is
scoped under `project`:

| Route | What it does |
|---|---|
| `POST /{project_id}/user` | create a user **in that project** (sets `project_id` from the path) |
| `GET \| PATCH /{project_id}/user/{user_id}` | 404 if the user's `project_id` ≠ the path id |
| `GET /{project_id}/user` | lists **only that project's** users |
| `GET \| … /users` | the top-level **unscoped** view — all users, every project |

You opt in with two spec fields plus a scope column on the model:

```python
class User(Base):
    # the scope column: {parent}_id, nullable, filterable
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True, index=True, info=q(filter=True))
    # ... name, email, attributes, version, timestamps ...

USER_SPEC = ModuleSpec(
    name="user",
    # project_id is a mutable field (the scoped create sets it) but NOT on
    # UserUpdate — so a PATCH can never re-scope a user to another project.
    mutable_fields=("project_id", "name", "email", "attributes"),
    scope_parent="project",   # nest under /{project_id}/, scope by project_id
    also_unscoped=True,        # ALSO expose the top-level /users route
    ...,
)
```

What the shared machinery enforces (in `ScopedModuleRoutes`,
`app/modules/shared/routes.py`), so you write none of it:

- **create** sets the scope column from the path id;
- **get / update** 404 on a cross-scope id — you can never read or mutate
  another parent's row through the nested route;
- **list** forces `WHERE project_id = {path id}` **and strips** any
  client-supplied `project_id` filter, so a caller cannot widen past its
  parent;
- `also_unscoped=True` additionally mounts the flat, plural `/users` route
  (unscoped: all rows) — omit it for a strictly-nested module.

The scope column must exist on the model and be filterable; the contract
suite's filter test skips it (it is set by the route, not the create body),
and `tests/integration/test_scoped_routing.py` pins the confinement rules.

## Extension points (when your module isn't plain CRUD)

The generic machinery is a default, not a cage. Three seams, all declared on
the spec:

**Custom behavior — `service_cls`.** Subclass the generic service, override
or extend the hooks (they are ordinary methods with generic defaults —
`_new_module`, `_content_matches`, `_build_event`, `_apply_changes` — and
`super()` works), add new verbs:

```python
class OrderService(VersionedModuleService[Order, OrderData, OrderUpdate]):
    async def cancel(self, order_id: uuid.UUID) -> Order: ...

    def _content_matches(self, module: Order, data: OrderData) -> bool:
        # tighten replay equality, then defer to the generic rule
        return module.currency == data.currency and super()._content_matches(module, data)

ORDER_SPEC = ModuleSpec(..., service_cls=OrderService,
                        field_validators={"currency": valid_iso4217})
```

(`field_validators` is the seam for a per-field rule that cannot live in an
Annotated type; it runs on create over all mutable fields and on update over
the fields actually sent.)

**Custom behavior and extra routes.** Subclass `ModuleRoutes`, override a
logic method (calling `super()`) and/or `extra_routes`, and point the spec at
it with `routes_cls=`. The four CRUD routes still come for free; you add only
what differs:

```python
class OrderRoutes(ModuleRoutes[Order, OrderData, OrderUpdate]):
    async def create(self, payload, response):
        result = await super().create(payload, response)   # reuse the choreography
        await self._notify_fulfilment(result)              # ...then a side effect
        return result

    def extra_routes(self, router: APIRouter) -> None:      # endpoints beyond CRUD
        @router.post("/{order_id}/cancel", response_model=self.spec.out)
        async def cancel_order(order_id: uuid.UUID):
            svc = cast(OrderService, self.service)          # wiring built exactly this
            return self.spec.out.model_validate(await svc.cancel(order_id))

ORDER_SPEC = ModuleSpec(..., service_cls=OrderService, routes_cls=OrderRoutes)
```

**Extra / different event handling.** Two optional spec fields, consumed by
`build_module_consumer` so the container loop never grows special cases:
`extra_event_handlers` registers handlers for event types beyond
created/updated (e.g. `order.cancelled`); `register_events` *replaces* the
generic created/updated registration entirely, for a module whose
consumption contract genuinely differs.

## Definition of done — checklist

- [ ] `app/modules/<module>.py` follows the section order of
      `app/modules/user.py`; no imports from sibling modules
- [ ] Strict types (`StrictEmail`, …) only in Create/Update; the floor in
      Data; Data has `extra="ignore"` + `validate_assignment=True`
- [ ] Columns tagged with `q()`; pure `Filters` model mirrors the filter
      tags; `ListParams(Filters, Pagination)` is the list route's query model
- [ ] Spec registered in `ALL_SPECS` (the only wiring edit)
- [ ] Fixtures entry added (the conftest TRUNCATE derives from ALL_SPECS)
- [ ] Migration generated, **reviewed**, applied; `downgrade()` works
- [ ] `pytest tests/module_contract` green with your module's id in every
      parametrized test; `uvx pyright` clean
- [ ] Module-specific rules (beyond the contract) get their own tests —
      next chapter: [Testing](06-testing.md)
- [ ] Onboarding updated if you changed any pattern this guide teaches
      ([Maintenance Contract](08-maintenance.md))
