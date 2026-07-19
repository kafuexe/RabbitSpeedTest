# Testing Your Module

The suite has two layers with a sharp boundary, both under `tests/` and both
run by plain `pytest` (`asyncio_mode = "auto"` in `pyproject.toml` — no
`@pytest.mark.asyncio` decorators anywhere).

| Layer | Runs against | For |
|---|---|---|
| `tests/unit/` | In-memory fakes from `tests/fakes.py` — zero I/O | Business rules, batching, UoW contract, consumer dispatch, settings validation. Fast enough to run on every save |
| `tests/integration/` | Real PostgreSQL (:5434) + real RabbitMQ (:5672) | Behavior only the real stack exhibits: asyncpg error classes, `ON CONFLICT` semantics, row locks, real broker delivery/redelivery, container lifecycle |

Integration tests **auto-skip** when infrastructure is absent:
`tests/integration/conftest.py` probes `localhost:5434` / `localhost:5672`
with `socket.create_connection(timeout=0.5)` at import time and exposes
`requires_pg` / `requires_rabbit` skipif markers, applied as `pytestmark` in
every integration module. The unit suite is green anywhere.

!!! note "Why fakes instead of mocks"
    A mock asserts "this method was called". A fake behaves like the real
    thing, so unit tests assert **outcomes**: that a replayed create left one
    row, that a rolled-back UoW published nothing, that a stale event changed
    nothing. When the business layer grows a new rule, the fakes rarely need
    touching — and when they do, the docstring at the top of `tests/fakes.py`
    states exactly which real contracts they must keep mirroring.

## The fakes (`tests/fakes.py`)

The fakes are not stubs — they honor the same contracts as the real
implementations, so unit tests exercise real semantics:

| Fake | Contract it honors |
|---|---|
| `FakeEventPublisher` | Records published `CloudEvent`s in order |
| `FakeMessageBus` | `MessagePublisher` port: records `(queue, body)` pairs; `consume` is intentionally unimplemented |
| `FakeUserRepository` | Stores **copies** (like a database — mutating your instance after the call never changes the "stored" row); `insert_if_absent` returns `None` on duplicate id; `upsert_if_newer_many` applies only strictly newer versions and bumps `updated_at` like the real `ON CONFLICT ... SET` does; `list` applies filters, sorting, and pagination in memory |
| `FakeUnitOfWork` | The full UoW contract: staged events reach the publisher **only on commit**; rollback (or exiting without commit) discards them; inbox marks are **transactional** — `mark_events_processed` returns only fresh `(source, id)` pairs and they join the shared inbox only on commit, exactly like the real `processed_events` insert. Retry-after-failure tests therefore behave like the real system |
| `FakeWorld` | One object wiring all of the above: `uow_factory`, `repo_factory`, shared `store`/`inbox`/`publisher`, and a record of every UoW created (so tests can assert "one transaction") |

## What the unit suite covers

| File | Pins |
|---|---|
| `tests/unit/test_user_service.py` | Business rules end-to-end on fakes: create publishes after commit, replay is idempotent and re-announces, contradictory replay conflicts, validation, update versioning + `expected_version`, and the whole consumer path (dedup, stale drop, out-of-order upsert, batches in one transaction, highest-version-wins within a batch) |
| `tests/unit/test_batcher.py` | The greedy micro-batcher's reliability contract: ack-after-commit, coalescing, no batch-fill delay, per-item retry isolating poison items, close() failing pending submits with a nackable error (never a hang, never `CancelledError`), fresh correlation id per flush |
| `tests/unit/test_unit_of_work.py` | `SqlAlchemyUnitOfWork` publish-after-commit with a fake session: rollback discards staged events, clean read-only exit expunges instances, ambiguous commit and cancelled publish log CRITICAL with event ids, publish failure after commit is swallowed |
| `tests/unit/test_consumer.py` | Dispatch edges: invalid envelopes and unknown types are logged and **acked** (never requeued), valid events dispatch with correlation id propagated |
| `tests/unit/test_event_handling.py` | Payload floor and poison classification at the dispatch layer (permissive consumer validation, `is_permanent_data_error`, PII-free logging) |
| `tests/unit/test_cloudevents.py` | CloudEvents 1.0 envelope: byte round-trip, missing attributes and unsupported specversion rejected |
| `tests/unit/test_query.py` | Pagination/sort/filter parsing helpers (`parse_sort`, `build_filters`, `make_page_request`) |
| `tests/unit/test_settings.py` | Misconfiguration fails at startup: consuming modes reject an empty `consume_queues` |
| `tests/unit/test_supervision.py` | Per-queue consumer retry, container-owned consumer task, readiness reflecting consumer death, `stop()` surviving a crashed consumer |

## The integration suite

All fixtures live in `tests/integration/conftest.py`. The `make_container`
factory fixture is the single owner of setup/teardown: it builds a
`Container` from test settings (queues `sds-test.events.in`/`.out`,
`service_mode="api"`, `log_level="WARNING"`), calls `container.start()`, runs
`TRUNCATE users, processed_events` for a clean slate, and `stop()`s every
container it created at teardown. The plain `container` fixture is just
`make_container()` with defaults.

Two things to know before writing an integration test:

- **State isolation is TRUNCATE, not transactions.** Every container the
  factory creates starts from empty `users` and `processed_events` tables.
  Tests that need their own settings (e.g. `service_mode="both"`) call
  `make_container(service_mode="both")` directly — see the `both_container`
  fixture in `tests/integration/test_lifecycle.py`.
- **API tests never bind a port.** `tests/integration/test_api.py` mounts the
  real app on `httpx.ASGITransport`, so the full middleware/error-mapping
  stack runs in-process; the lifespan is deliberately not run because the
  `container` fixture owns start/stop.

| File | Pins |
|---|---|
| `tests/integration/test_repository.py` | `UserRepository` on real PG: `insert_if_absent` idempotency via `RETURNING`, `FOR UPDATE` row locks, list filter/sort/pagination, per-source inbox dedup, inbox marks discarded on rollback, `upsert_if_newer_many` version guard, and — critically — that the exceptions the asyncpg dialect **actually** raises for NUL bytes and NaN are classified permanent by `is_permanent_data_error` (they are generic `DBAPIError`, never `sqlalchemy.exc.DataError`) |
| `tests/integration/test_api.py` | The REST surface over in-process `httpx.ASGITransport`: 201/200 create-replay, contradictory replay 409, `expected_version` 409, 404/400/422 mapping, list pagination, `/health`, `/ready`, correlation-id echo, OpenAPI exposure |
| `tests/integration/test_messaging.py` | End-to-end through a real broker: an auxiliary `RabbitClient` client injects CloudEvents into the in-queue; tests poll the real `users` table until the consumer commits. Pins create→dedup→stale-drop ordering, junk/unknown/invalid payloads not killing the consumer, and API create publishing a spec-compliant CloudEvent after commit. Its `aux` fixture deletes both test queues before and after each test |
| `tests/integration/test_lifecycle.py` | `Container` lifecycle: readiness includes `consumer: true` only in consuming modes, a dead consumer flips readiness to `false` and logs CRITICAL, `stop()` is idempotent, and a stop→start cycle rebuilds a working batcher (regression: it used to nack forever) |

## The RabbitMQ client dependency

The `RabbitClient` client is the `rabbit-client` package from
`../rabbit-client-python`, installed into the venv as a uv path dependency
(`[tool.uv.sources]` in `pyproject.toml`). Its own tests live in that
package (`../rabbit-client-python/tests/`), not here; this suite only tests
the service's adapter and consumer wiring on top of it.

## Testing a new module

Just built the `project` module from [Adding a Module](05-adding-a-module.md)?
Mirror the `user` tests one-for-one. The files to create:

```
tests/fakes.py                                   ← EXTEND: FakeProjectRepository
tests/unit/test_project_service.py               ← NEW: business rules on fakes
tests/unit/test_project_event_handling.py        ← NEW: dispatch/poison edges
tests/integration/test_project_repository.py     ← NEW: real-PG semantics
tests/integration/test_project_api.py            ← NEW: REST surface
tests/integration/conftest.py                    ← EDIT: TRUNCATE list
```

Step by step:

**1. Extend `tests/fakes.py`** — add `FakeProjectRepository` (copy
`FakeUserRepository`, keep the copy-on-store, `insert_if_absent`-returns-None,
and version-guarded `upsert_if_newer_many` semantics) and either extend
`FakeWorld` or add a `FakeProjectWorld` with a `repo_factory` for projects.
`FakeUnitOfWork` and `FakeEventPublisher` are module-agnostic — reuse them.

**2. Create `tests/unit/test_project_service.py`** — mirror
`tests/unit/test_user_service.py`. Minimum cases:

- create happy path: publishes `project.created` after commit, version 1
- create replay: idempotent, no duplicate row, re-announces state
- contradictory replay: same id, different content → `ConflictError`
- update: bumps version, publishes `project.updated`; wrong
  `expected_version` → `ConflictError`
- consumer path via `apply_project_events`: creates and publishes **nothing**
  (null publisher), duplicate event id deduped, stale version dropped,
  update-before-create upserted, batch = one UoW, highest version wins
  within a batch

**3. Create `tests/unit/test_project_event_handling.py`** (or extend the
existing dispatch tests) — drive full event bytes through the consumer for
your registered types: permissive payloads accepted, poison payloads (fails
`is_permanent_data_error`-class storability) **acked**, not requeued.

**4. Integration** — create `tests/integration/test_project_repository.py`
and `tests/integration/test_project_api.py` mirroring their user
counterparts (repository version guard + NUL/NaN classification; API
201/200/409/404/422). Add a `projects` case to
`tests/integration/test_messaging.py` if your module consumes events.

!!! warning "Update the TRUNCATE"
    `make_container` in `tests/integration/conftest.py` runs
    `TRUNCATE users, processed_events`. Add your new table
    (`TRUNCATE users, projects, processed_events`) or tests will bleed state
    into each other.

## Running things

```bash
.venv/bin/python -m pytest                    # everything (integration auto-skips without infra)
.venv/bin/python -m pytest tests/unit         # unit only, no infra needed
.venv/bin/python -m pytest tests/integration  # integration only
.venv/bin/python -m pytest tests/unit/test_user_service.py::test_create_replay_is_idempotent_and_reannounces  # one test
```

Beyond correctness there is `scripts/benchmark.py`
(`.venv/bin/python scripts/benchmark.py [--quick]`): it measures throughput
and latency percentiles on the real stack across four sections — DAL, HTTP
API, end-to-end event path, and 1-vs-K-process scaling — and writes
`scripts/benchmark_results.json`. Run it after performance-relevant changes
(batching, pooling, indexes) and compare against the committed baseline; the
scenario list and headline numbers live in [Operations](07-operations.md).
