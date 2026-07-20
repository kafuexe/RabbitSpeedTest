# Testing Your Module

The suite has three layers with sharp boundaries, all under `tests/` and all
run by plain `pytest` (`asyncio_mode = "auto"` in `pyproject.toml` — no
`@pytest.mark.asyncio` decorators anywhere).

| Layer | Runs against | For |
|---|---|---|
| `tests/unit/` | In-memory fakes from `tests/fakes.py` — zero I/O | The shared choreography, batching, UoW contract, consumer dispatch, settings validation. Fast enough to run on every save |
| `tests/entity_contract/` | Real PostgreSQL + the real app, **parametrized over every spec in `ALL_SPECS`** | The behavioral contract every entity must satisfy: CRUD semantics, list whitelists, event choreography, and infra-free sync guards. A new entity gets the whole suite from one fixtures entry — and a spec *without* one fails collection |
| `tests/integration/` | Real PostgreSQL (:5434) + real RabbitMQ (:5672) | Behavior only the real stack exhibits: asyncpg error classes, `ON CONFLICT` semantics, row locks, real broker delivery/redelivery, container lifecycle, plus entity-specific API cases the contract doesn't own |

Infra-dependent tests **auto-skip** when infrastructure is absent:
`tests/integration/conftest.py` probes `localhost:5434` / `localhost:5672`
with `socket.create_connection(timeout=0.5)` at import time and exposes
`requires_pg` / `requires_rabbit` skipif markers, applied as `pytestmark` in
every integration and contract module (the contract's sync guards in
`test_sync_contract.py` carry no marker — they are pure-Python and run
anywhere). The unit suite is green anywhere. Static checking runs with
pyright in strict mode (`uvx pyright`, configured by `pyrightconfig.json`).

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

The service takes an optional `repo_factory` precisely for this seam: unit
tests build `VersionedEntityService(USER_SPEC, world.uow_factory,
repo_factory=world.repo_factory, ...)` and the whole choreography runs
against the in-memory fake.

## What the unit suite covers

| File | Pins |
|---|---|
| `tests/unit/test_user_service.py` | The generic choreography end-to-end on fakes (instantiated with `USER_SPEC`): create publishes after commit, replay is idempotent and re-announces, contradictory replay conflicts, validation-by-construction, update versioning + `expected_version`, and the whole consumer path (dedup, stale drop, out-of-order upsert, batches in one transaction, highest-version-wins within a batch). This is the infra-free twin of the contract suite — it covers the shared machinery even where no database exists |
| `tests/unit/test_batcher.py` | The greedy micro-batcher's reliability contract: ack-after-commit, coalescing, no batch-fill delay, per-item retry isolating poison items, close() failing pending submits with a nackable error (never a hang, never `CancelledError`), fresh correlation id per flush |
| `tests/unit/test_unit_of_work.py` | `SqlAlchemyUnitOfWork` publish-after-commit with a fake session: rollback discards staged events, clean read-only exit expunges instances, ambiguous commit and cancelled publish log CRITICAL with event ids, publish failure after commit is swallowed |
| `tests/unit/test_consumer.py` | Dispatch edges: invalid envelopes and unknown types are logged and **acked** (never requeued), valid events dispatch with correlation id propagated |
| `tests/unit/test_event_handling.py` | Payload floor and poison classification at the dispatch layer (permissive consumer validation, `is_permanent_data_error`, PII-free logging) |
| `tests/unit/test_cloudevents.py` | CloudEvents 1.0 envelope: byte round-trip, missing attributes and unsupported specversion rejected |
| `tests/unit/test_query.py` | Pagination/sort/filter parsing helpers (`parse_sort`, `build_filters`, `make_page_request`) |
| `tests/unit/test_settings.py` | Misconfiguration fails at startup: consuming modes reject an empty `consume_queues` |
| `tests/unit/test_supervision.py` | Per-queue consumer retry, container-owned consumer task, readiness reflecting consumer death, `stop()` surviving a crashed consumer |
| `tests/unit/test_engine_tls.py` | TLS/CA settings: `SDS_DB_CA_FILE` ssl-context wiring and fail-at-startup on an invalid CA bundle |

## The integration suite

All fixtures live in `tests/integration/conftest.py`. The `make_container`
factory fixture is the single owner of setup/teardown: it builds a
`Container` from test settings (queues `sds-test.events.in`/`.out`,
`service_mode="api"`, `log_level="WARNING"`), calls `container.start()`, runs
`TRUNCATE users, projects, processed_events` for a clean slate, and `stop()`s
every container it created at teardown. The plain `container` fixture is just
`make_container()` with defaults; the contract suite's `conftest.py`
re-exports these same fixtures.

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
| `tests/integration/test_repository.py` | The generic `VersionedRepository` (bound to `User`) on real PG: `insert_if_absent` idempotency via `RETURNING`, `FOR UPDATE` row locks, list filter/sort/pagination, per-source inbox dedup, inbox marks discarded on rollback, `upsert_if_newer_many` version guard, and — critically — that the exceptions the asyncpg dialect **actually** raises for NUL bytes and NaN are classified permanent by `is_permanent_data_error` (they are generic `DBAPIError`, never `sqlalchemy.exc.DataError`) |
| `tests/integration/test_api.py` | What the contract suite does *not* own: strict-email 422 at the API edge, error bodies carrying `correlation_id`, list result **content** (ordering, filter hits), `/health`, `/ready`, correlation-id echo, OpenAPI exposure — over in-process `httpx.ASGITransport` |
| `tests/integration/test_messaging.py` | End-to-end through a real broker: an auxiliary `RabbitClient` client injects CloudEvents into the in-queue; tests poll the real `users` table until the consumer commits. Pins create→dedup→stale-drop ordering, junk/unknown/invalid payloads not killing the consumer, and API create publishing a spec-compliant CloudEvent after commit. Its `aux` fixture deletes both test queues before and after each test |
| `tests/integration/test_lifecycle.py` | `Container` lifecycle: readiness includes `consumer: true` only in consuming modes, a dead consumer flips readiness to `false` and logs CRITICAL, `stop()` is idempotent, and a stop→start cycle rebuilds working batchers (regression: they used to nack forever) |

## The entity-contract suite

`tests/entity_contract/` is the third layer and the one that scales with the
entity count. Every test is
`@pytest.mark.parametrize("spec", ALL_SPECS, ids=lambda s: s.name)`:

| File | Pins (per entity) |
|---|---|
| `test_crud_contract.py` | 201 create + announce, replay 200 + re-announce, contradictory 409, 404, patch + version bump, `expected_version` 409, empty patch 400, explicit-null-means-unchanged, invalid updates 422 |
| `test_list_contract.py` | pagination bounds 400, sort accepts every `q(sort=True)` + always-sortable field and rejects others, filters accept tagged fields and match created rows, unknown filters rejected at the service level |
| `test_event_contract.py` | event payload key set == the Data model's declared fields (the byte-compat guard), out-of-order apply, duplicate delivery no-op, within-batch highest-version-wins |
| `test_sync_contract.py` | infra-free drift guards: Filters ⇔ `q(filter=True)` tags, `mutable_fields` ⊆ model columns ∩ Data fields, Create/Update fields ⊆ Data fields, event-type names derive from `spec.name`, every spec has fixtures |

Per-entity test data lives in `tests/entity_contract/fixtures.py` — one
`EntityFixtures` entry per spec (valid data, second-valid-data with the same
id, create/update bodies, invalid-update cases). The suite's `conftest.py`
asserts at import time that the fixtures mapping and `ALL_SPECS` agree, so a
missing entry is a collection **error**, never a silent skip.

## The RabbitMQ client dependency

The `RabbitClient` client is the `hs-rabbit-client` package from
`../rabbit-client-python`, installed into the venv as a uv path dependency
(`[tool.uv.sources]` in `pyproject.toml`). Its own tests live in that
package (`../rabbit-client-python/tests/`), not here; this suite only tests
the service's adapter and consumer wiring on top of it.

## Testing a new module

Just built an entity from [Adding a Module](05-adding-a-module.md)? The
behavioral baseline is **not** copied test files — it's one entry:

```
tests/entity_contract/fixtures.py   ← ADD: your EntityFixtures entry
```

With that one edit your entity runs through every CRUD/list/event/sync
contract test automatically (and *without* the fixtures entry the suite
refuses to collect). What still deserves hand-written tests is what the
contract cannot know: rules unique to your entity — a strictness asymmetry
like the email one, a custom `service_cls` verb, an `extra_event_handlers`
type, a `_validate_data` override. Put those in `tests/unit/` (dispatch/
validation edges — see `tests/unit/test_event_handling.py` for the shape) or
`tests/integration/test_api.py`-style files (HTTP-observable specifics).

!!! note "State isolation is automatic"
    `make_container` in `tests/integration/conftest.py` TRUNCATEs every
    registered entity's table plus `processed_events` — the list derives
    from `ALL_SPECS`, so a new entity is covered the moment its spec is
    registered.

## Running things

```bash
.venv/bin/python -m pytest                      # everything (infra tests auto-skip without infra)
.venv/bin/python -m pytest tests/unit           # unit only, no infra needed
.venv/bin/python -m pytest tests/entity_contract  # the per-entity behavioral contract
.venv/bin/python -m pytest tests/integration    # integration only
.venv/bin/python -m pytest tests/unit/test_user_service.py::test_create_replay_is_idempotent_and_reannounces  # one test
uvx pyright                                     # strict type checking (pyrightconfig.json)
```

Beyond correctness there is `scripts/benchmark.py`
(`.venv/bin/python scripts/benchmark.py [--quick]`): it measures throughput
and latency percentiles on the real stack across four sections — DAL, HTTP
API, end-to-end event path, and 1-vs-K-process scaling — and writes
`scripts/benchmark_results.json`. Run it after performance-relevant changes
(batching, pooling, indexes) and compare against the committed baseline; the
scenario list and headline numbers live in [Operations](07-operations.md).
