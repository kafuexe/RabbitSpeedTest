# Development Setup

This chapter gets you from a fresh checkout to a running service with a
verified write path. Everything below is deterministic — copy, paste, done.

## Prerequisites

| Requirement | Why |
|---|---|
| Python 3.12 | `pyproject.toml` declares `requires-python = ">=3.12"` |
| Docker | Runs PostgreSQL and RabbitMQ locally |

No Node, no frontend toolchain. The service is pure Python.

## 1. Infrastructure

Two containers, exactly as the top-level `README.md` prescribes:

```bash title="PostgreSQL 16 on host port 5434"
docker run -d --name sds-postgres -p 5434:5432 \
  -e POSTGRES_USER=sds -e POSTGRES_PASSWORD=sds -e POSTGRES_DB=shared_data \
  postgres:16-alpine
```

```bash title="RabbitMQ 4 with management UI"
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

!!! note "Why port 5434"
    The Postgres container maps to host port **5434** (not the default 5432)
    so it never collides with a system Postgres. The default
    `SDS_DATABASE_URL` and the integration-test skip probe both expect 5434.

The RabbitMQ management UI is at <http://localhost:15672> with the default
credentials **guest / guest** — useful for watching queues fill and drain.

## 2. Install and migrate

From `shared_data_service/`:

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ../rabbit-client-python -r requirements.txt
.venv/bin/python -m alembic upgrade head
```

The RabbitMQ client is the local `rabbit-client` package in the sibling
`../rabbit-client-python` checkout — install it explicitly (editable) as
above so pip never goes looking for that name on PyPI; `requirements.txt`
pins everything else (runtime + test + docs tooling). The service itself is
never installed as a package — you run it from this directory.

Migrations create the `users` table and the `processed_events` consumer inbox.
`alembic.ini` carries a fallback URL, but `alembic/env.py` resolves the real
one from `Settings`, so `SDS_DATABASE_URL` (or `.env`) governs migrations too.

## 3. Configuration

All settings live in `app/config/settings.py`, loaded from environment
variables with the `SDS_` prefix. A `.env` file in the directory you launch
from (normally `shared_data_service/`) is read automatically
(`env_file=".env"`); unknown variables are ignored.

| Setting (env var) | Default | What it does |
|---|---|---|
| `SDS_DATABASE_URL` | `postgresql+asyncpg://sds:sds@localhost:5434/shared_data` | Async SQLAlchemy connection URL |
| `SDS_AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `SDS_CONSUME_QUEUES` | `["shared-data.events.in"]` | Queues the consumer reads (JSON list in the env var). **Must not be empty** when `service_mode` is `consumer` or `both` — a model validator rejects it at startup, because an empty list would start, consume nothing, and exit 0 looking successful |
| `SDS_PUBLISH_QUEUE` | `shared-data.events.out` | Queue that API-path commits publish CloudEvents to |
| `SDS_PREFETCH` | `500` | Broker prefetch (unacked message window per consumer) |
| `SDS_CONSUMER_BATCH_SIZE` | `200` | Upper bound for the greedy consumer micro-batch (one transaction per batch); the batcher never waits to fill it, so it adds no latency |
| `SDS_PERSISTENT_MESSAGES` | `true` | Publish messages with persistent delivery mode |
| `SDS_EVENT_SOURCE` | `urn:sds:shared-data-service` | CloudEvents `source` attribute on published events |
| `SDS_SERVICE_MODE` | `both` | `api` \| `consumer` \| `both` — see below |
| `SDS_API_HOST` | `127.0.0.1` | uvicorn bind host (loopback — set `0.0.0.0` in containers or the API is unreachable from outside) |
| `SDS_API_PORT` | `8080` | uvicorn bind port |
| `SDS_LOG_LEVEL` | `INFO` | Root log level (JSON structured logging) |
| `SDS_MAX_PAGE_SIZE` | `200` | Ceiling for the `limit` query parameter on list endpoints |
| `SDS_DB_POOL_SIZE` | `10` | SQLAlchemy engine pool size |
| `SDS_DB_MAX_OVERFLOW` | `20` | SQLAlchemy engine pool overflow |

## 4. Service modes

`SDS_SERVICE_MODE` selects what one process runs. `main.py` branches on it:

| Mode | What runs |
|---|---|
| `api` | uvicorn serves the FastAPI app (factory `app.bootstrap.api_app:create_app_from_env`). The lifespan starts the container but **not** the consumer; `/ready` has no `consumer` key |
| `consumer` | No HTTP at all. `main.py` calls `app/bootstrap/consumer_runner.py:main()`, which builds the `Container`, starts it, and awaits the supervised consumer task until cancelled |
| `both` (default) | uvicorn as in `api`, and the app lifespan additionally calls `container.start_consumer()` — API and consumer share one process. Dev-friendly |

## 5. Run it and verify

```bash
.venv/bin/python main.py
```

Then verify, in order:

1. **OpenAPI** — open <http://127.0.0.1:8080/docs>.
2. **Liveness** — `curl http://127.0.0.1:8080/health` returns
   `{"status": "ok"}` (process is up, nothing more).
3. **Readiness** — `curl http://127.0.0.1:8080/ready` returns
   `{"database": true, "rabbitmq": true, "consumer": true}` in `both` mode
   (real dependency checks; deep dive in [Operations](07-operations.md)).
4. **Write path** — create a user, then watch a `user.created` CloudEvent
   land on `shared-data.events.out` in the management UI (Queues →
   `shared-data.events.out` → Get Messages; the queue is declared
   automatically by the service on first publish):

```bash title="Prove the write path"
curl -s -X POST http://127.0.0.1:8080/users \
  -H 'Content-Type: application/json' \
  -d '{"id": "00000000-0000-0000-0000-000000000001",
       "name": "Ada Lovelace", "email": "ada@example.com",
       "attributes": {"role": "engineer"}}'
```

Expect HTTP 201 with `"version": 1`. Re-run the same curl: HTTP 200, same row
— create is a replay-safe idempotent write (see
[Architecture Tour](03-architecture-tour.md)).

## 6. Quick test run

```bash
.venv/bin/python -m pytest
```

The unit suite needs no infrastructure. Integration tests auto-skip when the
backing services are absent: `tests/integration/conftest.py` probes
`localhost:5434` and `localhost:5672` with a 0.5 s `socket.create_connection`
at import time and applies `pytest.mark.skipif` markers (`requires_pg` /
`requires_rabbit`) as each module's `pytestmark`. With both containers up,
everything runs. Full guide: [Testing Your Module](06-testing.md).

!!! warning "Troubleshooting"
    - **Port 5434 or 5672 already in use** — another Postgres/RabbitMQ (or an
      old `sds-postgres` / `rabbitmq` container) holds the port. `docker ps -a`,
      then remove or reuse it. The integration-test probe keys off these exact
      ports.
    - **`relation "users" does not exist`** — you skipped
      `.venv/bin/python -m alembic upgrade head`. Run it after every pull that
      adds a migration.
    - **Wrong Python in the venv** — the venv must be built with `python3.12`.
      Check `.venv/bin/python --version`; if it is older, delete `.venv` and
      recreate it.

Next: [Architecture Tour](03-architecture-tour.md) explains how the pieces
you just started fit together.
