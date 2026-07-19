# Shared Data Service

Authoritative storage microservice for shared application data.
PostgreSQL is the system of record; changes arrive over a REST API and over
RabbitMQ CloudEvents, and every successful commit on the API path publishes a
CloudEvent describing the new state.

Architecture spec: [`../shared_data_service_docs/`](../shared_data_service_docs/) ·
Design notes: [`docs/architecture.md`](docs/architecture.md)

## Stack

Python 3.12 · FastAPI · SQLAlchemy 2 async · Alembic · Pydantic v2 ·
PostgreSQL · RabbitMQ via the **simple-rabbit** client
([`../rabbit-client-python`](../rabbit-client-python)) · CloudEvents 1.0.

## Layout

```
app/
  api/          global FastAPI setup (middleware, error mapping, health)
  bootstrap/    composition root — the only place classes are wired together
  config/       env-driven settings (SDS_*)
  database/     engine, UnitOfWork, inbox (consumer idempotency)
  logging/      JSON structured logging + correlation ids
  messaging/    SimpleClient wrapper, CloudEvents, publisher, consumer, registry
  modules/
    shared/     generic module machinery: repository/service bases, event
                plumbing, pagination / filtering / sorting, domain errors
    user/       the user-specific rest: model, whitelists, data shapes,
                validation hooks, schemas, event contract, router
    project/    second module, built entirely from the shared machinery —
                the living proof of the onboarding "Adding a Module" chapter
alembic/        migrations
scripts/        benchmark suite
tests/          unit (fakes) + integration (real PG + RabbitMQ)
```

Each module owns its complete implementation; infrastructure stays central.
Dependency rules: API → Business → Repository → Database; Consumer → Business;
services never touch another module's repository.

## Run

```bash
# infrastructure
docker run -d --name sds-postgres -p 5434:5432 \
  -e POSTGRES_USER=sds -e POSTGRES_PASSWORD=sds -e POSTGRES_DB=shared_data \
  postgres:16-alpine
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management

python3.12 -m venv .venv && .venv/bin/pip install -e .
.venv/bin/python -m alembic upgrade head

# service (SDS_SERVICE_MODE = api | consumer | both)
.venv/bin/python main.py
```

OpenAPI: http://127.0.0.1:8080/docs · liveness `/health` · readiness `/ready`

## Deployment

All environment-specific settings live in two flat env files —
`deploy/config.env` (committed) and `deploy/secrets.env` (gitignored,
`cp deploy/secrets.env.example deploy/secrets.env`). The same files drive
bare Python, `docker compose up`, and `kubectl apply -k deploy/`
(Kustomize generates the ConfigMap/Secret from them). See
[deploy/README.md](deploy/README.md).

The RabbitMQ client is the `simple-rabbit` package from
[`../rabbit-client-python`](../rabbit-client-python), wired as a uv path
dependency (`[tool.uv.sources]` in `pyproject.toml`). The Docker image
builds from the repo root so that path resolves inside the build.

## Tests & benchmarks

```bash
.venv/bin/python -m pytest            # unit + integration (integration
                                      # auto-skips without PG/RabbitMQ)
.venv/bin/python scripts/benchmark.py # measured throughput/latency report
```

## Guarantees

- **Idempotent writes** — create replay returns the stored row; consumer
  dedup via the `processed_events` inbox (same transaction as the write).
- **Ordering-safe** — events carry full state + version; stale or
  out-of-order events are dropped, update-before-create upserts.
- **Publish after commit only**; the consumer path never republishes
  (wired with a null publisher).
- **Concurrency-safe updates** — row locks + optimistic `expected_version`.
- **Horizontally scalable** — all coordination state lives in PostgreSQL;
  run any number of API/consumer instances (see `scripts/benchmark.py`
  scaling section).
- **Fast consumption without weaker guarantees** — greedy micro-batching:
  one transaction per batch, acks strictly after commit, poison items
  isolated, zero added latency when idle (docs/architecture.md).
- **Outbox-ready** — events are staged in the UnitOfWork and delivered
  through the EventPublisher port after commit; swapping in a transactional
  outbox touches bootstrap only.
