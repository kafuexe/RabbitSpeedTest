# Start Here

The Shared Data Service is the authoritative storage microservice for shared
application data: PostgreSQL is the system of record, and changes arrive over
a REST API and over RabbitMQ CloudEvents (the CNCF-standard JSON event
envelope — `id`, `source`, `type`, `data`). Every successful commit on the API
path publishes a CloudEvent describing the new state, so other services can
mirror the data without ever querying it. It is a Python 3.12 / FastAPI /
SQLAlchemy-async service, built so that idempotency, ordering, and
crash-safety hold by construction rather than by discipline.

## Reading paths by role

| You are | Read | Why |
|---|---|---|
| **New maintainer** | [What & Why](01-what-and-why.md) → [Setup](02-setup.md) → [Architecture Tour](03-architecture-tour.md) → [Reliability Model](04-reliability-model.md) → [Adding a Module](05-adding-a-module.md) → [Testing](06-testing.md); then [Operations](07-operations.md) when you go on call | In order. Each chapter assumes the previous ones. |
| **Engineering manager** | [What & Why](01-what-and-why.md), then the failure-modes table in [Operations](07-operations.md) | What the service guarantees, and what can page your team. |
| **AI agent** | [Architecture Tour](03-architecture-tour.md), [Reliability Model](04-reliability-model.md), and [Adding a Module](05-adding-a-module.md) — in full, before writing code | See the binding rules below. |

!!! warning "Rules for AI agents"
    Read chapters 3, 4, and 5 in full before writing any code. Treat the
    definition-of-done checklist in [Adding a Module](05-adding-a-module.md)
    and the [Maintenance Contract](08-maintenance.md) as binding — check no
    box you have not verified. Never modify another module's internals:
    modules talk through events, not through each other's repositories.

## Five-minute quickstart

Full detail and troubleshooting in [Setup](02-setup.md); this is the happy
path. From `shared_data_service/`:

```bash
# 1. Infrastructure
docker run -d --name sds-postgres -p 5434:5432 \
  -e POSTGRES_USER=sds -e POSTGRES_PASSWORD=sds -e POSTGRES_DB=shared_data \
  postgres:16-alpine
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management

# 2. Install + migrate
python3.12 -m venv .venv && .venv/bin/pip install -e .
.venv/bin/python -m alembic upgrade head

# 3. Run (SDS_SERVICE_MODE = api | consumer | both; default both)
.venv/bin/python main.py
```

Verify:

```bash
curl -s http://127.0.0.1:8080/health
```

Then open http://127.0.0.1:8080/docs for the interactive OpenAPI UI.

!!! note "This site"
    The source of truth is the markdown in `onboarding/`. Browse it as a
    website with `.venv/bin/mkdocs serve` (config: `mkdocs.yml` at the
    service root). AI agents should read the `.md` files directly — the site
    is a rendering, not the artifact.

## Chapter map

| # | Chapter | Read this to… |
|---|---|---|
| 1 | [What & Why](01-what-and-why.md) | understand what the service does and guarantees, in plain language — no code required. |
| 2 | [Setup](02-setup.md) | get a working dev environment: infra containers, install, migrations, service modes, verification. |
| 3 | [Architecture Tour](03-architecture-tour.md) | learn the layers, the dependency rules, the two object graphs, and follow a request and an event end to end. |
| 4 | [Reliability Model](04-reliability-model.md) | see every failure mode and the mechanism that defuses it — plus the one known gap (the Outbox). |
| 5 | [Adding a Module](05-adding-a-module.md) | build a complete second module, file by file, with live verification and a definition-of-done checklist. |
| 6 | [Testing](06-testing.md) | test your module: unit-with-fakes vs integration, the fixture tour, the recipe. |
| 7 | [Operations](07-operations.md) | run it in production: health/ready, log catalog, failure responses, scaling, shutdown. |
| 8 | [Maintenance Contract](08-maintenance.md) | keep this guide true — what to update when you change what. |
