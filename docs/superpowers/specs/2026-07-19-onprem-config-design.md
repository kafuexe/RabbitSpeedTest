# shared_data_service — on-prem configuration & deployment design

**Date:** 2026-07-19
**Status:** Approved; verified against current (2025–2026) deployment
best practices (12-factor env config, uv multi-stage images, Kustomize
env-file generators, split Deployments, migration Job).

## Goal

Make **shared_data_service** deployable in an on-prem network by editing
flat env files only — no code changes per site. The same files drive all
three run paths: bare Python, Docker Compose, and Kubernetes.

## Scope

`shared_data_service/` only. It is an independent project within this
repo; the benchmark suite and everything else are untouched. The service
is FastAPI + PostgreSQL + RabbitMQ CloudEvents consumer, already
configured via pydantic-settings (`SDS_` prefix, `.env` support). All
existing key names are kept.

## Config contract

Two flat env files under `shared_data_service/deploy/`:

- **`deploy/config.env`** — non-secret settings (committed as an editable
  template): queue names, bind host/port, service mode, log level,
  prefetch/pool tuning, event source URN, optional TLS CA file paths.
- **`deploy/secrets.env`** — credentials-bearing values, gitignored; a
  committed `deploy/secrets.env.example` documents them:
  - `SDS_DATABASE_URL` (postgres user/pass/host/port/db)
  - `SDS_AMQP_URL` (amqp(s) user/pass/host/port/vhost)
  - RabbitMQ/Postgres bootstrap credentials used only by the local
    compose stack (`RABBITMQ_DEFAULT_USER/PASS`,
    `POSTGRES_USER/PASSWORD/DB`)

Rationale for the split: credentials must land in a K8s Secret, not a
ConfigMap; a separate file makes that mapping mechanical and keeps
secrets out of git.

### New optional TLS keys

- `SDS_AMQP_CA_FILE`, `SDS_DB_CA_FILE` — path to an internal CA bundle.
  Empty (default) = plaintext, exactly today's behavior. Set = TLS
  (`amqps://` / asyncpg `ssl`) verified against that CA. No client-cert
  auth in scope.

## Code changes (minimal)

1. **app/config/settings.py** — add the two optional TLS fields; extend
   `env_file` handling so the service also reads
   `deploy/config.env` + `deploy/secrets.env` when present (real env vars
   keep precedence, as pydantic-settings already guarantees).
2. Pass the CA settings into `SimpleRabbit` (SSL context) and the
   SQLAlchemy/asyncpg engine (`connect_args={"ssl": ctx}`) when set.
3. No renames, no behavior change when the new keys are unset. The
   vendored `_vendored_simple_rabbit.py` byte-sync with the root copy is
   preserved (drift test stays green).

## Container

**`shared_data_service/Dockerfile`** — multi-stage build with `uv`
(`uv sync --locked` in a builder stage, copy the venv into
`python:3.12-slim`), non-root user, `CMD ["python", "main.py"]`, honors
all `SDS_*` env vars. The same image serves api mode, consumer mode, and
the Alembic migration job.

## Docker Compose (`shared_data_service/docker-compose.yml`)

Services: `rabbitmq` (rabbitmq:4-management), `postgres` (postgres:16),
`migrate` (one-shot `alembic upgrade head`, service image), `sds`
(service image, depends on healthy broker+db and completed migrate).
All consume `deploy/config.env` + `deploy/secrets.env` via `env_file`;
compose-level values (published ports, bootstrap credentials) use
`${VAR}` interpolation. Healthchecks on rabbitmq/postgres gate startup
ordering. The rabbitmq/postgres services exist for self-contained local
runs; on-prem they are replaced by pointing the URLs at in-network
instances.

## Kubernetes (`shared_data_service/deploy/k8s/`, Kustomize)

Base + example overlay. The generators read the same two env files:

- `configMapGenerator` ← `deploy/config.env`
- `secretGenerator` ← `deploy/secrets.env`

Content-hashed generated names give automatic rolling restarts on config
change. Resources:

- **Deployment `sds-api`** — `SDS_SERVICE_MODE=api`, `SDS_API_HOST=0.0.0.0`
  (set at the overlay level; code default stays `127.0.0.1`), readiness
  `/ready`, liveness `/health`, `securityContext` (runAsNonRoot),
  resource requests/limits placeholders, terminationGracePeriodSeconds.
- **Deployment `sds-consumer`** — `SDS_SERVICE_MODE=consumer`, same
  probes. Split from the API so each scales independently.
- **Service** for the API.
- **Job `sds-migrate`** — `alembic upgrade head`, same image. A Job (not
  initContainers) is required here: two Deployments share one database,
  and per-pod initContainers would race on the migration.
- RabbitMQ/PostgreSQL are assumed to be existing in-network services;
  the overlay documents how to point at in-cluster ones instead.

## Docs

`shared_data_service/deploy/README.md`: every key with
meaning/default/example, the secret-vs-config split, TLS how-to with an
internal CA, and step-by-step run instructions for bare Python, compose,
and Kustomize (`kubectl apply -k`).

## Error handling

- Missing env files: the service falls back to current localhost
  defaults (dev experience unchanged); compose fails fast with a clear
  message if `deploy/secrets.env` is absent (documented `cp` step).
- Invalid CA path: fail at startup with a clear error, not at first
  connect.

## Testing

- Unit: TLS settings fields parse; env-file precedence (real env vars
  beat file values); vendored-copy sync test stays green.
- Integration: `docker compose up` smoke — `/ready` returns 200,
  migration job exits 0. `kubectl kustomize` renders without error (CI-
  friendly, no cluster needed).

## Out of scope (documented as future work)

Benchmark suite wiring (separate project), Prometheus metrics,
GitOps-managed secrets (SOPS with age keys is the pragmatic on-prem
choice; External Secrets Operator only if a real secret store like
Vault/OpenBao exists), mounted-secret-files instead of env vars (slightly
tighter exposure in K8s; pydantic-settings supports it natively if wanted
later), client-cert (mTLS) auth, Helm chart.
