# shared_data_service On-Prem Config & Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make shared_data_service deployable on-prem by editing two flat env files that drive bare Python, Docker Compose, and Kubernetes identically.

**Architecture:** Config contract = `deploy/config.env` (plain) + `deploy/secrets.env` (gitignored, credentials). pydantic-settings reads both; Compose consumes them via `env_file`; Kustomize generates the ConfigMap/Secret from the same files. Optional TLS via CA-file settings that augment the AMQP URL (aio-pika `cafile` query param) and the asyncpg `connect_args` — the shared `SimpleRabbit` client is never modified.

**Tech Stack:** Python 3.12, pydantic-settings v2, SQLAlchemy 2 async/asyncpg, aio-pika, uv (lockfile + multi-stage Docker), Docker Compose (override-file pattern), Kustomize.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-19-onprem-config-design.md`
- Scope is `shared_data_service/` only (plus root `.gitignore` entry and this repo's spec/plan docs). Do NOT touch `benchmark/`, root `simple_rabbit.py`, or `shared_data_service/app/messaging/_vendored_simple_rabbit.py` (a unit test enforces byte-identity with the root copy).
- No new runtime dependencies. Existing `SDS_*` key names unchanged.
- New settings keys: `SDS_AMQP_CA_FILE`, `SDS_DB_CA_FILE` — empty string default = exactly today's behavior.
- Docker uses `uv sync --locked` (requires committing `shared_data_service/uv.lock`).
- All commands below run from `shared_data_service/` unless stated otherwise. Tests: `python -m pytest tests/unit -q` must stay green after every task.
- `deploy/secrets.env` is gitignored; only `deploy/secrets.env.example` is committed.

---

### Task 1: Settings — env-file chain + TLS fields

**Files:**
- Modify: `shared_data_service/app/config/settings.py`
- Test: `shared_data_service/tests/unit/test_settings.py`

**Interfaces:**
- Produces: `Settings.amqp_ca_file: str`, `Settings.db_ca_file: str`, `Settings.effective_amqp_url: str` (property; used by Task 2's container wiring), env-file chain `("deploy/config.env", "deploy/secrets.env", ".env")`.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/test_settings.py`)

```python
from urllib.parse import parse_qsl, urlsplit


def test_missing_ca_file_fails_at_startup(tmp_path):
    with pytest.raises(ValidationError):
        Settings(amqp_ca_file=str(tmp_path / "missing.pem"))
    with pytest.raises(ValidationError):
        Settings(db_ca_file=str(tmp_path / "missing.pem"))


def test_effective_amqp_url_without_ca_is_unchanged():
    s = Settings()
    assert s.effective_amqp_url == s.amqp_url


def test_effective_amqp_url_appends_cafile(tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("dummy")
    s = Settings(
        amqp_url="amqps://u:p@broker.internal:5671/vh?heartbeat=30",
        amqp_ca_file=str(ca),
    )
    parts = urlsplit(s.effective_amqp_url)
    assert parts.scheme == "amqps"
    assert parts.netloc == "u:p@broker.internal:5671"
    assert dict(parse_qsl(parts.query)) == {"heartbeat": "30", "cafile": str(ca)}


def test_deploy_env_files_load_with_dotenv_priority(tmp_path, monkeypatch):
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "config.env").write_text(
        "SDS_LOG_LEVEL=WARNING\nSDS_API_PORT=9000\n"
    )
    (tmp_path / "deploy" / "secrets.env").write_text(
        "SDS_AMQP_URL=amqp://u:p@broker:5672/vh\n"
    )
    (tmp_path / ".env").write_text("SDS_API_PORT=9100\n")
    monkeypatch.chdir(tmp_path)
    s = Settings()
    assert s.log_level == "WARNING"
    assert s.amqp_url == "amqp://u:p@broker:5672/vh"
    # .env is the local-dev override; it wins over the deploy files.
    assert s.api_port == 9100


def test_real_env_vars_beat_env_files(tmp_path, monkeypatch):
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "config.env").write_text("SDS_LOG_LEVEL=WARNING\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SDS_LOG_LEVEL", "ERROR")
    assert Settings().log_level == "ERROR"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_settings.py -v`
Expected: new tests FAIL (`ValidationError` not raised / `AttributeError: effective_amqp_url` / env files not loaded); the two pre-existing tests still pass.

- [ ] **Step 3: Implement in `app/config/settings.py`**

Replace the imports and `model_config`, and add fields + validator + property:

```python
"""Application settings, loaded from environment variables (prefix SDS_)."""
from __future__ import annotations

from pathlib import Path
from typing import Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Later files win; real environment variables beat every file.
    # deploy/*.env is the on-prem contract; .env stays the local-dev override.
    model_config = SettingsConfigDict(
        env_prefix="SDS_",
        env_file=("deploy/config.env", "deploy/secrets.env", ".env"),
        extra="ignore",
    )
```

Keep every existing field unchanged; add after `db_max_overflow`:

```python
    # Optional TLS: path to an internal CA bundle. Empty = plaintext.
    amqp_ca_file: str = ""
    db_ca_file: str = ""
```

Add after the existing `_consumer_modes_need_queues` validator:

```python
    @model_validator(mode="after")
    def _ca_files_must_exist(self) -> "Settings":
        # A typo'd CA path must fail at startup, not at first TLS connect.
        for name in ("amqp_ca_file", "db_ca_file"):
            value = getattr(self, name)
            if value and not Path(value).is_file():
                raise ValueError(f"{name}: CA bundle not found: {value!r}")
        return self

    @property
    def effective_amqp_url(self) -> str:
        """amqp_url with the CA bundle attached as aio-pika's ``cafile``
        URL query parameter — SimpleRabbit itself stays untouched."""
        if not self.amqp_ca_file:
            return self.amqp_url
        parts = urlsplit(self.amqp_url)
        query = dict(parse_qsl(parts.query))
        query["cafile"] = self.amqp_ca_file
        return urlunsplit(parts._replace(query=urlencode(query)))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_settings.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Run the full unit suite**

Run: `python -m pytest tests/unit -q`
Expected: PASS (no regressions).

- [ ] **Step 6: Commit**

```bash
git add shared_data_service/app/config/settings.py shared_data_service/tests/unit/test_settings.py
git commit -m "feat(sds): deploy env-file chain and optional TLS CA settings"
```

---

### Task 2: TLS wiring — engine connect_args and container AMQP URL

**Files:**
- Modify: `shared_data_service/app/database/engine.py`
- Modify: `shared_data_service/app/bootstrap/container.py:40-44`
- Test: `shared_data_service/tests/unit/test_engine_tls.py` (new)

**Interfaces:**
- Consumes: `Settings.db_ca_file`, `Settings.effective_amqp_url` (Task 1).
- Produces: `_ssl_context(ca_file: str) -> ssl.SSLContext | None` in `app.database.engine`; `create_engine` passes `connect_args={"ssl": ctx}` when a CA is set.

- [ ] **Step 1: Write the failing test** (`tests/unit/test_engine_tls.py`)

```python
"""TLS plumbing: empty CA = plaintext exactly as before; bad CA fails fast."""
import ssl

import pytest

from app.database.engine import _ssl_context


def test_empty_ca_file_means_no_ssl_context():
    assert _ssl_context("") is None


def test_invalid_ca_bundle_fails_at_startup(tmp_path):
    bad = tmp_path / "ca.pem"
    bad.write_text("not a certificate")
    with pytest.raises(ssl.SSLError):
        _ssl_context(str(bad))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_engine_tls.py -v`
Expected: FAIL with `ImportError: cannot import name '_ssl_context'`.

- [ ] **Step 3: Implement `app/database/engine.py`**

```python
"""Async engine and session factory construction (wired by bootstrap only)."""
from __future__ import annotations

import ssl

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config.settings import Settings


def _ssl_context(ca_file: str) -> ssl.SSLContext | None:
    # Loading here (at startup) makes an unreadable/garbage CA bundle fail
    # the process immediately instead of at the first connection attempt.
    return ssl.create_default_context(cafile=ca_file) if ca_file else None


def create_engine(settings: Settings) -> AsyncEngine:
    connect_args = {}
    ctx = _ssl_context(settings.db_ca_file)
    if ctx is not None:
        connect_args["ssl"] = ctx
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 4: Wire the AMQP URL in `app/bootstrap/container.py`**

Change the `SimpleClientAdapter` construction (currently `settings.amqp_url`):

```python
        self.bus = SimpleClientAdapter(
            settings.effective_amqp_url,
            prefetch=settings.prefetch,
            persistent=settings.persistent_messages,
        )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/unit -q`
Expected: ALL PASS.

- [ ] **Step 6: Commit**

```bash
git add shared_data_service/app/database/engine.py shared_data_service/app/bootstrap/container.py shared_data_service/tests/unit/test_engine_tls.py
git commit -m "feat(sds): wire optional TLS CA into asyncpg engine and AMQP URL"
```

---

### Task 3: Config contract files + gitignore

**Files:**
- Create: `shared_data_service/deploy/config.env`
- Create: `shared_data_service/deploy/secrets.env.example`
- Modify: `.gitignore` (repo root)

**Interfaces:**
- Produces: the two env files consumed verbatim by Settings (Task 1), Compose (Task 5), and Kustomize generators (Task 6). Key set is final here.

- [ ] **Step 1: Create `deploy/config.env`**

```bash
# shared_data_service — site configuration (non-secret).
# One flat file drives every runtime:
#   bare python : read automatically when running from shared_data_service/
#   compose     : env_file: in docker-compose.yml
#   kubernetes  : configMapGenerator in deploy/kustomization.yaml
# Precedence: real environment variables > .env > secrets.env > this file.

# --- Messaging ---------------------------------------------------------
# JSON list. Every queue this instance consumes CloudEvents from.
SDS_CONSUME_QUEUES=["shared-data.events.in"]
# Queue that committed API writes publish CloudEvents to.
SDS_PUBLISH_QUEUE=shared-data.events.out
SDS_PREFETCH=500
SDS_CONSUMER_BATCH_SIZE=200
SDS_PERSISTENT_MESSAGES=true
# CloudEvents `source` URN stamped on published events.
SDS_EVENT_SOURCE=urn:sds:shared-data-service

# --- Process -----------------------------------------------------------
# api | consumer | both
SDS_SERVICE_MODE=both
SDS_API_HOST=127.0.0.1
SDS_API_PORT=8080
SDS_LOG_LEVEL=INFO

# --- Tuning ------------------------------------------------------------
SDS_MAX_PAGE_SIZE=200
SDS_DB_POOL_SIZE=10
SDS_DB_MAX_OVERFLOW=20

# --- TLS (optional) ----------------------------------------------------
# Path to your internal CA bundle (PEM). Empty = plaintext connections.
# With a CA set, use amqps:// in SDS_AMQP_URL and the service verifies
# both RabbitMQ and PostgreSQL against this CA.
SDS_AMQP_CA_FILE=
SDS_DB_CA_FILE=
```

- [ ] **Step 2: Create `deploy/secrets.env.example`**

```bash
# shared_data_service — credentials (SECRET). Copy to secrets.env and edit:
#   cp deploy/secrets.env.example deploy/secrets.env
# secrets.env is gitignored — never commit it.

# --- Service connection URLs ------------------------------------------
# Values below match the bundled docker-compose local stack.
# On-prem: point these at your in-network PostgreSQL and RabbitMQ.
# Bare-python against the local stack: use localhost:5434 / localhost:5672.
SDS_DATABASE_URL=postgresql+asyncpg://sds:sds@postgres:5432/shared_data
SDS_AMQP_URL=amqp://sds:sds@rabbitmq:5672/
# bare-python variants:
#SDS_DATABASE_URL=postgresql+asyncpg://sds:sds@localhost:5434/shared_data
#SDS_AMQP_URL=amqp://sds:sds@localhost:5672/

# --- Local-stack bootstrap (compose profile "local" only) -------------
# Ignored by the service itself; they initialize the bundled containers.
POSTGRES_USER=sds
POSTGRES_PASSWORD=sds
POSTGRES_DB=shared_data
RABBITMQ_DEFAULT_USER=sds
RABBITMQ_DEFAULT_PASS=sds
```

- [ ] **Step 3: Gitignore the real secrets file** (append to repo-root `.gitignore`)

```
shared_data_service/deploy/secrets.env
```

- [ ] **Step 4: Verify Settings picks the files up**

Run from `shared_data_service/`:
```bash
cp deploy/secrets.env.example deploy/secrets.env
python - <<'EOF'
from app.config.settings import Settings
s = Settings()
assert s.publish_queue == "shared-data.events.out"
assert s.amqp_url.startswith("amqp://sds:sds@"), s.amqp_url
print("config contract OK")
EOF
git check-ignore -q shared_data_service/deploy/secrets.env && echo "ignored OK"
```
Expected: `config contract OK` and `ignored OK`. (Run the `git check-ignore` from repo root.)

- [ ] **Step 5: Run unit tests** (guard against env-file bleed into tests)

Run: `python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add shared_data_service/deploy/config.env shared_data_service/deploy/secrets.env.example .gitignore
git commit -m "feat(sds): on-prem config contract — config.env + secrets.env template"
```

---

### Task 4: uv lockfile + Dockerfile + .dockerignore

**Files:**
- Create: `shared_data_service/uv.lock` (generated)
- Create: `shared_data_service/Dockerfile`
- Create: `shared_data_service/.dockerignore`

**Interfaces:**
- Produces: image tag `shared-data-service` used by Compose (Task 5) and Kustomize (Task 6). Image entrypoint `python main.py`; `alembic` available on PATH for the migrate job.

- [ ] **Step 1: Generate the lockfile**

Run from `shared_data_service/`:
```bash
uv lock
```
(If `uv` is missing: `brew install uv` or `pip install uv`.)
Expected: `uv.lock` created.

- [ ] **Step 2: Create `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

FROM python:3.12-slim
RUN groupadd -r sds && useradd -r -g sds -d /app sds
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini main.py ./
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER sds
EXPOSE 8080
CMD ["python", "main.py"]
```

- [ ] **Step 3: Create `.dockerignore`**

```
.venv
__pycache__
*.pyc
tests
scripts
docs
deploy
.env
```

- [ ] **Step 4: Build and smoke the image**

Run from `shared_data_service/` (start colima/docker first if needed):
```bash
docker build -t shared-data-service .
docker run --rm shared-data-service python -c "import app.config.settings; import alembic; print('image OK')"
```
Expected: build succeeds; prints `image OK`.

- [ ] **Step 5: Commit**

```bash
git add shared_data_service/uv.lock shared_data_service/Dockerfile shared_data_service/.dockerignore
git commit -m "feat(sds): uv-locked multi-stage Dockerfile"
```

---

### Task 5: Docker Compose (base + local override)

**Files:**
- Create: `shared_data_service/docker-compose.yml`
- Create: `shared_data_service/docker-compose.local.yml`

**Interfaces:**
- Consumes: `deploy/config.env`, `deploy/secrets.env` (Task 3), image build (Task 4).
- Produces: `docker compose up` (on-prem, external broker/db) and `docker compose -f docker-compose.yml -f docker-compose.local.yml up` (self-contained local stack).

- [ ] **Step 1: Create `docker-compose.yml`** (on-prem shape: service + migration only; URLs in secrets.env point at in-network PostgreSQL/RabbitMQ)

```yaml
services:
  migrate:
    build: .
    image: shared-data-service
    env_file:
      - deploy/config.env
      - deploy/secrets.env
    command: ["alembic", "upgrade", "head"]
    restart: "no"

  sds:
    build: .
    image: shared-data-service
    env_file:
      - deploy/config.env
      - deploy/secrets.env
    environment:
      SDS_API_HOST: 0.0.0.0   # containers must bind beyond loopback
    ports:
      - "8080:8080"
    depends_on:
      migrate:
        condition: service_completed_successfully
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health')"]
      interval: 10s
      timeout: 3s
      retries: 6
```

- [ ] **Step 2: Create `docker-compose.local.yml`** (adds a self-contained broker + database)

```yaml
services:
  rabbitmq:
    image: rabbitmq:4-management
    env_file:
      - deploy/secrets.env   # RABBITMQ_DEFAULT_USER / RABBITMQ_DEFAULT_PASS
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 5s
      retries: 12

  postgres:
    image: postgres:16
    env_file:
      - deploy/secrets.env   # POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB
    ports:
      - "5434:5432"          # matches the service's historical localhost:5434
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
      interval: 5s
      timeout: 5s
      retries: 12

  migrate:
    depends_on:
      postgres:
        condition: service_healthy

  sds:
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
```

- [ ] **Step 3: Validate the composition**

Run from `shared_data_service/`:
```bash
docker compose config -q
docker compose -f docker-compose.yml -f docker-compose.local.yml config -q
```
Expected: both exit 0 (silent).

- [ ] **Step 4: Full local smoke**

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
sleep 15
curl -fsS http://127.0.0.1:8080/ready
docker compose -f docker-compose.yml -f docker-compose.local.yml down -v
```
Expected: `/ready` returns `{"database":true,"rabbitmq":true,"consumer":true}`-shaped 200 JSON before teardown.

- [ ] **Step 5: Commit**

```bash
git add shared_data_service/docker-compose.yml shared_data_service/docker-compose.local.yml
git commit -m "feat(sds): compose stack fed by deploy env files (base + local override)"
```

---

### Task 6: Kustomize manifests

**Files:**
- Create: `shared_data_service/deploy/kustomization.yaml`
- Create: `shared_data_service/deploy/k8s/namespace.yaml`
- Create: `shared_data_service/deploy/k8s/api-deployment.yaml`
- Create: `shared_data_service/deploy/k8s/consumer-deployment.yaml`
- Create: `shared_data_service/deploy/k8s/api-service.yaml`
- Create: `shared_data_service/deploy/k8s/migrate-job.yaml`

**Interfaces:**
- Consumes: `deploy/config.env` + `deploy/secrets.env` (Task 3) via generators; image `shared-data-service` (Task 4).
- Produces: `kubectl apply -k shared_data_service/deploy` deploys everything; generated names `sds-config`/`sds-secrets` are referenced only via `envFrom` (kustomize rewrites the hashed names).

- [ ] **Step 1: Create `deploy/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: shared-data

resources:
  - k8s/namespace.yaml
  - k8s/api-deployment.yaml
  - k8s/consumer-deployment.yaml
  - k8s/api-service.yaml
  - k8s/migrate-job.yaml

# The SAME files that drive bare-python and compose. Content-hashed names
# mean any config change rolls the Deployments automatically.
configMapGenerator:
  - name: sds-config
    envs: [config.env]
secretGenerator:
  - name: sds-secrets
    envs: [secrets.env]

images:
  - name: shared-data-service
    newName: registry.example.internal/shared-data-service  # ← your registry
    newTag: latest                                          # ← your tag
```

- [ ] **Step 2: Create `deploy/k8s/namespace.yaml`**

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: shared-data
```

- [ ] **Step 3: Create `deploy/k8s/api-deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sds-api
  labels: {app: sds, component: api}
spec:
  replicas: 2
  selector:
    matchLabels: {app: sds, component: api}
  template:
    metadata:
      labels: {app: sds, component: api}
    spec:
      securityContext:
        runAsNonRoot: true
      terminationGracePeriodSeconds: 30
      containers:
        - name: sds
          image: shared-data-service
          envFrom:
            - configMapRef: {name: sds-config}
            - secretRef: {name: sds-secrets}
          env:  # explicit env beats envFrom — per-Deployment role override
            - {name: SDS_SERVICE_MODE, value: "api"}
            - {name: SDS_API_HOST, value: "0.0.0.0"}
          ports:
            - {name: http, containerPort: 8080}  # keep in sync with SDS_API_PORT
          readinessProbe:
            httpGet: {path: /ready, port: http}
            periodSeconds: 5
          livenessProbe:
            httpGet: {path: /health, port: http}
            periodSeconds: 10
          resources:
            requests: {cpu: 100m, memory: 256Mi}
            limits: {memory: 512Mi}
```

- [ ] **Step 4: Create `deploy/k8s/consumer-deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sds-consumer
  labels: {app: sds, component: consumer}
spec:
  replicas: 1
  selector:
    matchLabels: {app: sds, component: consumer}
  template:
    metadata:
      labels: {app: sds, component: consumer}
    spec:
      securityContext:
        runAsNonRoot: true
      terminationGracePeriodSeconds: 30
      containers:
        - name: sds
          image: shared-data-service
          envFrom:
            - configMapRef: {name: sds-config}
            - secretRef: {name: sds-secrets}
          env:
            - {name: SDS_SERVICE_MODE, value: "consumer"}
          # No HTTP probes: consumer mode has no server, and the process
          # exits when the consumer task dies — kubelet restart IS liveness.
          resources:
            requests: {cpu: 100m, memory: 256Mi}
            limits: {memory: 512Mi}
```

- [ ] **Step 5: Create `deploy/k8s/api-service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: sds-api
  labels: {app: sds, component: api}
spec:
  selector: {app: sds, component: api}
  ports:
    - {name: http, port: 80, targetPort: http}
```

- [ ] **Step 6: Create `deploy/k8s/migrate-job.yaml`**

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: sds-migrate
  labels: {app: sds, component: migrate}
spec:
  backoffLimit: 4
  template:
    metadata:
      labels: {app: sds, component: migrate}
    spec:
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
      containers:
        - name: migrate
          image: shared-data-service
          command: ["alembic", "upgrade", "head"]
          envFrom:
            - configMapRef: {name: sds-config}
            - secretRef: {name: sds-secrets}
```

- [ ] **Step 7: Verify the kustomization renders**

Run from `shared_data_service/deploy/` (needs `deploy/secrets.env` from Task 3 Step 4):
```bash
kubectl kustomize . > /dev/null && echo "kustomize OK"
```
If `kubectl` is unavailable, `docker run --rm -v "$PWD":/work -w /work registry.k8s.io/kustomize/kustomize:v5 build . > /dev/null`.
Expected: `kustomize OK` (or clean exit) — generators resolve, envFrom names rewrite.

- [ ] **Step 8: Commit**

```bash
git add shared_data_service/deploy/kustomization.yaml shared_data_service/deploy/k8s
git commit -m "feat(sds): kustomize deployment generated from the deploy env files"
```

---

### Task 7: Documentation

**Files:**
- Create: `shared_data_service/deploy/README.md`
- Modify: `shared_data_service/README.md` (add a Deployment section)
- Modify: `docs/superpowers/specs/2026-07-19-onprem-config-design.md` (consumer-probe correction)

**Interfaces:**
- Consumes: everything above; documents the exact key set from Task 3.

- [ ] **Step 1: Write `deploy/README.md`** covering, in this order (write real prose, not placeholders — the key reference table must list every key from config.env and secrets.env.example with meaning, default, and an on-prem example):
  1. **One-paragraph model:** two files (`config.env` committed, `secrets.env` gitignored, created via `cp secrets.env.example secrets.env`); precedence `env vars > .env > secrets.env > config.env`; the same files drive all three runtimes.
  2. **Key reference table** (all `SDS_*` keys + the compose bootstrap keys, marked "local stack only").
  3. **Run: bare Python** — `cd shared_data_service && python main.py` (files read automatically from `deploy/`).
  4. **Run: Docker Compose** — on-prem: `docker compose up -d --build`; self-contained local: `docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build`. Note that `deploy/secrets.env` must exist or compose fails on the missing env_file.
  5. **Run: Kubernetes** — edit registry/tag in `kustomization.yaml`, `kubectl apply -k deploy/`; re-running migrations needs `kubectl delete job sds-migrate` first (Jobs are immutable); config changes roll pods automatically via hashed generator names.
  6. **TLS with an internal CA** — set `SDS_AMQP_CA_FILE`/`SDS_DB_CA_FILE` to the CA bundle path, switch `SDS_AMQP_URL` to `amqps://…:5671/`; in K8s mount the CA from a ConfigMap/Secret and point the settings at the mount path; invalid/missing paths fail at startup by design.
  7. **Future work** — SOPS/Sealed Secrets for GitOps secrets, ESO if a Vault/OpenBao exists, KEDA queue-depth scaling for `sds-consumer`, mounted secret files instead of env vars.

- [ ] **Step 2: Add to `shared_data_service/README.md`** (after the existing quick-start material) a short **Deployment** section: three-line summary of the env-file contract and a pointer to `deploy/README.md`.

- [ ] **Step 3: Amend the spec** — in `docs/superpowers/specs/2026-07-19-onprem-config-design.md`: (a) replace the `sds-consumer` bullet's "same probes" with: no HTTP probes; consumer mode runs no server and the process exits when the consumer task dies, so kubelet restart is the liveness mechanism; (b) replace "Base + example overlay" with a single `deploy/kustomization.yaml` whose `images:` field is the per-site override point (sites overlay it from their own repos if needed).

- [ ] **Step 4: Commit**

```bash
git add shared_data_service/deploy/README.md shared_data_service/README.md docs/superpowers/specs/2026-07-19-onprem-config-design.md
git commit -m "docs(sds): deploy guide for env-file contract across python/compose/k8s"
```

---

### Task 8: Final verification

- [ ] **Step 1: Full test suite**

Run from `shared_data_service/`: `python -m pytest tests/unit -q`
Expected: PASS, including `test_vendored_client.py` (byte-identity untouched).

- [ ] **Step 2: Scope check**

Run from repo root: `git diff main --stat`
Expected: changes only under `shared_data_service/`, `docs/superpowers/`, and `.gitignore`.

- [ ] **Step 3: Re-validate compose + kustomize** (both commands from Task 5 Step 3 and Task 6 Step 7)

Expected: exit 0.
