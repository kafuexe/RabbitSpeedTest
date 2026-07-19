# Deploying shared_data_service on-prem

All site-specific configuration lives in **two flat env files** in this
directory. Edit them once per site; every runtime reads the same files:

| File | Contents | In git? |
|---|---|---|
| `config.env` | Non-secret settings (queues, ports, tuning, TLS paths) | yes — edit in place |
| `secrets.env` | Credentials-bearing URLs and bootstrap passwords | **no** (gitignored) |

Create the secrets file from its template:

```bash
cp deploy/secrets.env.example deploy/secrets.env
# then edit deploy/secrets.env
```

Precedence, lowest to highest: `config.env` → `secrets.env` → `.env`
(local-dev override) → real environment variables. A key exported in the
shell or set by Kubernetes always wins over any file.

## Key reference

### `secrets.env` (secret)

| Key | Meaning | On-prem example |
|---|---|---|
| `SDS_DATABASE_URL` | PostgreSQL DSN (asyncpg) | `postgresql+asyncpg://svc_sds:PASS@pg.internal:5432/shared_data` |
| `SDS_AMQP_URL` | RabbitMQ URL (user, pass, host, port, vhost) | `amqp://svc_sds:PASS@mq.internal:5672/prod` |
| `POSTGRES_USER/PASSWORD/DB` | Bootstrap creds for the **bundled local** postgres only | local stack only |
| `RABBITMQ_DEFAULT_USER/PASS` | Bootstrap creds for the **bundled local** rabbitmq only | local stack only |

### `config.env` (non-secret)

| Key | Meaning | Default |
|---|---|---|
| `SDS_CONSUME_QUEUES` | JSON list of queues to consume CloudEvents from | `["shared-data.events.in"]` |
| `SDS_PUBLISH_QUEUE` | Queue for CloudEvents published after API commits | `shared-data.events.out` |
| `SDS_PREFETCH` | AMQP prefetch per consumer connection | `500` |
| `SDS_CONSUMER_BATCH_SIZE` | Max events per consumer transaction | `200` |
| `SDS_PERSISTENT_MESSAGES` | Publish messages as persistent | `true` |
| `SDS_EVENT_SOURCE` | CloudEvents `source` URN on published events | `urn:sds:shared-data-service` |
| `SDS_SERVICE_MODE` | `api` \| `consumer` \| `both` | `both` |
| `SDS_API_HOST` | HTTP bind address (containers override to `0.0.0.0`) | `127.0.0.1` |
| `SDS_API_PORT` | HTTP port | `8080` |
| `SDS_LOG_LEVEL` | Root log level | `INFO` |
| `SDS_MAX_PAGE_SIZE` | API pagination cap | `200` |
| `SDS_DB_POOL_SIZE` / `SDS_DB_MAX_OVERFLOW` | SQLAlchemy pool sizing | `10` / `20` |
| `SDS_AMQP_CA_FILE` | CA bundle for TLS to RabbitMQ (empty = plaintext) | *(empty)* |
| `SDS_DB_CA_FILE` | CA bundle for TLS to PostgreSQL (empty = plaintext) | *(empty)* |

## Run: bare Python

```bash
cd shared_data_service
python main.py           # reads deploy/config.env + deploy/secrets.env automatically
```

Use the `localhost` URL variants from `secrets.env.example` when the
database/broker are reached through published ports.

## Run: Docker Compose

Against your in-network PostgreSQL and RabbitMQ (URLs in `secrets.env`
point at them):

```bash
docker compose up -d --build
```

Self-contained local stack (bundled broker + database):

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
curl http://127.0.0.1:8080/ready
```

Compose fails fast if `deploy/secrets.env` is missing — create it first
(see above). The one-shot `migrate` service runs `alembic upgrade head`
before the service starts.

## Run: Kubernetes

1. Build and push the image, then set your registry/tag in
   `deploy/kustomization.yaml` under `images:`:

   ```bash
   # from the repo root — the context MUST be the repo root because the
   # image bundles ../rabbit-client-python
   docker build -f shared_data_service/Dockerfile \
     -t <registry>/shared-data-service:<tag> .
   docker push <registry>/shared-data-service:<tag>
   ```
2. Edit `config.env` / `secrets.env` as for any other runtime.
3. Apply:

```bash
kubectl apply -k shared_data_service/deploy
```

The ConfigMap and Secret are **generated from the same two env files**
(`configMapGenerator` / `secretGenerator`). Generated names are
content-hashed, so changing a file and re-applying rolls the Deployments
automatically.

What gets deployed:

- `sds-api` Deployment (2 replicas) — REST API, readiness `/ready`,
  liveness `/health`, exposed by the `sds-api` Service on port 80.
- `sds-consumer` Deployment (1 replica) — headless consumer. No HTTP
  probes: the process exits if the consumer dies and kubelet restarts it.
  Scale it independently of the API (queue-depth autoscaling via KEDA is
  a natural next step).
- `sds-migrate` Job — Alembic migrations. Jobs are immutable: to re-run
  after a config change, `kubectl -n shared-data delete job sds-migrate`
  and apply again.

PostgreSQL and RabbitMQ are assumed to exist in your network; point the
URLs in `secrets.env` at them (in-cluster services work the same way —
use their cluster DNS names).

## TLS with an internal CA

1. Place your CA bundle (PEM) where the service can read it. In
   Kubernetes, mount it from a ConfigMap or Secret.
2. Set in `config.env`:
   `SDS_AMQP_CA_FILE=/etc/ssl/internal/ca.pem` and/or
   `SDS_DB_CA_FILE=/etc/ssl/internal/ca.pem`.
3. Switch `SDS_AMQP_URL` to `amqps://…:5671/…` in `secrets.env`.

Connections are then verified against your CA. A missing or unreadable
CA path fails at startup by design — never silently at first connect.

## Future work

- GitOps-managed secrets: SOPS with age keys is the pragmatic on-prem
  choice; External Secrets Operator only if you already run a secret
  store (Vault/OpenBao).
- KEDA queue-depth autoscaling for `sds-consumer`.
- Mounted secret *files* instead of env vars (pydantic-settings supports
  `secrets_dir`) for tighter secret exposure.
- Client-certificate (mTLS) auth.
