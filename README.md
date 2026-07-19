# RabbitSpeedTest

RabbitMQ client libraries, the services built on them, and the benchmark
suite that keeps them honest — one repo, four self-contained projects.

| Project | What it is |
|---------|------------|
| [`rabbit-client-python/`](rabbit-client-python/) | **hs-rabbit-client** — minimal Python RabbitMQ publisher/consumer library (aio-pika, robust reconnect, per-message acks). This is the client every Python service should install. |
| [`rabbit-client-typescript/`](rabbit-client-typescript/) | The TypeScript counterpart — same contract on amqplib + amqp-connection-manager. |
| [`shared_data_service/`](shared_data_service/) | Authoritative storage microservice (FastAPI + Postgres) consuming and publishing events through the `hs-rabbit-client` library. |
| [`rabbit-benchmark/`](rabbit-benchmark/) | Benchmark suite comparing pika, aio-pika, a max-throughput hybrid client, and the hs-rabbit-client library — the numbers behind the library's design choices. |

## Using the client from another service

- **Python** — install `hs-rabbit-client` as a local path dependency:
  [`rabbit-client-python/README.md`](rabbit-client-python/README.md) has the
  canonical install instructions, and
  [`rabbit-client-python/docs/api.md`](rabbit-client-python/docs/api.md) is the
  full API reference (no need to read the library source).
- **TypeScript** — see [`rabbit-client-typescript/README.md`](rabbit-client-typescript/README.md).

How the projects fit together, why local path deps, and which client to pick:
[`docs/architecture.md`](docs/architecture.md).

## Benchmark results site

📊 **<https://kafuexe.github.io/RabbitSpeedTest/>** — interactive reports for
every captured run. The site is served from this repo's root (`index.html`,
`clients.html`, `results/`); benchmark runs land in this repo-root `results/`
tree when launched via the `rabbit-benchmark` make targets (which pass
`--output-dir ../results`) — a bare `python -m benchmark.main` writes to a
local, gitignored `rabbit-benchmark/results/` instead.

## Repo layout notes

- Each project is standalone: its own pyproject/package.json, tests, and
  README. Cross-project links are local path dependencies — no registry
  needed. Full picture: [`docs/architecture.md`](docs/architecture.md).
- Publishing both clients to on-prem Artifactory (tag-triggered workflow, required vars/secrets, release steps): [`docs/publishing.md`](docs/publishing.md).
- Design specs live in `docs/superpowers/specs/`.
