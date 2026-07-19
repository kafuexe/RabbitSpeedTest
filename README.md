# rabbit-platform

RabbitMQ client libraries, the services built on them, and the benchmark
suite that keeps them honest — one repo, four self-contained projects.

| Project | What it is |
|---------|------------|
| [`rabbit-client-python/`](rabbit-client-python/) | **simple-rabbit** — minimal Python RabbitMQ publisher/consumer library (aio-pika, robust reconnect, per-message acks). This is the client every Python service should install. |
| [`rabbit-client-typescript/`](rabbit-client-typescript/) | The TypeScript counterpart — same contract on amqplib + amqp-connection-manager. |
| [`shared_data_service/`](shared_data_service/) | Authoritative storage microservice (FastAPI + Postgres) consuming and publishing events through the `simple-rabbit` library. |
| [`rabbit-benchmark/`](rabbit-benchmark/) | Benchmark suite comparing pika, aio-pika, and the simple-rabbit client — the numbers behind the library's design choices. |

## Using the client from another service

Python (uv):

```toml
[project]
dependencies = ["simple-rabbit"]

[tool.uv.sources]
simple-rabbit = { path = "../rabbit-client-python", editable = true }
```

Python (pip): `pip install -e path/to/rabbit-client-python`

TypeScript: see [`rabbit-client-typescript/README.md`](rabbit-client-typescript/README.md).

## Benchmark results site

📊 **<https://kafuexe.github.io/rabbit-platform/>** — interactive reports for
every captured run. The site is served from this repo's root (`index.html`,
`clients.html`, `results/`); benchmark runs write new results into
`results/` by default.

## Repo layout notes

- Each project is standalone: its own pyproject/package.json, tests, and
  README. Cross-project links are local path dependencies — no registry
  needed.
- Design specs live in `docs/superpowers/specs/`.
