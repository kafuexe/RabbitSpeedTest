# rabbit-platform architecture

How the four projects in this repo fit together, why they are wired the way
they are, and what you need to do to adopt the client in a new service.

Repo: `github.com/kafuexe/rabbit-platform` (formerly RabbitSpeedTest).

## The four projects

| Project | Role |
|---------|------|
| [`rabbit-client-python/`](../rabbit-client-python/) | **The library.** Python package `rabbit-client` (import `rabbit_client`, class `RabbitClient`) — a minimal aio-pika publisher/consumer with robust reconnect and per-message acks. This is what every Python service at the org should install. [README](../rabbit-client-python/README.md) · [API reference](../rabbit-client-python/docs/api.md) |
| [`rabbit-client-typescript/`](../rabbit-client-typescript/) | The TypeScript counterpart, npm package `@kafuexe/rabbit-client` (class `RabbitClient`), built on amqplib + amqp-connection-manager. Same contract and delivery semantics as the Python client. [README](../rabbit-client-typescript/README.md) |
| [`shared_data_service/`](../shared_data_service/) | A production consumer of the library: FastAPI + Postgres storage service that consumes and publishes RabbitMQ CloudEvents through `rabbit-client`. Also the reference example of wiring the path dependency. [README](../shared_data_service/README.md) |
| [`rabbit-benchmark/`](../rabbit-benchmark/) | The benchmark suite comparing pika, aio-pika, a max-throughput "hybrid" client, and `rabbit-client` itself (benchmarked under the name `simple`). Its numbers justify the library's design choices. [README](../rabbit-benchmark/README.md) |

### Dependency arrows

```
shared_data_service ──(uv path dep, editable)──▶ rabbit-client-python
rabbit-benchmark ─────(pip install -e, via requirements.txt)──▶ rabbit-client-python
rabbit-benchmark ──(writes runs)──▶ results/ ◀──(reads)── index.html / clients.html  (GitHub Pages)
rabbit-client-typescript ── no code dependency; mirrors the Python client's contract
```

- `shared_data_service` declares `rabbit-client` in `[project.dependencies]`
  and resolves it with `[tool.uv.sources] rabbit-client = { path =
  "../rabbit-client-python", editable = true }`. Its Docker image builds from
  the repo root so that relative path resolves inside the build.
- `rabbit-benchmark`'s `requirements.txt` installs the sibling library
  editable (`make install` runs it).
- The TypeScript client shares no code with the Python one — parity is by
  contract (documented in its README's semantics table), not by imports.

## Why local path dependencies (and not a workspace or registry)

Two constraints, decided in the
[repo-reorg design spec](superpowers/specs/2026-07-19-repo-reorg-rabbit-clients-design.md):

1. **No package registry is available here**, so the libraries cannot be
   `pip install rabbit-client`'d from an index. Consumers inside the repo use
   local path dependencies; consumers outside it install from a checkout.
2. **Deliberately not a uv workspace.** A single workspace would relocate
   shared_data_service's lockfile and couple every project's dependency
   resolution. Instead each project is fully standalone — its own
   pyproject/package.json, lockfile, tests, and README — and cross-project
   links are plain path deps.

## Two clients, one contract — which do I pick?

Pick by the language of your service; there is no other tradeoff:

- **Python service** → `rabbit-client-python`. The canonical, benchmarked
  implementation.
- **Node/TypeScript service** → `rabbit-client-typescript`. Same guarantees
  (separate publish/consume connections, publisher confirms, ack-after-handler,
  at-least-once, always-durable queues); the one deliberate divergence
  (no `ConsumerCancelledError` — the JS stack auto-recovers from broker-side
  consumer cancels) is explained in
  [its README](../rabbit-client-typescript/README.md#reconnect-behavior-delegated-and-one-deliberate-divergence).

The benchmark suite's `pika`, `aio-pika`, and `hybrid` clients are **not** for
application use — they exist to measure the design space. `hybrid` is the
~2x-faster, higher-maintenance frontier consumer; `rabbit-client` is the
maintenance-free client services should use.

## Adopting the client in a new service

The canonical, always-current install instructions live in
[`rabbit-client-python/README.md`](../rabbit-client-python/README.md) — treat
that file as the single source of truth. In short, with a checkout of this
repo available at a relative path:

```toml
# your service's pyproject.toml
[project]
dependencies = ["rabbit-client"]

[tool.uv.sources]
rabbit-client = { path = "../rabbit-platform/rabbit-client-python", editable = true }
```

or with pip: `pip install -e path/to/rabbit-platform/rabbit-client-python`.

Then read the [API reference](../rabbit-client-python/docs/api.md) for the
full `RabbitClient` surface and its delivery-guarantee semantics — it is
written so you never need to read the library source.

For TypeScript services, see
[`rabbit-client-typescript/README.md`](../rabbit-client-typescript/README.md)
— no registry here either: build the package in a checkout (`npm run build`),
then `npm install path/to/rabbit-platform/rabbit-client-typescript`.

## Benchmark results and the GitHub Pages site

**Site:** <https://kafuexe.github.io/rabbit-platform/>

The site is served straight from this repo's **root** — that is why
`index.html`, `clients.html`, `.nojekyll`, and `results/` live at the top
level instead of inside `rabbit-benchmark/` (moving them would break the
published URLs):

- `results/<timestamp>/` — one directory per captured run, containing
  `results.json`, `results.csv`, `report.html` (interactive Plotly report),
  and optionally `report.pdf`. Benchmark runs write here by default: the
  `rabbit-benchmark` Makefile passes `--output-dir ../results`.
- `index.html` — the results viewer. It embeds a checked-in `RUNS` array
  whose entries point at `results/<run>/report.html` and shows them in a
  dropdown.
- `clients.html` — the per-client stats page; it filters the same `RUNS`
  array and fetches each run's `results.json` directly.

**Note for maintainers:** the `RUNS` arrays are hand-maintained. A new run
written into `results/` does **not** appear on the site until an entry is
added to the arrays in `index.html` and `clients.html` (and the change is
pushed — Pages serves whatever is committed).

## Document map

- Root [`README.md`](../README.md) — one-screen overview and links.
- This file — cross-project architecture.
- [`rabbit-client-python/README.md`](../rabbit-client-python/README.md) —
  canonical install + design guarantees;
  [`rabbit-client-python/docs/api.md`](../rabbit-client-python/docs/api.md) —
  full API reference.
- [`rabbit-client-typescript/README.md`](../rabbit-client-typescript/README.md)
  — TS install, usage, semantics table, reconnect divergence.
- [`rabbit-benchmark/README.md`](../rabbit-benchmark/README.md) — running the
  suite, CLI flags, output layout, fairness caveats.
- [`shared_data_service/README.md`](../shared_data_service/README.md) and
  `shared_data_service/docs/` — the service's own architecture docs.
- [`docs/superpowers/specs/`](superpowers/specs/) — design specs, including
  the repo reorganization decision record.
