# RabbitSpeedTest architecture

How the four projects in this repo fit together, why they are wired the way
they are, and what you need to do to adopt the client in a new service.

Repo: `github.com/kafuexe/RabbitSpeedTest`.

## The four projects

| Project | Role |
|---------|------|
| [`rabbit-client-python/`](../rabbit-client-python/) | **The library.** Python package `rabbit-client` (import `rabbit_client`, class `RabbitClient`) — a minimal aio-pika publisher/consumer with robust reconnect and per-message acks. This is what every Python service at the org should install. [README](../rabbit-client-python/README.md) · [API reference](../rabbit-client-python/docs/api.md) |
| [`rabbit-client-typescript/`](../rabbit-client-typescript/) | The TypeScript counterpart, npm package `@kafuexe/rabbit-client` (class `RabbitClient`), built on amqplib + amqp-connection-manager. Same contract and delivery semantics as the Python client — with one deliberate divergence: no `ConsumerCancelledError` ([why](../rabbit-client-typescript/README.md#reconnect-behavior-delegated-and-one-deliberate-divergence)). [README](../rabbit-client-typescript/README.md) |
| [`shared_data_service/`](../shared_data_service/) | A production consumer of the library: FastAPI + Postgres storage service that consumes and publishes RabbitMQ [CloudEvents](https://cloudevents.io/) (a small standard JSON envelope for event metadata) through `rabbit-client`. Also the reference example of wiring the path dependency. [README](../shared_data_service/README.md) |
| [`rabbit-benchmark/`](../rabbit-benchmark/) | The benchmark suite comparing pika, aio-pika, a max-throughput "hybrid" client, and `rabbit-client` itself (benchmarked under the name `simple`). Its numbers justify the library's design choices. [README](../rabbit-benchmark/README.md) |

### Dependency arrows

```
shared_data_service ──(uv path dep, editable)──▶ rabbit-client-python
rabbit-benchmark ─────(pip install -e, via requirements.txt)──▶ rabbit-client-python
rabbit-benchmark ──(writes runs)──▶ results/ ◀──(reads)── index.html / clients.html  (GitHub Pages)
rabbit-client-typescript ── no code dependency; mirrors the Python client's contract
```

- `shared_data_service` declares `rabbit-client` as a dependency resolved via
  a `[tool.uv.sources]` path entry (exact stanza: see the
  [library README](../rabbit-client-python/README.md#install)). Its Docker
  image builds from the repo root so that relative path resolves inside the
  build.
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
that file as the single source of truth. In short:

```toml
# your service's pyproject.toml
[project]
dependencies = ["rabbit-client"]
```

resolved by a `[tool.uv.sources]` path entry pointing at the
`rabbit-client-python/` directory of a checkout — the exact uv stanza and the
pip alternative are in the [README](../rabbit-client-python/README.md#install).

Then read the [API reference](../rabbit-client-python/docs/api.md) for the
full `RabbitClient` surface and its delivery-guarantee semantics — it is
written so you never need to read the library source.

For TypeScript services, the install steps (no registry here either) are in
[`rabbit-client-typescript/README.md`](../rabbit-client-typescript/README.md).

## Benchmark results and the GitHub Pages site

**Site:** <https://kafuexe.github.io/RabbitSpeedTest/>

The site is served straight from this repo's **root** — that is why
`index.html`, `clients.html`, `.nojekyll`, and `results/` live at the top
level instead of inside `rabbit-benchmark/` (moving them would break the
published URLs):

- `results/<timestamp>/` — one directory per captured run, containing
  `results.json`, `results.csv`, `report.html` (interactive Plotly report),
  and optionally `report.pdf`. The `rabbit-benchmark` make targets write here
  (they pass `--output-dir ../results`); a bare `python -m benchmark.main`
  writes to a gitignored `rabbit-benchmark/results/` instead.
- `index.html` — the results viewer. It embeds a checked-in `RUNS` array
  whose entries point at `results/<run>/report.html` and shows them in a
  dropdown.
- `clients.html` — the per-client stats page; it embeds its **own separate
  copy** of the run list (in a different entry shape) and fetches each run's
  `results.json` directly.

**Publishing a run (note for maintainers):** the run lists are
hand-maintained, and there are exactly **two places to edit** — the `RUNS`
array in `index.html` and the differently-shaped `RUNS` array in
`clients.html`; they do not share data. A new run in `results/` does not
appear on the site until both get an entry and the change is pushed (Pages
serves whatever is committed). Example entry for `index.html` (single-line
JSON; `clients` is a display string, `mc` = message count, `nres` = number of
result entries):

```json
{"name": "20260711-200226", "label": "20260711-200226  |  simple, hybrid, 500 msgs", "href": "results/20260711-200226/report.html", "clients": "simple, hybrid", "mc": 500, "nres": 64}
```

`clients.html`'s copy uses `{name, dir, clients, mc}` with `clients` as a
real array and `dir` the URL-encoded run directory — the exact per-file
formats, with an example for each, are in the
[benchmark README](../rabbit-benchmark/README.md#publishing-a-run-to-the-site).

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
