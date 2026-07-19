# Repo reorganization: standalone RabbitMQ client libraries + consumers

**Date:** 2026-07-19
**Status:** Approved (autonomous session — decisions taken with noted assumptions)

## Goal

Reorganize the repository into separate, self-contained projects so that any
service needing RabbitMQ can depend on a produced client library instead of
vendoring code:

1. **`rabbit-client-py/`** — Python RabbitMQ consumer/publisher library.
2. **`rabbit-client-ts/`** — TypeScript RabbitMQ consumer/publisher library.
3. **`shared_data_service/`** — the existing service, refactored to consume
   the Python library as a real dependency (no more vendored copy).
4. **`rabbit-benchmark/`** — the existing benchmark suite, moved out of the
   repo root into its own project.

The repo root keeps only the GitHub Pages assets (`index.html`,
`clients.html`, `results/`, `.nojekyll`) — moving them would break the
published results site — plus an overview README and this spec.

## Decisions & rationale

### Python library: promote `simple_rabbit.py` as-is

The root `simple_rabbit.py` is the canonical, benchmarked client and is
already what shared_data_service vendors. It becomes an installable package:

- Distribution name **`simple-rabbit`**, import name **`simple_rabbit`** —
  unchanged import path means zero churn in the benchmark suite, the SDS
  adapter, its tests, and its scripts.
- `src/` layout: `rabbit-client-py/src/simple_rabbit/__init__.py` holds the
  client (moved with `git mv` to preserve history).
- Its broker-integration test (`tests/test_simple_rabbit.py`, auto-skips
  without a broker) moves into the library project.

### Dependency wiring: independent projects with path deps (not a uv workspace)

A single uv workspace would relocate shared_data_service's lockfile and
couple all projects' dependency resolution. Instead each project stays
standalone:

- **shared_data_service**: `simple-rabbit` in `[project.dependencies]` with
  `[tool.uv.sources] simple-rabbit = { path = "../rabbit-client-py", editable = true }`.
  The vendored `_vendored_simple_rabbit.py`, the drift test, and the
  try/except import seam in `simple_client.py` are deleted.
- **rabbit-benchmark**: `make install` adds `pip install -e ../rabbit-client-py`.
- Docker: the SDS image build context moves to the repo root
  (`compose build: context: ..`) so the library directory is available;
  the Dockerfile copies `rabbit-client-py/` alongside the app.

### TypeScript library: same contract, same philosophy

`rabbit-client-ts` ports SimpleRabbit's semantics on top of **amqplib +
amqp-connection-manager** (the auto-reconnect equivalent of aio-pika's
`connect_robust` — "zero hand-rolled AMQP logic" carries over):

- Separate publish/consume connections, publisher confirms, pipelined
  `publishMany`, per-message ack after the handler resolves / nack+requeue
  when it rejects, `prefetch` concurrency, durable queues with optional
  persistent messages.
- Unit tests mock the connection manager; no broker needed in CI.

### Benchmark suite: own project, results still published from root

`benchmark/`, `tests/`, `configs/`, `Makefile`, `pyproject.toml`,
`requirements.txt`, and the suite README move into `rabbit-benchmark/`.
`OUTPUT_DIR` defaults to `../results` in the Makefile so new runs keep
feeding the GitHub Pages site at the repo root.

## Error handling / testing

- All existing behavior is preserved by construction (file moves + import
  rewiring; the client code itself is untouched).
- Test gates: SDS suite via `uv run pytest` (must pass; integration tests
  auto-skip without infra), benchmark suite via fresh venv (broker-free),
  TS suite via `npm test`, plus `tsc` build.

## Out of scope

- Publishing either library to a registry (no registry available here).
- Porting the benchmark suite to TypeScript.
- Any behavioral change to SimpleRabbit or the service.
