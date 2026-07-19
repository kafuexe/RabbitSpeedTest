# RabbitMQ Client Benchmark Suite

> ## 📊 [**View the interactive benchmark results →**](https://kafuexe.github.io/RabbitSpeedTest/)
> Browse every captured run in your browser — no install, no broker. Pick a result from the dropdown.

The measurements behind the `hs-rabbit-client` library's design. The suite drives
five client adapters — **pika** (synchronous, driven through a thread
executor), **aio-pika** (asyncio-native), **hybrid** (the max-throughput async
combo), **simple** (the `hs-rabbit-client` app library itself), and **fake** (an
in-memory stand-in for broker-free runs) — across latency, throughput,
round-trip, and concurrency, then produces an HTML (and optionally PDF) report
with interactive charts. The default client set is `pika,aio-pika`, the two
baselines `hs-rabbit-client` is compared against; pick any subset with
`--clients`.

This suite lives in `rabbit-benchmark/` — run all commands below from this
directory.

## Install

One command installs everything, including the `hs-rabbit-client` library
(benchmarked as the `simple` client), which is pulled in as an editable
install from the sibling `rabbit-client-python/` package:

```bash
pip install -r requirements.txt
```

Python 3.12+ is required (developed against 3.14).

## Start a RabbitMQ broker

Any reachable broker works. The quickest is Docker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Or use a local/remote install. The default connection is
`amqp://guest:guest@localhost:5672/`.

## Run

Quick broker-free sanity run (uses an in-memory fake client — no RabbitMQ needed):

```bash
python -m benchmark.main --clients fake --message-count 100
```

Full run against a live broker (the default `pika,aio-pika` pair):

```bash
python -m benchmark.main
```

Where results land depends on how you launch the run:

- A **bare `python -m benchmark.main`** writes to `results/<timestamp>/`
  relative to your current directory — i.e. `rabbit-benchmark/results/`, which
  is **gitignored** (scratch output, never published).
- The **make targets** pass `--output-dir ../results` (`OUTPUT_DIR ?=
  ../results` in the Makefile), so their runs land in the repo-root
  `results/` tree — the one published via GitHub Pages.

### Make targets

- `make install` — install all dependencies, including the editable
  `hs-rabbit-client` library.
- `make test` — run the test suite (no broker needed).
- `make run-fake` — broker-free sanity run with the in-memory fake client
  (200 messages, 3 iterations).
- `make run-local` — quick run against local RabbitMQ
  (`guest:guest@localhost`) using the trimmed `configs/smoke.json`.
- `make run-local-full` — full-defaults run against local RabbitMQ with
  `pika,aio-pika` (large; takes a while).

All run targets honor `OUTPUT_DIR` (e.g. `make run-fake OUTPUT_DIR=results`
to keep a run local).

## CLI options

| Flag | Meaning | Default |
|------|---------|---------|
| `--config PATH` | JSON config file to load (CLI flags override it) | none |
| `--amqp-url URL` | Broker URL | `amqp://guest:guest@localhost:5672/` |
| `--message-count N` | Messages per throughput/concurrency iteration | `50000` |
| `--iterations N` | Measured iterations per benchmark | `10` |
| `--clients LIST` | Comma-separated client names (`pika`, `aio-pika`, `hybrid`, `simple`, `fake`) — the five adapters described at the top of this README | `pika,aio-pika` |
| `--output-dir DIR` | Root output directory | `results` |
| `--confirms` / `--no-confirms` | Publisher confirms on/off | on |
| `--durable` / `--no-durable` | Persistent messages (`delivery_mode=2`) vs transient. Queues are always durable — RabbitMQ 4 denies transient non-exclusive queues | off (transient) |
| `--no-report` | Skip report generation (write JSON/CSV only) | off |

The URL and management URL can also be set via `RABBITMQ_URL` /
`RABBITMQ_MANAGEMENT_URL` environment variables.

## Output layout

Each run produces one timestamped directory (where it lands depends on how
you launch the run — see [Run](#run)):

```
<output-dir>/<timestamp>/
  results.json     # full suite result: config, environment, per-iteration samples
  results.csv      # flattened per-iteration rows
  report.html      # interactive report (Plotly charts inline)
  report.pdf       # only if WeasyPrint native libs are installed (see below)
```

### Publishing a run to the site

A run sitting in repo-root `results/` does **not** appear on the site by
itself. The run dropdown is hand-maintained, and there are **two separate
copies to edit — `index.html` and `clients.html` at the repo root — in two
different shapes** (they do not share one array). For a run directory
`results/20260711-200226/`:

In `index.html`, append to the single-line `RUNS` JSON array (`clients` is a
display **string**; `mc` = message count, `nres` = number of result entries
in `results.json`; `href` must be URL-encoded if the directory name has
spaces):

```json
{"name": "20260711-200226", "label": "20260711-200226  |  simple, hybrid, 500 msgs", "href": "results/20260711-200226/report.html", "clients": "simple, hybrid", "mc": 500, "nres": 64}
```

In `clients.html`, append to its multi-line `RUNS` array (`clients` is a real
**array** here; `dir` is the URL-encoded run directory, no `label`/`nres`):

```js
{name:"20260711-200226", dir:"results/20260711-200226/", clients:["simple","hybrid"], mc:500},
```

Then push — Pages serves whatever is committed. How the site works, and how
this suite relates to the client libraries it measures, is covered in
[`../docs/architecture.md`](../docs/architecture.md).

## Benchmarks

- **publish_latency** / **consume_latency** — single-operation latency per message size.
- **round_trip** — publish then consume the same message.
- **publish_throughput** / **consume_throughput** — messages/sec for bulk publish / drain.
  The drain uses a push consumer (`basic.consume`); **consume_throughput_get** measures the
  same drain through a `basic.get` polling loop for a push-vs-get comparison.
- **concurrent_publish** / **concurrent_consume** — aggregate msgs/sec and scaling
  efficiency across concurrency levels `[1, 2, 4, 8, 16, 32]`. Each worker gets its own
  connection; a run that drains fewer messages than it published is recorded as a failure.

Each benchmark runs warm-up iterations (discarded) followed by measured iterations, times
with `time.perf_counter_ns()`, records per-iteration failures without aborting, and reports
avg / median / min / max / stddev / p95 / p99 (plus msgs/sec and total duration for
throughput).

## PDF reports

PDF generation is pluggable and **degrades gracefully**: the HTML report is always written.
PDF is produced only when [WeasyPrint](https://weasyprint.org/) and its native libraries are
installed and importable; otherwise the console prints an HTML-only notice and `report.pdf`
is skipped. Static PNG charts (embedded in the PDF) are rendered via kaleido and are only
generated when a PDF is actually going to be produced — the HTML report uses interactive
Plotly divs and never launches a browser.

## Fairness caveats

The two clients use different concurrency models, so a few methodology notes apply when
interpreting results:

- **Threads vs coroutines.** pika is synchronous; every call is dispatched to a single
  dedicated thread so one connection is owned by one thread (pika connections are not
  thread-safe). aio-pika runs natively on the event loop. The thread hand-off adds a small
  fixed overhead to the pika path.
- **Async pipelining.** aio-pika's bulk publish overlaps sends with `asyncio.gather` in
  bounded batches (natural async pipelining); pika publishes sequentially on its thread.
- Queue handles are cached on the aio-pika client so repeated consume/purge calls don't pay
  an extra `queue.declare` round-trip the pika path doesn't — keeping the two measured
  symmetrically.

## Extending

Add a new client by implementing `benchmark.clients.base.BenchmarkClient` and registering it
in `benchmark.runner.build_client`. Reporting depends only on the results schema, so no
report changes are needed.

## Tests

```bash
python -m pytest -q
```

The suite runs entirely without a broker (using the in-memory fake client).

The RabbitClient broker-integration test (formerly hosted in this suite's
`tests/`) now lives in the `rabbit-client-python` library alongside the client
itself.
