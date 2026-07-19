# RabbitMQ Client Benchmark Suite

> ## 📊 [**View the interactive benchmark results →**](https://kafuexe.github.io/rabbit-platform/)
> Browse every captured run in your browser — no install, no broker. Pick a result from the dropdown.

Benchmarks and compares two Python RabbitMQ clients — **pika** (synchronous, driven
through a thread executor) and **aio-pika** (asyncio-native) — across latency,
throughput, round-trip, and concurrency, then produces an HTML (and optionally PDF)
report with interactive charts.

This suite lives in `rabbit-benchmark/` — run all commands below from this
directory.

## Install

One command installs everything, including the `rabbit-client` library
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

Full run against a live broker (both real clients):

```bash
python -m benchmark.main
```

Results and the report are written to `../results/<timestamp>/` by default (the
repo root, published via GitHub Pages) when using the make targets; see the
output layout below.

## CLI options

| Flag | Meaning | Default |
|------|---------|---------|
| `--config PATH` | JSON config file to load (CLI flags override it) | none |
| `--amqp-url URL` | Broker URL | `amqp://guest:guest@localhost:5672/` |
| `--message-count N` | Messages per throughput/concurrency iteration | `50000` |
| `--iterations N` | Measured iterations per benchmark | `10` |
| `--clients LIST` | Comma-separated client names (`pika`, `aio-pika`, `hybrid`, `simple`, `fake`); `hybrid` is the max-throughput async combo, `simple` benchmarks the `RabbitClient` app client from the `rabbit-client-python` library (`rabbit_client` module) | `pika,aio-pika` |
| `--output-dir DIR` | Root output directory | `results` |
| `--confirms` / `--no-confirms` | Publisher confirms on/off | on |
| `--durable` / `--no-durable` | Persistent messages (`delivery_mode=2`) vs transient. Queues are always durable — RabbitMQ 4 denies transient non-exclusive queues | off (transient) |
| `--no-report` | Skip report generation (write JSON/CSV only) | off |

The URL and management URL can also be set via `RABBITMQ_URL` /
`RABBITMQ_MANAGEMENT_URL` environment variables.

## Output layout

By default results are written to `../results/<timestamp>/` — the repo-root
`results/` directory, published via GitHub Pages (the make targets pass
`--output-dir ../results`; a bare `python -m benchmark.main` writes to a local
`results/` unless you pass `--output-dir`):

```
../results/<timestamp>/
  results.json     # full suite result: config, environment, per-iteration samples
  results.csv      # flattened per-iteration rows
  report.html      # interactive report (Plotly charts inline)
  report.pdf       # only if WeasyPrint native libs are installed (see below)
```

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
