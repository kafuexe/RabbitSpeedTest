# RabbitMQ Client Benchmark Suite — Design

**Date:** 2026-07-10
**Status:** Approved (pending spec review)

## Purpose

A Python benchmarking suite that produces a fair, repeatable, statistically
meaningful comparison between the synchronous **pika** and asynchronous
**aio-pika** RabbitMQ clients. It runs a fixed set of workloads against a
RabbitMQ broker, saves raw and summarized results, and generates a polished
HTML report plus a PDF derived from it.

The suite must be **extensible**: adding another RabbitMQ client should require
only a new client implementation, and the reporting layer must render any
results conforming to the shared schema — with no coupling to how those results
were produced.

## Decisions (from brainstorming)

- **Broker:** local RabbitMQ at `amqp://guest:guest@localhost:5672/` by default;
  user runs the instance. Connection URL is configurable via env/JSON/CLI.
- **PDF:** pluggable backend with graceful fallback. HTML is always produced.
  PDF is attempted via WeasyPrint; if native GTK/Pango libs are missing, the
  tool warns clearly and continues (HTML-only) rather than crashing.
- **Async fairness:** the *workload* is identical across clients (same message
  counts, sizes, order). aio-pika is allowed to overlap operations where its
  model naturally would — most visibly in throughput and concurrent benchmarks.
  Single-op latency is measured one operation at a time. This relaxation of the
  "identical work" rule is stated explicitly in the report so results are not
  misread.
- **Concurrency model:** pika (blocking) uses threads for concurrent
  publishers/consumers; aio-pika uses coroutines on one event loop. This is the
  honest comparison of each library's idiomatic concurrency model and is
  labeled as such in the report.
- **Publisher confirms:** ON by default (timing the actual broker ack, not just
  the socket write), exposed as a config flag.
- **Throughput volume:** default `message_count = 50_000` for throughput and
  concurrent benchmarks, so even a 32-worker run does sustained, measurable work
  and the broker's real ceiling (expected 10k–50k+ msgs/sec) is what stands out.
- **Concurrency levels:** `[1, 2, 4, 8, 16, 32]`, config-driven (extendable to
  64/128 to find the saturation knee).
- **Scope:** full working implementation.

## Architecture

Six layers with strictly one-directional dependencies:

```
main.py / runner.py        orchestration (CLI, config load, run benchmarks, report)
      │
      ├── config.py        dataclass config; env/JSON/CLI merge
      ├── clients/         BenchmarkClient interface + pika / aio-pika impls
      ├── benchmarks/      one module per benchmark; depend ONLY on client interface
      ├── statistics.py    pure functions: stats from raw ns samples
      ├── results.py       dataclasses: results schema + JSON/CSV persistence
      └── reporting/       report_generator + charts; consume results schema ONLY
```

**Core contracts:**

- Benchmarks depend only on the abstract `BenchmarkClient` interface — never on
  concrete clients. Adding a client = new implementation, no benchmark changes.
- Reporting depends only on the results schema — never on clients or benchmark
  internals. It can re-render any past run reloaded from JSON.

**Uniform sync/async interface:** `BenchmarkClient` is an async protocol. The
pika (sync) client wraps its blocking calls in a dedicated thread executor
(`asyncio.to_thread`) so the runner drives both clients through one async
orchestration path with no branching.

## Client interface (`clients/base.py`)

`BenchmarkClient` (abstract/protocol), async methods:

- `connect()` / `close()`
- `declare_queue(name)` / `purge_queue(name)` / `delete_queue(name)`
- `publish(exchange, routing_key, body, *, confirm)` → publish latency point
- `consume_one(queue)` → one message (for latency/round-trip)
- `publish_many(exchange, routing_key, bodies, *, confirm)` → throughput
- `consume_many(queue, count)` → throughput drain
- `server_version()` → RabbitMQ version string or None
- `name: str` (e.g. "pika", "aio-pika")

Implementations: `PikaClient` (blocking pika via thread executor),
`AioPikaClient` (native async). Both set publisher confirms and prefetch
identically per config.

## Data model (`results.py`)

- **`IterationSample`** — one raw measurement: `client`, `benchmark`,
  `iteration`, `value_ns`, `success: bool`, `error: str | None`, `params: dict`
  (msg_size, concurrency, etc.).
- **`StatSummary`** — `avg, median, min, max, stddev, p95, p99` (ns);
  `messages_per_sec`, `total_duration_ns` where relevant; `n_success`,
  `n_failed`.
- **`BenchmarkResult`** — one (client × benchmark × parameter-set): config echo,
  `StatSummary`, and full list of raw `IterationSample`s.
- **`BenchmarkSuiteResult`** — top level: environment info, config, timestamp,
  all `BenchmarkResult`s. Single object that serializes to JSON and flattens to
  CSV, and the single object reporting reads.

**Persistence** (in `results/<timestamp>/`):

- **JSON** — full nested structure incl. every raw sample.
- **CSV** — flat long-format, one row per `IterationSample`, for pandas/Excel.

**Environment info:** Python version, OS/platform, CPU model + core count
(`platform` / `os.cpu_count`), total memory (`psutil` if available, else
best-effort), RabbitMQ version (management HTTP API or AMQP server properties;
null if unreachable).

## Statistics (`statistics.py`)

Pure functions over a list of `value_ns` samples returning a `StatSummary`:
avg, median, min, max, stddev, p95, p99. For throughput benchmarks also
messages/sec and total duration. Implemented with numpy/pandas. No I/O, no
client knowledge — fully unit-testable.

## Benchmark harness & methodology

Shared harness (`runner.py`) wraps every benchmark:

- **Warm-up** iterations (default 5), discarded.
- **Measured** iterations (default 10), each timed with `perf_counter_ns()`.
- Per-iteration exception handling: a failure records `success=False` with the
  error and does **not** abort the run (failed iterations tracked separately).
- Payloads for all sizes (256 B / 1 KB / 10 KB / 100 KB) **pre-generated once**
  as `bytes` and reused — allocation is never timed.
- Queues declared/purged fresh per benchmark for isolation.

Each benchmark module exposes one function `run(client, config) ->
list[BenchmarkResult]`.

| Benchmark | Timed unit | Notes |
|---|---|---|
| `publish_latency` | single publish (confirmed) | full 7-stat block |
| `consume_latency` | single consume | queue pre-loaded before measuring |
| `publish_throughput` | N msgs, wall time | async may pipeline; msgs/sec + avg latency |
| `consume_throughput` | drain N msgs | prefetch tuned; msgs/sec + avg latency |
| `round_trip` | publish→consume same msg | correlation-id match; full 7-stat block |
| `concurrent_publish` | 1/2/4/8/16/32 workers | pika=threads, aio-pika=tasks; aggregate msgs/sec, total duration, avg latency, scaling efficiency |
| `concurrent_consume` | 1/2/4/8/16/32 workers | same model |

**Scaling efficiency** = `throughput_n / (n × throughput_1)` (1.0 = perfect
linear scaling).

## Configuration (`config.py`)

`BenchmarkConfig` dataclass, merged from defaults → JSON file → env → CLI:

- `amqp_url` (default `amqp://guest:guest@localhost:5672/`)
- `queue_name`, `exchange`, `routing_key`
- `message_count` (default 50_000 for throughput/concurrent)
- `message_sizes` (256 B, 1 KB, 10 KB, 100 KB)
- `iterations` (default 10), `warmup_iterations` (default 5)
- `concurrency_levels` (default `[1,2,4,8,16,32]`)
- `publisher_confirms` (default True), `prefetch`
- `clients` to run (default both)
- output directory

## Reporting (`reporting/`)

Fully decoupled — reads a `BenchmarkSuiteResult` (in-memory or reloaded JSON)
and nothing else.

**`charts.py`** builds Plotly figures, each emitted twice:

- Interactive HTML (plotly.js inlined → self-contained report).
- Static PNG (kaleido) for the PDF (which cannot run JS).

Charts:

1. Latency comparison — grouped bar (publish / consume / round-trip).
2. Throughput comparison — horizontal bar (publish / consume).
3. Concurrent publishers — line, msgs/sec vs workers, one line per client.
4. Concurrent consumers — line, msgs/sec vs workers, one line per client.
5. Scaling efficiency — line vs concurrency.
6. Distribution — box plots (median, quartiles, outliers) from raw samples.

**`report_generator.py`** renders `templates/report.html.j2` with:

1. Title page
2. Benchmark configuration
3. Environment information
4. Executive summary (auto-derived: which client won each category, by how much)
5. Benchmark results
6. Interactive charts (HTML)
7. Static charts (PDF)
8. Comparison tables
9. Overall conclusions
10. Appendix: raw statistics

**PDF:** pluggable `PdfBackend`. `WeasyPrintBackend` used if native libs load;
otherwise a clear warning and HTML-only output. PDF uses the static PNGs.

## Project structure

```
benchmark/
├── clients/
│   ├── base.py
│   ├── pika_client.py
│   └── aio_pika_client.py
├── benchmarks/
│   ├── publish_latency.py
│   ├── consume_latency.py
│   ├── publish_throughput.py
│   ├── consume_throughput.py
│   ├── round_trip.py
│   ├── concurrent_publish.py
│   └── concurrent_consume.py
├── reporting/
│   ├── report_generator.py
│   ├── charts.py
│   ├── templates/report.html.j2
│   └── assets/
├── statistics.py
├── config.py
├── runner.py
├── results.py
└── main.py
```

## Testing strategy

- **Unit tests** (no broker): `statistics.py` against known inputs; results
  serialization round-trip (JSON/CSV); config merge precedence; report
  generation from a synthetic `BenchmarkSuiteResult` (HTML renders, PDF backend
  fallback path). A fake/in-memory `BenchmarkClient` exercises benchmark
  harness logic (warm-up discard, failure recording) without a real broker.
- **Live smoke test** (broker required): a small end-to-end run
  (`message_count` small, 1–2 iterations) verifying both real clients connect,
  publish, consume, and produce a report.

## Tech stack

Python 3.12+ (3.14 present here), pika, aio-pika, pandas/numpy, plotly + kaleido,
jinja2, weasyprint (optional at runtime), psutil (optional), `perf_counter_ns`,
dataclasses, full type hints.

## Non-goals / YAGNI

- No persistent time-series DB or historical trend tracking.
- No distributed/multi-host load generation.
- No live web dashboard — static HTML/PDF only.
- No auto-provisioning of RabbitMQ (user supplies the broker).
