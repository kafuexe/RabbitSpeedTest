# rabbit-client

Minimal RabbitMQ client for apps: aio-pika only, zero hand-rolled AMQP logic.
One class (`RabbitClient`) covering publish and consume, with everything
subtle delegated to aio-pika's maintained robust machinery.

## Design guarantees

- **Separate publish/consume connections** — broker flow control on a busy
  publisher can never stall your consumers.
- **Publisher confirms** — the publish channel runs with confirms enabled, so
  `publish()` resolves only once the broker has accepted the message.
- **Pipelined `publish_many`** — confirms are awaited in batches (pipeline
  depth 1000) instead of one round-trip per message.
- **Ack after handler** — each message is acked only *after* your handler
  returns; if the handler raises, that one message is nacked and requeued.
  Per-message acks are inherently safe under concurrency (at-least-once
  delivery).
- **Prefetch concurrency** — deliveries run as concurrent tasks up to
  `prefetch` (default 200), so a handler awaiting a database overlaps with
  others. Prefetch applies per consumer.
- **Robust reconnect** — connections are made with aio-pika's
  `connect_robust`, which re-establishes connections, channels, queues and
  consumers after a broker restart or network blip.
- **Broker-cancel watchdog** — a broker-sent `Basic.Cancel` (e.g. the queue
  was deleted) silently drops an aio-pika consumer. `consume()` polls its
  consumer tag and raises `ConsumerCancelledError` instead of parking dead,
  so callers can re-declare and retry.

Queues are always declared durable (RabbitMQ 4 denies transient
non-exclusive queues); the `durable` flag governs *message* persistence.

## Install

Monorepo consumers (editable install from the repo):

```sh
pip install -e path/to/rabbit-client-python
```

Or as a path dependency with [uv](https://docs.astral.sh/uv/):

```toml
# pyproject.toml of the consuming project
[project]
dependencies = ["rabbit-client"]

[tool.uv.sources]
rabbit-client = { path = "../rabbit-client-python", editable = true }
```

Requires Python >= 3.12.

## Usage

```python
client = RabbitClient("amqp://user:pass@host/")
await client.connect()
await client.publish_many("jobs", [b"payload"] * 1000)

async def handler(body: bytes) -> None:
    await db.insert(body)          # your async work; raise to requeue

await client.consume("jobs", handler)   # runs until the task is cancelled
```

## Development

```sh
uv venv .venv
uv pip install -p .venv/bin/python -e . --group dev
.venv/bin/python -m pytest -q   # broker-integration tests auto-skip without a local broker
```
