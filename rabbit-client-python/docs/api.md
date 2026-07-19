# rabbit-client API reference

Package `rabbit-client`, module `rabbit_client`. Two public names:

```python
from rabbit_client import RabbitClient, ConsumerCancelledError
```

Install instructions: [README](../README.md). Requires Python >= 3.12.

Everything below matches the implementation in
[`src/rabbit_client/__init__.py`](../src/rabbit_client/__init__.py); the
behaviors are pinned by the unit tests (`tests/test_unit.py`, broker-free)
and the integration tests (`tests/test_rabbit_client.py`, auto-skip without
a local broker).

## Quick example

```python
import asyncio
from rabbit_client import RabbitClient, ConsumerCancelledError

client = RabbitClient("amqp://user:pass@host/", durable=True)
await client.connect()

# Publish — resolves only after the broker confirms acceptance.
await client.publish("jobs", b"one payload")
await client.publish_many("jobs", [b"payload"] * 5000)

async def handler(body: bytes) -> None:
    await db.insert(body)      # your async work; raise to requeue this message

# Consume — runs until the surrounding task is cancelled.
task = asyncio.create_task(client.consume("jobs", handler))
...
task.cancel()                  # stop consuming
await client.close()
```

---

## class `RabbitClient`

```python
RabbitClient(
    amqp_url: str,
    *,
    prefetch: int = 200,
    durable: bool = False,
    cancel_check_interval: float = 5.0,
)
```

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `amqp_url` | — | Broker URL, e.g. `amqp://user:pass@host:5672/`. |
| `prefetch` | `200` | Per-consumer prefetch (`basic.qos`). Also the concurrency ceiling: deliveries run as concurrent tasks, so up to `prefetch` handlers overlap. With many busy queues on one client, remember it applies **per consumer** — size it accordingly (e.g. `prefetch=50`). |
| `durable` | `False` | Governs **message persistence** (`delivery_mode` PERSISTENT vs NOT_PERSISTENT) for everything this client publishes. It does **not** govern queue durability — queues are *always* declared durable, because RabbitMQ 4 denies transient non-exclusive queues. See [Delivery guarantees](#delivery-guarantees). |
| `cancel_check_interval` | `5.0` | Seconds between watchdog checks for broker-side consumer cancellation. A silent broker cancel is turned into `ConsumerCancelledError` within at most ~2x this interval. See [`consume`](#consume). |

Constructing the client opens nothing; call [`connect()`](#connect) first.
One client holds **two** broker connections — one for publishing, one for
consuming — so broker flow control on a busy publisher can never stall your
consumers.

### `connect()`

```python
await client.connect() -> None
```

Opens the two connections concurrently (both via aio-pika's
`connect_robust`), then a publish channel **with publisher confirms** and a
consume channel with `prefetch` applied. Also resets the internal
queue-declare caches.

**Partial failure is cleaned up.** If one connection succeeds and the other
fails (connection limit, broker mid-restart), the survivor is explicitly
closed — a robust connection left behind would keep its reconnect machinery
alive with no way to reach it — and the first error is re-raised. An error
raised while closing the survivor never masks the real connect failure.

**A failed `connect()` leaves clean retry state.** No connection attributes
are assigned unless *both* connections succeeded, so after a failure the
client holds nothing half-open, `is_connected` is `False`, and you can simply
call `connect()` again:

```python
while True:
    try:
        await client.connect()
        break
    except Exception:
        await asyncio.sleep(2)
```

### `close()`

```python
await client.close() -> None
```

Closes both connections (skipping any that are absent or already closed).
Safe to call more than once.

### `is_connected`

```python
client.is_connected -> bool   # property, not a coroutine
```

`True` only when **both** connections are live *right now*. This deliberately
checks more than `is_closed`: a robust connection sitting in its reconnect
loop after a broker outage is not closed, but not usable either — during the
outage `is_connected` reports `False`, and flips back to `True` once the
reconnect completes. Before `connect()` and after `close()` it is `False`.

Use it for health/readiness endpoints; do not gate every publish on it
(reconnects are handled for you — see [Reconnect behavior](#reconnect-behavior)).

### `publish()`

```python
await client.publish(queue: str, body: bytes) -> None
```

Publishes one message to `queue` via the default exchange (the queue name is
the routing key). The first publish to a queue declares it (durable); the
declare is cached, so subsequent publishes skip the round-trip. Because the
publish channel runs with confirms, `publish()` resolves only once the broker
has **accepted** the message.

Message persistence follows the constructor's `durable` flag.

### `publish_many()`

```python
await client.publish_many(queue: str, bodies: list[bytes]) -> None
```

Bulk publish with **pipelined confirms**: messages are fired in batches of
1000 and each batch's confirms are awaited together, instead of one
round-trip per message (~9k msg/s for 1KB messages on the benchmark setup).
Same declare caching, confirm, and persistence semantics as `publish()`.

### `consume()`

```python
await client.consume(
    queue: str,
    handler: Callable[[bytes], Awaitable[None]],
) -> None            # never returns normally — runs until cancelled or raises
```

Declares `queue` (durable, cached) and consumes from it, calling
`await handler(message_body)` for each delivery. Handlers run as concurrent
tasks, up to `prefetch` in flight — a handler awaiting a database overlaps
with others, which is what decides real throughput for a DB-bound consumer.

Per message:

1. `handler(body)` is awaited.
2. If it returns normally, the message is **acked** — exactly once, strictly
   after the handler finishes.
3. If it raises any `Exception`, that one message is **nacked with
   `requeue=True`** (it will be redelivered — to this consumer or another)
   and is never acked. The consume loop itself keeps running; a poison
   message that always raises will redeliver forever, so add your own
   retry-limit/dead-letter logic in the handler if you need one.

`consume()` runs until one of:

- **You cancel the task** (`task.cancel()`) — the consumer is cancelled at
  the broker, and `asyncio.CancelledError` propagates.
- **The broker cancels the consumer** — e.g. the queue was deleted. aio-pika
  swallows a broker-sent `Basic.Cancel` silently (nothing is raised into the
  consume task, and consumers are only re-established on *reconnect*), so a
  watchdog inside `consume()` polls the consumer tag every
  `cancel_check_interval` seconds and raises
  [`ConsumerCancelledError`](#consumercancellederror) after two consecutive
  misses — i.e. within at most ~2x `cancel_check_interval` (~10 s at the
  default). The internal queue cache is dropped first, so a retry re-declares
  the queue:

  ```python
  while True:
      try:
          await client.consume("jobs", handler)
      except ConsumerCancelledError:
          continue      # queue re-declared and consumption resumed
  ```

  The watchdog is reconnect-aware: while the connection is down or the
  channel is being restored it resets its miss counter, so a slow reconnect
  is never mistaken for a cancel (it tracks the identity of the underlying
  channel object to tell the two apart).

Constraints and details:

- **One `consume()` per queue per client.** Consumers are cheap — they are
  multiplexed on the single consume connection, no extra threads — so consume
  many queues by calling `consume()` once for each, in separate tasks.
- **Acks use the raw aiormq channel deliberately.** Acks and nacks are issued
  through `message.channel.basic_ack(...)` / `basic_nack(...)` (the aiormq
  layer under aio-pika) rather than aio-pika's `message.ack()` wrapper. This
  is intentional: it allows `basic_ack(..., wait=False)`, which skips
  awaiting the socket drain on every ack (+10% consume throughput, measured).
  The ack is still sent per message, after the handler — see the
  at-least-once caveat under [Delivery guarantees](#delivery-guarantees).
- Only `Exception` subclasses are converted to nack+requeue;
  `asyncio.CancelledError` in a handler propagates as cancellation, leaving
  the message unacked (the broker redelivers it later).
- If cancelling the consumer at the broker fails on exit (broken channel),
  the client purges aio-pika's robust bookkeeping for that consumer, so a
  later reconnect cannot resurrect it alongside a retry's new consumer.

### `delete_queue()`

```python
await client.delete_queue(queue: str) -> None
```

Deletes the queue at the broker (a no-op on RabbitMQ if the queue does not
exist) and drops it from both internal declare caches, so the next
`publish()`/`consume()` re-declares instead of trusting a stale cache.

**`delete_queue()` does not cancel a live consumer.** Deleting a queue that
one of your own `consume()` tasks is consuming makes the *broker* send
`Basic.Cancel`; the consume task then raises `ConsumerCancelledError` via the
watchdog, within ~2x `cancel_check_interval`. If you want to stop consuming a
queue, cancel the consume task — don't delete the queue.

---

## `ConsumerCancelledError`

```python
class ConsumerCancelledError(RuntimeError)
```

Raised out of [`consume()`](#consume) when the broker cancelled the consumer
(typically: the queue was deleted). Exists because aio-pika drops a
broker-cancelled consumer silently and would otherwise leave the consume task
parked forever doing nothing. Catch it and retry `consume()` to re-declare
the queue and resume (see the loop above).

---

## Delivery guarantees

**The model is at-least-once.** Expect duplicates under failure; make
handlers idempotent.

- **Ack after handler.** A message is acked only after your handler returns.
  No batch ack exists that could cover an unfinished handler; per-message
  acks are inherently safe under concurrency.
- **Nack + requeue on raise.** A failing handler requeues exactly that one
  message; nothing else is affected and nothing is lost.
- **Consumer crash → redelivery, not loss.** Unacked messages (including any
  whose handler was mid-flight) return to the queue and are redelivered.
- **Ack-flush caveat (`wait=False`).** Acks are sent without awaiting the
  socket drain. If the process crashes at the wrong instant, the last few
  messages whose handlers *completed* but whose acks were not yet flushed to
  the broker are redelivered. That is duplicate work, never loss — the
  at-least-once contract, traded for ~10% consume throughput.
- **Publish is confirmed.** `publish()`/`publish_many()` resolve only after
  the broker accepts the message, so a resolved publish is safely in the
  broker's hands.
- **Broker crash — this is where `durable` matters.** Queues are always
  durable (the queue *definition* survives a broker restart), but messages
  survive only if published with `durable=True` (persistent delivery mode).
  With the default `durable=False`, messages in queues **are lost on broker
  restart** — even confirmed, even unconsumed. If you cannot afford that,
  construct the client with `durable=True`.

Summary: can you lose messages after a crash? Client/consumer crash — no
(redelivery, possibly duplicates). Broker crash — only the messages published
with `durable=False`.

## Reconnect behavior

Connections are created with aio-pika's `connect_robust`, so after a broker
restart or network blip the library re-establishes **connections, channels,
queues, and consumers** by itself — no application code needed. Details:

- The queue-declare caches stay valid across reconnects: aio-pika re-declares
  robust queues automatically when the connection returns.
- During an outage `is_connected` is `False` (the connection is "not closed"
  but not usable — the property checks the live `connected` event, not just
  `is_closed`).
- The `consume()` cancel watchdog suspends judgment during reconnects (miss
  counter resets while the connection or channel is down/being restored), so
  robust recovery is never misreported as `ConsumerCancelledError`.
- The one thing robust reconnect does *not* fix is a broker-side
  `Basic.Cancel` on a live connection — that is exactly the gap the watchdog
  covers.

## Performance reference points

Measured on the companion benchmark setup (1KB messages, local broker; see
[`rabbit-benchmark/`](../../rabbit-benchmark/)): publish ~9k msg/s per
connection with pipelined confirms; consume ceiling ~17.5k msg/s per process.
If you outgrow that, run more consumer processes — or read about the
benchmark suite's higher-maintenance `hybrid` client in the
[architecture doc](../../docs/architecture.md).
