# rabbit-client API reference

Package `rabbit-client`, module `rabbit_client`. Three public names:

```python
from rabbit_client import RabbitClient, Consumer, ConsumerCancelledError
```

Install instructions: [README](../README.md). Requires Python >= 3.12.

Everything below matches the implementation in
[`src/rabbit_client/client.py`](../src/rabbit_client/client.py) (the package
root [`src/rabbit_client/__init__.py`](../src/rabbit_client/__init__.py) only
re-exports it); the behaviors are pinned by the unit tests
(`tests/test_unit.py` and `tests/test_watchdog.py`, broker-free) and the
integration tests (`tests/test_rabbit_client.py`, auto-skip without a local
broker).

This client is the canonical Python sibling of the TypeScript client
([`rabbit-client-typescript/src/rabbit-client.ts`](../../rabbit-client-typescript/src/rabbit-client.ts));
the two are behaviorally equivalent, including consumer handles and
broker-cancel auto-recovery.

## Quick example

```python
import asyncio
from rabbit_client import RabbitClient

client = RabbitClient("amqp://user:pass@host/", durable=True)
await client.connect()

# Publish — resolves only after the broker confirms acceptance.
await client.publish("jobs", b"one payload")
await client.publish_many("jobs", [b"payload"] * 5000)

# Per-publish overrides / AMQP properties (all keyword-only, all optional):
await client.publish(
    "jobs", b"important",
    persistent=True,                  # override the constructor's durable flag
    headers={"x-attempt": 1},
    correlation_id="req-123",
    content_type="application/json",
)

async def handler(body: bytes) -> None:
    await db.insert(body)      # your async work; raise to requeue this message

# Consume — returns a handle; the consumer runs until YOU cancel it.
consumer = await client.consume("jobs", handler)
...
await consumer.cancel()        # stop consuming (idempotent)
await client.close()
```

A worker process that should park forever on its consumers:

```python
consumer = await client.consume("jobs", handler)
await consumer.wait()          # parks until cancel()/close(); this is the
                               # v0.1.x "await client.consume(...)" behavior
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
| `prefetch` | `200` | Default per-consumer prefetch (`basic.qos`). Also the concurrency ceiling: deliveries run as concurrent tasks, so up to `prefetch` handlers overlap. With many busy queues on one client, remember it applies **per consumer** — size it accordingly (e.g. `prefetch=50`), or override it per consumer via [`consume(..., prefetch=...)`](#consume). |
| `durable` | `False` | Governs **message persistence** (`delivery_mode` PERSISTENT vs NOT_PERSISTENT) for everything this client publishes, overridable per publish via `persistent=`. It does **not** govern queue durability — queues are *always* declared durable, because RabbitMQ 4 denies transient non-exclusive queues. See [Delivery guarantees](#delivery-guarantees). |
| `cancel_check_interval` | `5.0` | Seconds between watchdog checks for broker-side consumer cancellation. A silent broker cancel is detected within at most ~2x this interval, then automatically recovered from. See [Broker-cancel auto-recovery](#broker-cancel-auto-recovery). |

Constructing the client opens nothing; call [`connect()`](#connect) first.
One client holds **two** broker connections — one for publishing, one for
consuming — so broker flow control on a busy publisher can never stall your
consumers.

### The not-connected contract

Every method that talks to the broker — `publish()`, `publish_many()`,
`consume()` and `delete_queue()` — raises

```python
RuntimeError("rabbit-client is not connected — call connect() first")
```

when called before `connect()`, or after a `connect()` that failed. This is a
deliberate misuse guard, not an incidental crash; nothing else raises it.
(`close()` and `is_connected` are safe to use at any time.)

### `connect()`

```python
await client.connect() -> None
```

Opens the two connections concurrently (both via aio-pika's
`connect_robust`), then a publish channel **with publisher confirms** and a
consume channel with the constructor `prefetch` applied. Also resets the
internal queue-declare caches.

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

**Cancels all outstanding [`Consumer`](#class-consumer) handles first** —
their internal tasks are cancelled and awaited while the connection is still
usable (so the broker-side `basic.cancel` goes out) — then closes both
connections (skipping any that are absent or already closed). A pending
`Consumer.wait()` returns `None`, exactly as if `cancel()` had been called on
each handle; the handles are left in the cancelled state and their `cancel()`
remains a safe no-op. Safe to call more than once.

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
await client.publish(
    queue: str,
    body: bytes,
    *,
    persistent: bool | None = None,
    headers: dict | None = None,
    correlation_id: str | None = None,
    message_id: str | None = None,
    content_type: str | None = None,
    expiration: float | None = None,     # seconds
    priority: int | None = None,
) -> None
```

Publishes one message to `queue` via the default exchange (the queue name is
the routing key). The first publish to a queue declares it (durable); the
declare is cached, so subsequent publishes skip the round-trip. Because the
publish channel runs with confirms, `publish()` resolves only once the broker
has **accepted** the message.

Raises the [not-connected `RuntimeError`](#the-not-connected-contract) if
called before `connect()`.

**Per-publish overrides and properties** (all keyword-only, all default
`None` = "not set"):

| Keyword | Meaning |
|---------|---------|
| `persistent` | Overrides the constructor's `durable` flag for this message: `True` → delivery mode PERSISTENT, `False` → NOT_PERSISTENT, `None` → whatever the constructor said. |
| `headers` | AMQP application headers (`dict`). This is the extensibility seam — retry counters, tracing IDs, routing hints for downstream DLX setups all live here. |
| `correlation_id` | AMQP `correlation-id` property (request/reply correlation). |
| `message_id` | AMQP `message-id` property (deduplication keys, audit). |
| `content_type` | AMQP `content-type` property (e.g. `"application/json"`). |
| `expiration` | Per-message TTL in **seconds** (aio-pika accepts seconds and converts to the millisecond TTL the broker expects). An expired message is dropped — or dead-lettered if the queue has a DLX. |
| `priority` | Message priority (only meaningful on priority queues). |

These map *directly* onto `aio_pika.Message` keyword arguments — the client
adds zero interpretation of its own, so aio-pika's documentation for these
fields applies verbatim. Note that consuming through this client hands your
handler **only the body bytes**; properties are for brokers, middleware, and
raw-AMQP consumers to read (see
[Extensibility & deliberate non-features](#extensibility--deliberate-non-features)).

### `publish_many()`

```python
await client.publish_many(
    queue: str,
    bodies: list[bytes],
    *,
    persistent: bool | None = None,
    headers: dict | None = None,
    correlation_id: str | None = None,
    message_id: str | None = None,
    content_type: str | None = None,
    expiration: float | None = None,     # seconds
    priority: int | None = None,
) -> None
```

Bulk publish with **pipelined confirms**: messages are fired in batches of
1000 and each batch's confirms are awaited together, instead of one
round-trip per message (~9k msg/s for 1KB messages on the benchmark setup).
Same declare caching, confirm, persistence and
[not-connected](#the-not-connected-contract) semantics as `publish()`.

The keyword properties are identical to `publish()`'s and are applied to
**every message in the batch** (there is no per-body variation; publish
individually if you need that).

### `consume()`

```python
await client.consume(
    queue: str,
    handler: Callable[[bytes], Awaitable[None]],
    *,
    prefetch: int | None = None,
) -> Consumer
```

Declares `queue` (durable, cached) and starts consuming from it, calling
`await handler(message_body)` for each delivery. Handlers run as concurrent
tasks, up to the effective prefetch in flight — a handler awaiting a database
overlaps with others, which is what decides real throughput for a DB-bound
consumer.

**The consumer is fully established before `consume()` returns** — queue
declared, `basic.consume` issued — so setup errors (bad queue name, broken
channel, [not connected](#the-not-connected-contract)) raise at the call
site, not inside some background task. What you get back is a
[`Consumer`](#class-consumer) handle; the consumer then runs until you call
`cancel()` on it (or [`close()`](#close) the client). It survives both
reconnects *and* broker-side cancels — see
[Broker-cancel auto-recovery](#broker-cancel-auto-recovery).

**Per-consume `prefetch` override.** When `prefetch` is not `None`, the
client issues `basic.qos(prefetch_count=prefetch, global=false)` on the
consume channel immediately before `basic.consume` — RabbitMQ binds the
channel's current qos to a consumer *at consume time*, which is what makes
the override per-consumer — and re-issues it before every internal
re-consume (auto-recovery), so the override sticks for the consumer's whole
life. When `prefetch` is `None`, the constructor's prefetch (applied at
`connect()`) is in effect. One sharp edge inherited from AMQP itself: the
override changes the *channel's* current qos, so a later
`consume(prefetch=None)` on the same client picks up the most recent
override rather than the constructor value — if you mix overridden and
non-overridden consumers, start the non-overridden ones first or give every
consumer an explicit `prefetch`.

Per message:

1. `handler(body)` is awaited.
2. If it returns normally, the message is **acked** — exactly once, strictly
   after the handler finishes.
3. If it raises any `Exception`, that one message is **nacked with
   `requeue=True`** (it will be redelivered — to this consumer or another)
   and is never acked. The consume loop itself keeps running. See the
   [hot-loop caveat](#extensibility--deliberate-non-features) — with a single
   consumer, a poison message is redelivered *immediately*, ahead of the
   queue.

Constraints and details:

- **One `consume()` per queue per client.** Consumers are cheap — they are
  multiplexed on the single consume connection, no extra threads — so consume
  many queues by calling `consume()` once for each and holding the handles.
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
- If cancelling the consumer at the broker fails on teardown (broken
  channel), the client purges aio-pika's robust bookkeeping for that
  consumer, so a later reconnect cannot resurrect it alongside a replacement.

### `delete_queue()`

```python
await client.delete_queue(queue: str) -> None
```

Deletes the queue at the broker (a no-op on RabbitMQ if the queue does not
exist) and drops it from both internal declare caches, so the next
`publish()`/`consume()` re-declares instead of trusting a stale cache. Raises
the [not-connected `RuntimeError`](#the-not-connected-contract) if called
before `connect()`.

**`delete_queue()` does not cancel your own live consumer** — and since
v0.2.0 that combination *re-creates the queue*: deleting a consumed queue
makes the broker send `Basic.Cancel`, auto-recovery kicks in, and the
consumer re-declares the queue and resumes on the (now empty) replacement.
If you mean to stop consuming a queue and remove it, do it in this order:

```python
await consumer.cancel()
await client.delete_queue("jobs")
```

---

## class `Consumer`

The handle returned by [`consume()`](#consume). Not constructed directly.

| Member | Signature | Behavior |
|--------|-----------|----------|
| `queue` | `str` attribute | The queue this consumer was started on. |
| `cancel` | `await consumer.cancel() -> None` | Stops consuming: cancels the internal task, which cancels the consumer at the broker on its way out. **Idempotent and concurrent-safe** — the second (and any concurrent) caller awaits the same underlying cancellation; exactly one broker-side `basic.cancel` is ever issued. When `cancel()` returns, the consumer is fully stopped. |
| `wait` | `await consumer.wait() -> None` | Parks until the consumer is cancelled — via `cancel()` or [`client.close()`](#close) — then returns `None` (calling it *after* cancellation also returns `None` immediately). An **unexpected internal error** (the recovery machinery itself failing — never a raising message handler, which only nack+requeues its message) is re-raised here. Cancelling the task that is awaiting `wait()` propagates `CancelledError` normally and does **not** stop the consumer. |

Migration note: the v0.1.x `await client.consume(q, h)` — which parked
forever — is now spelled

```python
consumer = await client.consume(q, h)
await consumer.wait()
```

## Broker-cancel auto-recovery

aio-pika handles a broker-sent `Basic.Cancel` (typically: the consumed queue
was deleted) by *silently dropping* the consumer — nothing is raised into any
task, and consumers are only re-established on **reconnect**. Left alone,
your consumer would park forever doing nothing. The TypeScript client never
had this problem because amqp-connection-manager re-establishes
broker-cancelled consumers automatically; since v0.2.0 this client does the
same:

1. A watchdog inside the consumer's internal task polls the underlying
   channel's consumer table every `cancel_check_interval` seconds and treats
   **two consecutive misses** as a broker cancel (detection latency ≤ ~2x the
   interval, ~10 s at the default). It is reconnect-aware: while the
   connection is down or the channel object is being replaced/restored, the
   miss counter resets — a slow reconnect is never mistaken for a cancel.
2. On detection it logs a WARNING via `logging.getLogger("rabbit_client")` —
   message exactly

   ```
   consumer cancelled by broker; re-declaring and resuming
   ```

   with `extra={"queue": <queue name>}` (structured-logging friendly: the
   record carries a `queue` attribute).
3. It sleeps a short backoff (module constant, **1.0 s** — long enough not to
   hot-loop against a broker that keeps deleting the queue), then
   **re-declares the queue and re-consumes**, re-applying the per-consume
   `prefetch` override if one was given. This repeats forever — every
   subsequent broker cancel is recovered from the same way — until you
   `cancel()` the handle.

The handle never notices: `wait()` keeps parking across recoveries, and
`ConsumerCancelledError` is **not** raised to you. Handle `cancel()` (task
cancellation) always takes effect promptly, including mid-backoff.

### `ConsumerCancelledError`

```python
class ConsumerCancelledError(RuntimeError)
```

Still exported, but since v0.2.0 it is an *internal* signal: the watchdog
raises it inside the consumer task and the recovery loop absorbs it. It no
longer surfaces through `Consumer.wait()`. You would only ever see it if you
drive the library's internals directly; there is no reason to catch it in
application code anymore (v0.1.x retry loops that caught it are simply
obsolete — delete them).

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
- **Broker crash — this is where `durable`/`persistent` matters.** Queues are
  always durable (the queue *definition* survives a broker restart), but
  messages survive only if published persistent (`durable=True` on the
  constructor, or `persistent=True` on the call). Otherwise messages in
  queues **are lost on broker restart** — even confirmed, even unconsumed.

Summary: can you lose messages after a crash? Client/consumer crash — no
(redelivery, possibly duplicates). Broker crash — only the messages published
non-persistent.

## Reconnect behavior

Connections are created with aio-pika's `connect_robust`, so after a broker
restart or network blip the library re-establishes **connections, channels,
queues, and consumers** by itself — no application code needed. Details:

- The queue-declare caches stay valid across reconnects: aio-pika re-declares
  robust queues automatically when the connection returns.
- During an outage `is_connected` is `False` (the connection is "not closed"
  but not usable — the property checks the live `connected` event, not just
  `is_closed`).
- The watchdog suspends judgment during reconnects (miss counter resets while
  the connection or channel is down/being restored), so robust recovery is
  never misreported as a broker cancel.
- The one thing robust reconnect does *not* fix is a broker-side
  `Basic.Cancel` on a live connection — that is exactly the gap the
  [auto-recovery machinery](#broker-cancel-auto-recovery) covers.

## Extensibility & deliberate non-features

This client is deliberately small; the following are *decisions*, not
omissions:

- **Default exchange only.** Messages are routed by queue name via the
  default exchange. No exchange declaration, no bindings, no topic/fanout
  routing. If you need routing topology, declare it out-of-band (deployment
  scripts, definitions file) or use aio-pika directly — this client will
  still happily consume the queues that topology feeds.
- **No queue arguments, no DLX wiring.** `declare_queue` is always plain
  `durable=True` — no `x-dead-letter-exchange`, `x-max-priority`,
  `x-message-ttl`, quorum flags etc. Queue arguments must match on every
  redeclare forever (they are effectively schema), which is the opposite of a
  drop-in client. Declare argumented queues out-of-band; note that priority
  (`x-max-priority`) and DLX behavior then work with messages published here.
- **The properties seam is where retry/DLX patterns attach.** Everything the
  usual resilience patterns need travels through the
  [`publish()` properties](#publish): retry counters in `headers`
  (`x-attempt`), per-message TTL via `expiration` (expired messages
  dead-letter if the queue — declared out-of-band — has a DLX),
  `correlation_id`/`message_id` for tracing and dedup. A retry-with-backoff
  publisher, for example, is a dozen lines *on top of* this client, with no
  changes inside it.
- **Handlers receive bytes, not messages.** `handler(body: bytes)` keeps the
  handler contract trivial and fake-able. The cost: incoming properties
  (headers etc.) are not visible to handlers. If a consumer must read
  properties, that is the signal you have outgrown this client's consume side
  — drop to aio-pika for that queue.
- **Immediate-requeue hot-loop caveat.** A raising handler nack+requeues, and
  RabbitMQ requeues toward the *head* of the queue — with a single consumer,
  a poison message is redelivered immediately, over and over, ahead of
  everything behind it. There is no built-in retry limit. If poison messages
  are possible in your workload, either count attempts in your handler's own
  storage, or (the robust answer) declare the queue out-of-band with a
  dead-letter exchange and have your handler `raise` only until an
  attempt-count header (republished via the properties seam) crosses your
  limit, then ack and republish to a parking queue.

## Performance reference points

Measured on the companion benchmark setup (1KB messages, local broker; see
[`rabbit-benchmark/`](../../rabbit-benchmark/)): publish ~9k msg/s per
connection with pipelined confirms; consume ceiling ~17.5k msg/s per process.
If you outgrow that, run more consumer processes — or read about the
benchmark suite's higher-maintenance `hybrid` client in the
[architecture doc](../../docs/architecture.md).
