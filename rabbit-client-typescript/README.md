# @kafuexe/rabbit-client

Minimal RabbitMQ client for Node apps: **amqplib + amqp-connection-manager, zero
hand-rolled AMQP logic**.

This is the TypeScript counterpart of the canonical Python client in
[`rabbit-client-python/`](../rabbit-client-python/)
(`rabbit_client.RabbitClient`, built on aio-pika — full API reference in
[`../rabbit-client-python/docs/api.md`](../rabbit-client-python/docs/api.md)).
How the two clients fit into the repo and which to pick:
[`../docs/architecture.md`](../docs/architecture.md). Same
contract, same philosophy: everything subtle — reconnect, re-declares, consumer
re-establishment, publisher confirms — is delegated to
[amqp-connection-manager](https://github.com/jwalton/node-amqp-connection-manager),
which is maintained for you (the equivalent of aio-pika's `connect_robust`).

> **License note:** a license has not been chosen for this repository yet, so
> `package.json` says `UNLICENSED` for now.

## Install

The package is not published to a registry (none is available here — see
[`../docs/architecture.md`](../docs/architecture.md)), so install it from a
local checkout of this repo:

```sh
# once, in the checkout: build dist/
cd path/to/RabbitSpeedTest/rabbit-client-typescript && npm install && npm run build

# in your service: install by path
npm install path/to/RabbitSpeedTest/rabbit-client-typescript
```

(If it is ever published, this becomes `npm install @kafuexe/rabbit-client`.)

Requires Node >= 20. Ships compiled CommonJS with type declarations.

## Usage

```ts
import { RabbitClient } from '@kafuexe/rabbit-client';

const client = new RabbitClient('amqp://user:pass@host/', {
    prefetch: 200,   // per-consumer concurrency window (default 200)
    durable: false,  // message persistence (default false); queues are ALWAYS durable
});

await client.connect();

// Publish (confirmed by the broker before the promise resolves)
await client.publish('jobs', Buffer.from('payload'));

// Optional per-message properties — passed straight through to amqplib:
await client.publish('jobs', Buffer.from('payload'), {
    persistent: true,               // overrides the constructor `durable`
    headers: { 'x-source': 'api' },
    correlationId: 'req-123',
    messageId: 'msg-1',
    contentType: 'application/octet-stream',
    expiration: 30,                 // SECONDS (converted to the ms string amqplib expects)
    priority: 5,
});

// Bulk publish: pipelined confirms in batches of 1000
// (an optional third argument applies the same properties to every message)
await client.publishMany('jobs', Array.from({ length: 5000 }, () => Buffer.from('payload')));

// Consume: runs until you cancel it
const consumer = await client.consume('jobs', async (body: Buffer) => {
    await db.insert(body);   // your async work; throw/reject to requeue that message
});

// Per-consume prefetch override (falls back to the constructor value):
const slowConsumer = await client.consume('reports', handler, { prefetch: 5 });

// ... later
await consumer.cancel();     // or pass { signal } to consume() and abort it
await client.close();
```

Cancellation comes in two equivalent shapes — pick whichever fits your code:

```ts
// 1. The returned handle (primary API)
const consumer = await client.consume('jobs', handler);
await consumer.cancel();                 // idempotent

// 2. AbortSignal (plays well with structured shutdown code)
const controller = new AbortController();
await client.consume('jobs', handler, { signal: controller.signal });
controller.abort();
```

## Semantics (mirrors the Python client)

| Concern | Behavior | Python equivalent |
| --- | --- | --- |
| Connections | SEPARATE publish and consume connections — broker flow control on a busy publisher can never stall your consumers | identical |
| `connect()` | Opens both connections; if one side fails, the survivor is closed (never leaked) and the error is rethrown. Optional `{ timeoutMs }` caps the initial wait; without it the manager retries until the broker is reachable. Safe to call again after a failed/stale connection: it tears down the previous connection pair first | `connect()` (aio-pika raises on first failure; here the retry loop is the library default) |
| `isConnected()` | True only when BOTH connections are live *right now*; a manager mid-reconnect reports false | `is_connected` property |
| `publish(queue, body, options?)` | Declares the queue once (cached per queue per side), publishes to the default exchange on a confirm channel; resolves after the broker confirms | `publish()` |
| `publishMany(queue, bodies, options?)` | Pipelined confirms: fire 1000, await all confirms, next batch; `options` applies identically to every message (the amqplib options object is built once per call, keeping the hot path allocation-free) | `publish_many()` (same `_PIPELINE = 1000`) |
| Publish options | `persistent` (overrides constructor `durable`), `headers`, `correlationId`, `messageId`, `contentType`, `expiration` (**seconds**; converted to the string-milliseconds form amqplib wants), `priority` — pure passthrough to amqplib, zero hand-rolled logic | `persistent`, `headers`, `correlation_id`, `message_id`, `content_type`, `expiration` (seconds there too), `priority` |
| `consume(queue, handler, options?)` | Per-consumer prefetch (`basic.qos` global=false), overridable per consume via `options.prefetch` (falls back to the constructor value); ack only AFTER the handler resolves; a rejected handler nacks that ONE message with `requeue=true`; handlers overlap up to `prefetch` | `consume()` (per-consume `prefetch` kwarg) |
| `ConsumerHandle.cancel()` | Truly idempotent: concurrent and subsequent callers all await the SAME in-flight cancel RPC (nobody is told "done" early); only a FAILED RPC clears the latch so a retry re-issues the cancel | `cancel()` |
| `deleteQueue(queue)` | Cancels any active consumers on the queue first, **best-effort** — a failed cancel RPC does not abort the delete (safe: the wrapper deregisters the consumer synchronously before the RPC, so it can never resurrect); then deletes the queue and drops the cached declares on both sides (including the reconnect setup functions, so a reconnect cannot resurrect the queue). Handles are deregistered either way; their `cancel()` becomes a no-op | `delete_queue()` |
| `close()` | Cancels ALL registered consumer handles best-effort first (held handles become resolved no-ops — `close()` invalidates them), then closes both connections | `close()` |
| Queue durability | Queues are ALWAYS declared `durable: true` — RabbitMQ 4 denies transient non-exclusive queues. The `durable` option governs *message* persistence (`persistent: true/false`) | identical |
| Delivery guarantee | At-least-once: per-message acks after the handler, redelivery on crash/requeue | identical |

## Reconnect behavior (delegated, and one deliberate divergence)

All reconnect logic lives in amqp-connection-manager:

- **Connections** re-establish themselves after a broker restart or network
  blip.
- **Queue declares** are registered as ChannelWrapper *setup functions*: they
  run once when first needed, are cached by this client (never re-sent per
  publish), and are re-run automatically by the library on every reconnect —
  so the declare cache stays valid across outages.
- **Consumers** are re-established by `ChannelWrapper.consume()` on reconnect
  **and** when the broker cancels them: a broker-sent `Basic.Cancel` (e.g. the
  queue was deleted) surfaces in amqplib as a `null` delivery, and
  amqp-connection-manager reacts by re-consuming immediately (re-running the
  queue-declare setup first if the channel bounces).
- **Publishes** issued while disconnected are buffered by the ChannelWrapper
  and flushed (and confirmed) after reconnect.
- **Acks from pre-reconnect deliveries are dropped by design.** Delivery tags
  are only valid on the channel that issued them, so when a handler finishes
  *after* a reconnect, its ack/nack is silently discarded instead of being
  sent on the new channel (where the stale tag would be a protocol error —
  or worse, would ack an unrelated in-flight message that happens to carry
  the same tag number). The broker redelivers every message that was unacked
  at the moment of the reconnect, so under the at-least-once contract this
  is duplicate work, never loss — the same idempotent-handler requirement as
  the shutdown story below.
- **The reconnect epoch is tracked by a channel *setup function*, not the
  `'connect'` event — the timing matters.** The ChannelWrapper re-establishes
  consumers *before* it emits `'connect'`, and amqplib dispatches deliveries
  synchronously from the same TCP burst as the consume-ok. So the first
  post-reconnect deliveries can arrive before any `'connect'` listener runs:
  an event-based epoch bump ran too late, those deliveries captured the
  stale epoch, all their acks were dropped, and the consumer wedged with a
  full prefetch window of permanently-unacked messages. Setup functions run
  strictly before consumer re-establishment, so the epoch is always current
  before the first possible delivery. (A broker-side `Basic.Cancel`
  re-consume happens on the *same* channel without re-running setups —
  correctly no bump there, since the outstanding tags stay valid.)
- **`deleteQueue()` cancels the queue's active consumers first,
  best-effort** — a failed cancel RPC does not abort the delete, because the
  wrapper deregisters the consumer synchronously before the RPC, so even a
  failed cancel can never be resurrected on reconnect. Handles are
  deregistered either way (their `cancel()` becomes a no-op), and the
  reconnect machinery never tries to re-establish a consumer on a queue that
  no longer exists.
- **`close()` cancels all registered consumer handles first, best-effort**
  (failures ignored — the connections are going away). Handles still held by
  callers become resolved no-ops instead of silently dead references:
  `close()` invalidates them. Since a reentrant `connect()` calls `close()`
  on the previous pair, it cleanly tears down the old connection's consumers
  too.

**Divergence from the Python client:** there is no `ConsumerCancelledError`
here, on purpose. The Python client needs a polling watchdog because aio-pika
silently drops broker-cancelled consumers and only restores them on a full
reconnect — so it raises for the caller to retry. In the JS stack the broker
cancel is delivered synchronously to the client library, and
amqp-connection-manager already re-establishes the consumer: after a
broker-side cancel or a reconnect, consuming resumes on its own — documented
upstream behavior, covered by that library's own test suite, so you never
need to audit its internals. Porting the watchdog verbatim
would add hand-rolled AMQP logic to guard against a failure mode the library
already handles — the opposite of this package's philosophy. `consume()`
therefore genuinely runs until *you* cancel it.

## Graceful shutdown

Be precise about what `cancel()` does: it cancels the consumer at the broker
(`basic.cancel`), which **stops new deliveries only — it does not wait for
in-flight handlers to finish**. Handlers are dispatched fire-and-forget for
concurrency, and the client keeps no registry of the outstanding promises, so
there is nothing for `cancel()` to await.

If you call `close()` right after `cancel()`, any handler still running will
finish its work, but its ack goes to a channel that no longer exists and
never reaches the broker. Those messages stay unacked, so the broker
**redelivers them** to the next consumer. Under the at-least-once contract
that is duplicate work, never loss — an acceptable cost on deploys *if your
handlers are idempotent* (they must be anyway).

To actually drain before exiting, track in-flight work yourself:

```ts
import { setTimeout as sleep } from 'node:timers/promises';

let inFlight = 0;
const consumer = await client.consume('jobs', async (body) => {
    inFlight++;
    try {
        await db.insert(body);
    } finally {
        inFlight--;
    }
});

process.on('SIGTERM', async () => {
    await consumer.cancel();                 // stops NEW deliveries; does not drain
    while (inFlight > 0) await sleep(50);    // drain: wait out the handlers yourself
    await client.close();                    // safe now — every finished handler's ack got out
    process.exit(0);
});
```

If you skip the drain loop, nothing is lost — you are just choosing
duplicate-work-on-deploy instead of a slower shutdown. (Add a deadline around
the loop if a stuck handler must not block SIGTERM forever; whatever is still
unacked at `close()` is redelivered.)

## Development

```sh
npm install
npm run build   # tsc -> dist/ (CJS + .d.ts)
npm test        # vitest unit tests; amqp-connection-manager is mocked, no broker needed
```
