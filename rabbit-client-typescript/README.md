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
cd path/to/rabbit-platform/rabbit-client-typescript && npm install && npm run build

# in your service: install by path
npm install path/to/rabbit-platform/rabbit-client-typescript
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

// Bulk publish: pipelined confirms in batches of 1000
await client.publishMany('jobs', Array.from({ length: 5000 }, () => Buffer.from('payload')));

// Consume: runs until you cancel it
const consumer = await client.consume('jobs', async (body: Buffer) => {
    await db.insert(body);   // your async work; throw/reject to requeue that message
});

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
| `connect()` | Opens both connections; if one side fails, the survivor is closed (never leaked) and the error is rethrown. Optional `{ timeoutMs }` caps the initial wait; without it the manager retries until the broker is reachable | `connect()` (aio-pika raises on first failure; here the retry loop is the library default) |
| `isConnected()` | True only when BOTH connections are live *right now*; a manager mid-reconnect reports false | `is_connected` property |
| `publish(queue, body)` | Declares the queue once (cached per queue per side), publishes to the default exchange on a confirm channel; resolves after the broker confirms | `publish()` |
| `publishMany(queue, bodies)` | Pipelined confirms: fire 1000, await all confirms, next batch | `publish_many()` (same `_PIPELINE = 1000`) |
| `consume(queue, handler)` | Per-consumer prefetch (`basic.qos` global=false); ack only AFTER the handler resolves; a rejected handler nacks that ONE message with `requeue=true`; handlers overlap up to `prefetch` | `consume()` |
| `deleteQueue(queue)` | Deletes the queue and drops the cached declares on both sides (including the reconnect setup functions, so a reconnect cannot resurrect the queue) | `delete_queue()` |
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

**Divergence from the Python client:** there is no `ConsumerCancelledError`
here, on purpose. The Python client needs a polling watchdog because aio-pika
silently drops broker-cancelled consumers and only restores them on a full
reconnect — so it raises for the caller to retry. In the JS stack the broker
cancel is delivered synchronously to the client library, and
amqp-connection-manager already resurrects the consumer (see
`_reconnectConsumer` in its `ChannelWrapper`). Porting the watchdog verbatim
would add hand-rolled AMQP logic to guard against a failure mode the library
already handles — the opposite of this package's philosophy. `consume()`
therefore genuinely runs until *you* cancel it.

## Development

```sh
npm install
npm run build   # tsc -> dist/ (CJS + .d.ts)
npm test        # vitest unit tests; amqp-connection-manager is mocked, no broker needed
```
