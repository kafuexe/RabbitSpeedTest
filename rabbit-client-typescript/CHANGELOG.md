# Changelog

All notable changes to `@kafuexe/rabbit-client` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Reconnect-epoch timing: the epoch is now bumped by a channel SETUP
  function instead of the ChannelWrapper `'connect'` event — the event-based
  bump wedged consumers after every reconnect.** The wrapper re-establishes
  consumers *before* emitting `'connect'`, and amqplib dispatches deliveries
  synchronously from the same TCP burst as the consume-ok, so the first
  post-reconnect deliveries captured the PRE-bump epoch. The epoch guard
  then (correctly, per its own logic) dropped every one of their acks: with
  a single consumer the entire prefetch window leaked (e.g. 200 unacked
  messages that could never be acked), the broker stopped delivering, and
  the consumer wedged forever with ready messages sitting in the queue.
  Setup functions run strictly before consumer re-establishment, so the
  epoch is now always current before the first possible delivery.
  (Broker-cancel re-consumes happen on the same channel without re-running
  setups — correctly no bump, since outstanding tags stay valid.)
- `ConsumerHandle.cancel()` is now truly idempotent under concurrency: all
  callers — concurrent or subsequent — share the SAME in-flight cancellation
  promise, so a second caller can no longer be told "done" while the first
  RPC is still pending (or have its reported success invalidated by a later
  failure resetting the latch). Only a failed RPC clears the stored promise,
  so a retry re-issues the cancel.
- Consume path: acks/nacks from handlers that settle **after a reconnect**
  are now dropped instead of being sent to the new channel with a stale
  delivery tag (reconnect-epoch guard on the consume `ChannelWrapper`).
  Previously the stale tag caused a broker `406 PRECONDITION_FAILED` that
  tore down the whole consume connection — dropping every consumer — and
  could even silently ack an unrelated in-flight message that reused the
  same tag number on the new channel. The broker redelivers the unacked
  message, preserving at-least-once semantics.

### Changed

- `deleteQueue()` consumer cancels are now **best-effort**: a failed cancel
  RPC no longer rejects the whole delete (which used to leave the queue and
  its declare setups fully intact). Safe because `ChannelWrapper.cancel()`
  removes the consumer from its registry synchronously, before the RPC —
  a failed cancel can never be resurrected on reconnect. Handles are
  deregistered regardless of RPC outcome; `removeSetup` and the queue
  delete always proceed.
- `close()` now cancels all registered consumer handles best-effort before
  closing the connections (previously it just cleared the registry, leaving
  caller-held handles silently dead). `close()` thereby invalidates
  outstanding handles — they become resolved no-ops — and a reentrant
  `connect()` (which calls `close()`) cleanly tears down the previous
  connection's consumers.

### Added

- Per-consume `prefetch` override: `consume(queue, handler, { prefetch })`
  applies `basic.qos(global=false)` for that consumer only, falling back to
  the constructor `prefetch` (mirrors the Python client's per-consume
  `prefetch`).
- Publish message-property passthrough on `publish()` and `publishMany()`:
  `persistent` (overrides constructor `durable`), `headers`,
  `correlationId`, `messageId`, `contentType`, `expiration` (**seconds**,
  converted to the string-milliseconds form amqplib expects) and
  `priority` — mapped straight to amqplib publish options with zero
  hand-rolled logic. `publishMany` applies the same options to every
  message and builds the options object once per call, keeping the bulk
  hot path allocation-free.
- `deleteQueue()` now cancels all active consumers on the queue **before**
  deleting it (their handles' `cancel()` becomes a no-op). Previously the
  consumer registration survived the delete: the broker's `Basic.Cancel`
  made the wrapper re-consume on the now-missing queue, the resulting `404`
  closed the shared consume channel, and the reconnect machinery re-ran all
  consumers — including the dead one — in an infinite 5-second loop that
  starved every other consumer on the channel and produced unhandled
  rejections.
- `connect()` is now reentrant: if a previous `connect()` left connection
  managers behind (e.g. after a failed or stale connection), they are torn
  down via `close()` before the new pair is opened. Previously a second
  `connect()` silently overwrote both managers, leaving the abandoned pair
  reconnecting forever with its consumers still consuming in parallel.
- `ConsumerHandle.cancel()` no longer latches permanently when the cancel
  RPC rejects — a failed cancel can now be retried.

## [0.1.0] - 2026-07-19

### Added

- `RabbitClient` — minimal RabbitMQ client built on `amqplib` +
  `amqp-connection-manager` with zero hand-rolled AMQP logic; TypeScript
  counterpart of the canonical Python `rabbit_client`.
- Separate publish and consume connections, so broker flow control on a busy
  publisher never stalls consumers.
- `connect()` / `close()` / `isConnected()` lifecycle, with an optional
  `timeoutMs` cap on the initial connect and automatic reconnect (including
  queue re-declares and consumer resurrection) afterwards.
- `publish()` and `publishMany()` on a confirm channel; `publishMany`
  pipelines confirms in batches of 1000 (measured bulk-publishing knee).
- `consume()` returning a `ConsumerHandle` (`consumerTag`, idempotent
  `cancel()`), with optional `AbortSignal` cancellation, per-consumer
  prefetch (default 200), ack-after-handler-resolves, and
  nack-with-requeue on handler rejection.
- `deleteQueue()` that also removes the cached declare setups so reconnects
  cannot resurrect a deleted queue.
- Queue declares cached once per queue per side and re-run automatically on
  reconnect; queues always declared durable (RabbitMQ 4 denies transient
  non-exclusive queues); `durable` option governs message persistence.
- Mocked unit test suite (vitest) covering connection topology, declare
  caching, confirm pipelining, ack/nack ordering, cancellation, and cleanup.
- Integration test suite against a live broker (auto-skipped when no broker
  listens on `localhost:5672`): round-trip payload integrity, redelivery on
  handler rejection, 3000-message bulk drain, real prefetch enforcement,
  consumer cancel, and queue deletion.
- Strict TypeScript build (CommonJS, ES2022 target) with declarations,
  declaration maps, and source maps; `typecheck` script over src + tests.
- Benchmark harness (`npm run bench`).
