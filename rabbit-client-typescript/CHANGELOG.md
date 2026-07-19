# Changelog

All notable changes to `@kafuexe/rabbit-client` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
