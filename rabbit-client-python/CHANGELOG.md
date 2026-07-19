# Changelog

All notable changes to `hs-rabbit-client` (Python) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **BREAKING: distribution and module renamed** — the distribution is now
  `hs-rabbit-client` and the import module `hs_rabbit_client`
  (`from hs_rabbit_client import RabbitClient`). The old name `rabbit-client`
  is squatted on public PyPI, so keeping it risked dependency confusion
  through proxying indexes (e.g. Artifactory virtual repos). Class names
  (`RabbitClient`, `Consumer`, `ConsumerCancelledError`) are unchanged.
- **BREAKING: `consume()` returns a `Consumer` handle** instead of parking
  forever. The consumer is fully established (declare + `basic.consume`)
  before `consume()` returns, so setup errors raise at the call site. The
  handle exposes `queue`, `await cancel()` (idempotent and concurrent-safe)
  and `await wait()` (parks until cancelled; returns `None` after `cancel()`;
  re-raises unexpected internal errors). The old parking behavior is now
  `consumer = await client.consume(...)` followed by `await consumer.wait()`.
- **BREAKING: broker-side cancels are auto-recovered, not raised.** When the
  broker cancels a consumer (e.g. the queue was deleted), the internal
  watchdog now logs a WARNING (`hs_rabbit_client` logger, message
  `consumer cancelled by broker; re-declaring and resuming`,
  `extra={"queue": ...}`), backs off 1 s, re-declares the queue and resumes —
  forever, until `cancel()` (parity with the TypeScript client /
  amqp-connection-manager). `ConsumerCancelledError` no longer surfaces to
  callers (it remains exported as an internal signal); v0.1.x
  catch-and-retry loops around `consume()` are obsolete. Consequence:
  `delete_queue()` on a queue you are consuming now re-creates the queue via
  recovery — `cancel()` the consumer first.
- `close()` now cancels all outstanding `Consumer` handles (their broker-side
  consumers included) before closing the connections, so a pending
  `Consumer.wait()` returns `None`.
- Using the client before `connect()` (or after a failed connect) now raises a
  clear `RuntimeError("hs-rabbit-client is not connected — call connect() first")`
  from `publish()`, `publish_many()`, `consume()` and `delete_queue()`, instead
  of an incidental `AttributeError` on an internal `None` channel.
- Internal restructure: the implementation moved from
  `src/hs_rabbit_client/__init__.py` to `src/hs_rabbit_client/client.py`; the
  package root now only re-exports the public names. The public import path
  `from hs_rabbit_client import RabbitClient` is unchanged.

### Added

- Per-consume prefetch override: `consume(queue, handler, prefetch=N)` issues
  `basic.qos` (global=false) immediately before `basic.consume`, scoping the
  override to that consumer; it is re-applied on every internal re-consume.
  The constructor `prefetch` stays the default.
- Per-publish overrides and AMQP properties passthrough on `publish()` and
  `publish_many()` (keyword-only, applied to every message of a batch):
  `persistent` (overrides the constructor `durable` flag per message),
  `headers`, `correlation_id`, `message_id`, `content_type`, `expiration`
  (seconds), `priority` — mapped directly onto `aio_pika.Message` kwargs.
- `Consumer` is exported from the package root.

## [0.1.0] - 2026-07-19

Initial release: the library was extracted from the RabbitSpeedTest benchmark
suite's `simple` client into a standalone, installable package
(`rabbit-client-python/` in the [RabbitSpeedTest](https://github.com/kafuexe/RabbitSpeedTest)
repo). A planned repository rename to `rabbit-platform` was cancelled; all
references point at `RabbitSpeedTest`.

### Added

- `RabbitClient`: minimal aio-pika-only RabbitMQ publisher/consumer.
  - `connect()` / `close()` — two robust connections (publish and consume are
    isolated, so publisher flow control can never stall consumers), with
    partial-failure cleanup (a surviving connection is closed, not leaked).
  - `publish()` / `publish_many()` — publisher confirms, pipelined in batches
    of 1000 for bulk throughput; queue declares cached once per queue.
  - `consume()` — per-message ack strictly after the handler returns; a
    raising handler nacks with requeue (at-least-once). Deliveries run as
    concurrent tasks up to `prefetch`.
  - Broker-cancel watchdog: a broker-sent `Basic.Cancel` (e.g. queue deleted)
    is silently swallowed by aio-pika; `consume()` polls the underlying
    channel's consumer table and raises `ConsumerCancelledError` after two
    consecutive misses so callers can re-declare and retry. Reconnects and
    channel resets are recognized and never mistaken for a cancel.
  - `is_connected` — true only when both connections are live right now
    (a robust connection mid-reconnect reports `False`).
  - `delete_queue()` — deletes and invalidates both declare caches.
- `ConsumerCancelledError` exception type.
- `py.typed` marker (PEP 561) — the package ships inline type annotations.
- Test suite: broker-free unit tests (fakes over `aio_pika.connect_robust`),
  watchdog/lifecycle unit tests, and integration tests that auto-skip when no
  broker listens on `localhost:5672`.
- Tooling: ruff (lint + format) and strict mypy configuration in
  `pyproject.toml`; CI via the repo-level GitHub Actions workflow.

[Unreleased]: https://github.com/kafuexe/RabbitSpeedTest/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kafuexe/RabbitSpeedTest/releases/tag/v0.1.0
