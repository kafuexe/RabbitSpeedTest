# Changelog

All notable changes to `rabbit-client` (Python) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
