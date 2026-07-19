# Acceptance Checklist

## Architecture

-   [ ] API never accesses repositories.
-   [ ] Consumer never accesses repositories.
-   [ ] Business layer independent of FastAPI.
-   [ ] Business layer independent of RabbitMQ.
-   [ ] Every service owns only its repository.
-   [ ] Cross-entity communication only through services.
-   [ ] Constructor DI everywhere.
-   [ ] Bootstrap wires dependencies.

## Messaging

-   [ ] CloudEvents.
-   [ ] Uses RabbitClient.
-   [ ] Publish after commit only.
-   [ ] Consumer never republishes.
-   [ ] Unknown events handled.

## Database

-   [ ] Explicit transactions.
-   [ ] Rollbacks.
-   [ ] Pagination.
-   [ ] Filtering.
-   [ ] Sorting.

## Reliability

-   [ ] Idempotent.
-   [ ] Duplicate-safe.
-   [ ] Horizontal scaling.
-   [ ] Outbox-ready.

## Quality

-   [ ] Structured logging.
-   [ ] Strong typing.
-   [ ] Health endpoint.
-   [ ] Readiness endpoint.
-   [ ] Unit tests.
-   [ ] Integration tests.
