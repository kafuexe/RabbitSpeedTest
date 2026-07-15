# Shared Data Service Requirements

## Purpose

Build a production-ready Python microservice that serves as the
authoritative storage service for shared application data.

## Functional Requirements

-   PostgreSQL is the system of record.
-   REST API for Create, Update, Get, List.
-   RabbitMQ consumer for Create/Update events.
-   Publish CloudEvents after successful DB commits only.
-   Consumer must never republish processed events.
-   Idempotent create/update handling.
-   Pagination, filtering, sorting.
-   Horizontal scalability.
-   Support Outbox pattern in the future.
-   Async-first implementation.

## Technology

-   Python 3.12
-   FastAPI
-   PostgreSQL
-   SQLAlchemy 2.x Async
-   Alembic
-   Pydantic v2
-   RabbitMQ using the existing **SimpleClient** implementation.

## Non-Functional Requirements

-   SOLID
-   Composition over inheritance
-   Strong typing
-   Structured logging
-   Health & readiness endpoints
-   OpenAPI
-   Unit, integration and messaging tests
-   Extensible architecture

## Edge Cases

-   Duplicate deliveries
-   Out-of-order events
-   Concurrent updates
-   Commit succeeds but publish fails
-   Invalid CloudEvents
-   Unknown event/entity types
-   Invalid filters/sorts
-   Large pagination requests
