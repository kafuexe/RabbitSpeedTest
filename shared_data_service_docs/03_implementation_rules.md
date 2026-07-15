# Implementation Rules

## General

-   Python 3.12
-   Async only
-   SQLAlchemy 2.x
-   Pydantic v2
-   Strong typing everywhere
-   No global mutable state

## Dependency Injection

-   Constructor injection only.
-   Everything resolved by bootstrap.
-   Depend on interfaces, not implementations.

## Business Layer

-   Independent of FastAPI.
-   Independent of RabbitMQ.
-   Uses only its own DAL.
-   May call other business services.
-   Owns validation and orchestration.

## DAL

-   CRUD only.
-   No business rules.
-   Explicit transactions.
-   Pagination/filter support.

## API

-   Thin controllers.
-   No business logic.
-   No repository access.

## Messaging

-   Use existing **SimpleClient**.
-   Do not introduce another RabbitMQ client.
-   Interface-wrap SimpleClient for mocking.
-   CloudEvents everywhere.

## Logging

-   Structured logging.
-   Correlation IDs.
-   Never log secrets or PII.

## Testing

-   Unit tests
-   Repository tests
-   Business tests
-   API integration tests
-   RabbitMQ integration tests
