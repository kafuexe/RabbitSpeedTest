# Architecture

## Layering

API ↓ Business ↓ DAL ↓ PostgreSQL

RabbitMQ Consumer ↓ Business ↓ DAL

## Rules

-   Dependencies flow downward only.
-   API and Consumer never access repositories directly.
-   Business services own orchestration.
-   Every business service uses only its own repository.
-   Business services may call other business services but never another
    service's DAL.
-   Constructor dependency injection only.
-   Bootstrap/composition root wires the application.

## Folder Structure

``` text
app/
  api/
  business/
  repositories/
  messaging/
  database/
  models/
  schemas/
  bootstrap/
  config/
  logging/
```

## Messaging

-   CloudEvents envelope
-   RabbitMQ via **SimpleClient**
-   Wrap SimpleClient behind an interface for testing.
-   Publish only after commit.
-   Consumer never republishes.
-   Unknown events are logged and rejected.

## Extensibility

Adding an entity should require only: - Model - Repository - Business
Service - API Schema - Routes - Event registration
