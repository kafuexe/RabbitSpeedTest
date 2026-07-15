# Architecture Notes

## Layering and flows

```
HTTP request ──► api/router (thin) ──► business service ──► repository ──► PostgreSQL
                                          │
                                          └─ UnitOfWork: stage events → commit → publish

RabbitMQ ──► messaging/consumer ──► registry ──► module event handler
             (decode CloudEvent)                      │
                                                      ▼
                                              business service (same one)
                                              inbox dedup + version guard
                                              commit — nothing republished
```

Dependencies point downward only. The business layer imports neither FastAPI
nor RabbitMQ; the API and consumer edges import the business layer, never
repositories.

Supervision: the container owns the consumer task — its death is logged
CRITICAL and flips `/ready`'s `consumer` check to false (a dead consumer can
never look healthy). Each queue is retried independently on failure, so one
bad queue neither kills nor hides the others. Broker readiness uses the
connection's live `connected` event, not `is_closed` (which stays false
during a reconnect loop).

## Two object graphs, one codebase

The composition root (`app/bootstrap/container.py`) wires the same
`UserService` twice:

| graph    | UnitOfWork publisher   | effect                                |
|----------|------------------------|---------------------------------------|
| API      | `QueueEventPublisher`  | events published after commit         |
| consumer | `NullEventPublisher`   | consumer can never republish, by construction |

## Transactions

One `UnitOfWork` per API request / consumed message. Repositories join the
UoW's session and never commit. Domain events are staged during the
transaction; `commit()` publishes them strictly afterwards, `rollback()`
discards them. Read paths also commit (releases locks, keeps ORM instances
usable after the session closes).

**Commit-succeeds-publish-fails** is logged loudly with the event payload and
correlation id and does not fail the request — this is the documented gap the
Outbox closes: implement an `OutboxPublisher` that inserts staged events into
an `outbox` table on the same session, plus a relay process; only bootstrap
changes.

## Idempotency & ordering

- **Inbox** (`processed_events`, PK = source + event id): inserted in the
  same transaction as the entity write; duplicate deliveries hit the PK and
  are skipped. Works across any number of instances.
- **Versioned state events**: `user.created` / `user.updated` carry the full
  entity state and its `version`. The consumer upserts when the row is
  missing (update-before-create), applies when `event.version > stored`,
  drops as stale otherwise (late create after update).
- **API**: create is an idempotent insert keyed on the client-supplied id
  (replay → 200, contradictory replay — name, email OR attributes — → 409);
  updates take a row lock and honor optional `expected_version` (409 on
  mismatch). A replayed create RE-ANNOUNCES the stored state event: if the
  first attempt died in the ambiguous commit window (commit applied, ack
  lost, event dropped — logged CRITICAL by the UoW), the retry publishes it;
  consumers drop the duplicate via the version guard.

## Consumer micro-batching (throughput without losing guarantees)

One PostgreSQL commit per consumed message caps throughput at the database's
fsync rate. The consumer therefore funnels concurrent deliveries through a
**greedy batcher** (`messaging/batcher.py`) into one business call —
`apply_user_events(batch)` — which runs ONE transaction: bulk inbox insert
(duplicates filtered by RETURNING), highest-version-per-user wins within the
batch, and a single atomic `INSERT .. ON CONFLICT DO UPDATE .. WHERE
stored.version < new.version` (no row locks; the version guard is evaluated
by PostgreSQL).

Reliability is unchanged:

- The batcher is greedy — it never waits to fill a batch. Idle traffic gets
  batches of one (zero added latency); batches only grow when a backlog
  exists because deliveries queue while the previous commit is in flight.
- `submit()` resolves only after the batch's COMMIT, so SimpleClient acks
  each message strictly after its data is durable — at-least-once, same as
  unbatched.
- A failed batch is retried item-by-item: a poison item fails alone (its
  message requeues); the rest are applied and acked.
- Crash mid-batch: nothing committed → all messages redeliver → the inbox
  and version guard make the replay a no-op beyond the original effect.
- Shutdown never hangs or drops: a closed batcher fails queued AND in-flight
  submits with `BatcherClosedError` (a plain Exception → SimpleClient nacks
  while the channel is still open) and a late submit cannot resurrect it.
- Each flush runs under its own correlation id (a batch merges many message
  contexts); per-event ids are logged at DEBUG.

## Poison-message defense in depth

Anything that can never succeed must be acked away, not requeued:

1. Envelope: CloudEvents id/source/type are bounded to 255 chars (the inbox
   column width) — oversized attributes are an invalid envelope.
2. Payload: `UserEventData` enforces the business floor (non-blank name,
   '@' in email) AND storability (no NUL bytes anywhere — PostgreSQL rejects
   \x00 in text/JSONB). The API schemas enforce the same, so events and
   requests obey one rule set.
3. Last resort: a `DataError` raised by the database (deterministic storage
   rejection that slipped past validation) is classified permanent by the
   event handler — logged and acked, never looped. Transient errors
   (connection loss etc.) still raise → requeue → retry.

Rejection logs carry field locations and messages only, never input values
(`validation_error_reason`) — no PII in logs.

## SimpleClient semantics the consumer relies on

`simple_rabbit.SimpleRabbit`: handler **return = ack**, handler
**raise = nack + requeue**. Therefore permanent failures (invalid envelope,
unknown type, invalid payload, stale version, duplicate) LOG AND RETURN so the
message is acked away — never poison-looped; only transient failures
(database down) raise and get redelivered.

## Adding an entity

Create `app/modules/<entity>/` with `model.py`, `repository.py`,
`business.py`, `schemas.py`, `events.py`, `router.py`; register the router
and event handlers in bootstrap; add an Alembic revision. Nothing outside
the module and bootstrap changes.
