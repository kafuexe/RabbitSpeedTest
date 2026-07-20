# The Reliability Model

The [Architecture Tour](03-architecture-tour.md) showed you the shape of the
system. This chapter shows you why you can trust it. Every guarantee in the
README's "Guarantees" list is backed by a specific mechanism in a specific
file, and by the end of this chapter you will have seen each one. The goal is
not that you memorize the list — it's that when a duplicate delivery or a
poison message shows up at 3am, you already know exactly which line of code
absorbs it.

## The threat table

Distributed systems fail in enumerable ways. Here is everything this service
is designed to survive, and where the defense lives:

| Threat | What saves you | Section |
|---|---|---|
| Duplicate delivery of a consumed event | `processed_events` inbox insert, same transaction as the write | [Inbox](#at-least-once-delivery-and-the-inbox) |
| Out-of-order delivery (update before create, stale replay) | Full-state events + version guard in a SQL `WHERE` | [Versioned events](#versioned-full-state-events) |
| Consumer crash after commit, before ack | Redelivery replays the message; inbox makes it a no-op | [Inbox](#at-least-once-delivery-and-the-inbox) |
| Consumer crash before commit | Nothing committed; redelivery retries from scratch | [Micro-batching](#micro-batching-without-weaker-guarantees) |
| Concurrent API updates to the same row | Row lock (`get_for_update`) + optional `expected_version` → 409 | [API path](#api-path-idempotency-and-concurrency) |
| Replayed API create (client retry, ambiguous commit) | Idempotent insert keyed on client id; replay re-announces the event | [API path](#api-path-idempotency-and-concurrency) |
| Poison message (can never succeed) | Three validation layers + SQLSTATE classification → ack away | [Poison defense](#poison-message-defense-in-depth) |
| PostgreSQL rejects data at execute time | `is_permanent_data_error` classifies SQLSTATE class 22 → ack, not requeue loop | [Poison defense](#poison-message-defense-in-depth) |
| Broker publish fails after commit | Logged loudly with event id/type + correlation id; Outbox is the designed fix | [The honest gap](#the-honest-gap-commit-succeeds-publish-fails) |
| Shutdown mid-work | `BatcherClosedError` fails every pending submit → nack while the channel is still open | [Micro-batching](#micro-batching-without-weaker-guarantees) |

Everything below is the mechanism behind one or more of these rows.

## At-least-once delivery and the inbox

RabbitMQ, like every broker worth using, delivers **at least once**. There is
no configuration that makes duplicates impossible: if the consumer commits its
transaction and crashes before the ack reaches the broker, the broker — which
only knows the ack never arrived — redelivers. Duplicates are not a bug to
eliminate; they are a certainty to absorb.

The absorber is the inbox table `processed_events` in `app/database/inbox.py`:

```python title="app/database/inbox.py (the whole table)"
class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    source: Mapped[str] = mapped_column(String(255), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
```

The primary key is `(source, event_id)` — CloudEvents guarantees an event id
is unique *per source*, so the pair is globally unique. The crucial property
is **where** the insert happens: `SqlAlchemyUnitOfWork.mark_events_processed`
in `app/database/unit_of_work.py` runs on the *same session* — the same
transaction — as the module write. Either both commit or neither does. There
is no window where the data is written but the dedup record is missing, or
vice versa.

`mark_events_processed` is a single bulk statement for the whole batch:
`INSERT .. ON CONFLICT DO NOTHING .. RETURNING source, event_id`. The rows
that come back in `RETURNING` are the genuinely **new** deliveries; anything
absent from the result hit the primary key and is a duplicate.
`VersionedModuleService.apply_state_events`
(`app/modules/shared/service.py` — one generic implementation for every
module) then simply skips every item not in the `fresh` set.

!!! note "Why this scales horizontally"
    The dedup state lives in PostgreSQL, not in process memory. Run one
    consumer instance or twenty — a duplicate delivered to instance B after
    instance A processed the original still hits the same primary key. No
    sticky routing, no distributed cache, no coordination protocol.

## Versioned full-state events

The inbox handles *duplicates*. Ordering is a different problem: with
multiple producers, redeliveries, and parallel consumers, `user.updated` v3
can arrive before `user.created` v1, or a stale v2 can arrive after v3 was
applied.

The design answer: **every event carries the module's full state plus its
`version`**. Both `user.created` and `user.updated` use the same payload
schema — `UserData` in `app/modules/user.py`, which is simultaneously the
business model and the event payload — and the same generic handler
(`register_module_event_handlers` in `app/modules/shared/events.py`). An
event is not a delta to apply — it is an announcement of a complete state
you can adopt or ignore.

The consumer's write is `VersionedRepository.upsert_if_newer_many` in
`app/modules/shared/repository.py`: one atomic
`INSERT .. ON CONFLICT (id) DO UPDATE .. WHERE users.version < excluded.version`.
The decision table:

| Situation | Stored row | Result |
|---|---|---|
| Row missing (update arrived before create) | — | Inserted from the event's full state |
| `event.version > stored.version` | older | Overwritten with the event's state |
| `event.version <= stored.version` (stale / late create) | newer or equal | Silently skipped |

No row locks, no read-modify-write, no retry loop. The guard is a `WHERE`
clause **evaluated by PostgreSQL inside the statement** — concurrent
consumers racing on the same user cannot interleave between the read and the
write, because there is no separate read. Within a single batch,
`apply_state_events` pre-resolves races the same way: highest version per user
id wins before the statement is even built.

## API-path idempotency and concurrency

The API edge faces the mirror-image threats: client retries and concurrent
writers.

**Create is idempotent, keyed on the client-supplied id.**
`VersionedModuleService.create` calls `insert_if_absent` — an
`INSERT .. ON CONFLICT DO NOTHING .. RETURNING` in one round trip. Three
outcomes:

| Replay content vs stored row | HTTP | Behavior |
|---|---|---|
| First time seen | 201 | Insert, stage `user.created`, commit, publish |
| Identical (every field in `spec.mutable_fields` matches) | 200 | Return stored row **and re-announce** `user.created` |
| Contradictory (any field differs) | 409 | `ConflictError` — nothing written |
| Same id mid-insert by a concurrent request | 409 | momentary race window (`is being created concurrently`) — the retry after it resolves gets a 200 |

The re-announce on replay is deliberate and load-bearing. Suppose the first
create attempt died in the **ambiguous commit window**: the commit applied,
but the event never went out — either the commit call errored ambiguously
(the UnitOfWork logs that CRITICAL — see
[the honest gap](#the-honest-gap-commit-succeeds-publish-fails)) or the
process was killed between commit and publish (which logs nothing; a dead
process can't write a log line). Either way the client saw a failure or a
timeout, so it retries, gets a 200 — and `create` stages the stored
state event again. If the original event *did* go out, downstream consumers drop
the duplicate via the version guard; if it didn't, it is now recovered. A
harmless duplicate buys back a lost event.

**Updates take a real row lock plus optional optimistic concurrency.**
`VersionedModuleService.update` reads through `get_for_update` —
`SELECT .. FOR UPDATE` — so two concurrent PATCHes on the same user serialize
at the database rather than clobbering each other's `version` increment. On
top of that, a client may send `expected_version`; a mismatch raises
`ConflictError` → 409, letting the client detect that someone else got there
first. Every successful update increments `version` and stages a full-state
`user.updated` — which is exactly what feeds the consumer-side version guard
above.

## Micro-batching without weaker guarantees

One PostgreSQL commit per consumed message caps throughput at the database's
fsync rate. The fix is `Batcher` in `app/messaging/batcher.py`, which funnels
concurrent deliveries into single calls to `apply_state_events` — one
transaction per batch — without giving up a single delivery guarantee:

- **Greedy, never waits.** A flush takes only what is already queued. Idle
  traffic gets batches of one — zero added latency, no batch-fill timer.
  Batches grow only under backlog, because new deliveries queue while the
  previous batch's commit is in flight. Throughput scales exactly when it
  needs to.
- **Acks are strictly post-durability.** `Batcher.submit()` resolves its
  future only after the batch's transaction has COMMITTED. RabbitClient acks
  when the handler returns, and the handler returns when `submit()` resolves
  — so no message is ever acked before its data is durable. At-least-once,
  identical to unbatched consumption.
- **A failed batch is retried item-by-item.** `_flush` catches the batch
  failure and `_apply_individually` re-runs each item alone: the poison
  item's error propagates to dispatch, which classifies it (permanent → the
  message is acked away, transient → nack + redeliver; see the
  [poison defenses](#poison-message-defense-in-depth)), while the healthy
  rest commit and ack.
- **Crash mid-batch is safe by composition.** Nothing committed → every
  message in the batch redelivers → the inbox filters the duplicates and the
  version guard drops anything stale. Full redelivery is a no-op beyond the
  original effect. This is why the batcher's documented contract requires
  `apply_batch` to be all-or-nothing *and* idempotent — one transaction plus
  inbox plus version guard satisfies both.
- **Shutdown never hangs and never drops.** A closed batcher fails queued
  **and** in-flight submits with `BatcherClosedError` — a plain `Exception`,
  so RabbitClient's handler raises, and the messages **nack while the AMQP
  channel is still open**, guaranteeing redelivery. `close()` drains the
  queue itself (not only in the runner's `finally`) because cancelling a task
  that never got its first step skips the coroutine body entirely — a subtle
  asyncio trap that would otherwise leave futures pending forever, handlers
  hung, and messages neither acked nor nacked.

!!! warning "The contract you inherit"
    If you build a new module ([Adding a Module](05-adding-a-module.md)),
    your batch-apply function must keep this contract: one transaction, inbox
    insert inside it, version-guarded upsert. Break any leg and "failed batch
    retried item-by-item" or "crash = harmless replay" stops being true.

## Poison-message defense in depth

A message that can *never* succeed must be acked away, not requeued — a
requeue loop on deterministic failure is an outage with extra CPU. The
defense has three layers, each catching what the previous one cannot:

**Layer 1 — envelope bounds** (`app/messaging/cloudevents.py`). CloudEvent
`id`, `source`, and `type` are bounded to 255 characters and rejected if they
contain NUL. Neither limit is arbitrary: the values land verbatim in
`processed_events`' `String(255)` text columns, and PostgreSQL rejects NUL in
text. Without this layer, an oversized or NUL-bearing id would pass decoding
and then fail the inbox INSERT *on every redelivery, forever*. Instead it is
an invalid envelope: logged and acked away at the top of
`EventConsumer`'s handler.

**Layer 2 — payload floors** (`UserData` in `app/modules/user.py` +
`app/modules/shared/validation.py`). The module's `Data` model — which IS
the event payload — enforces storability — no NUL bytes anywhere, no
NaN/Infinity in `attributes` (both are things PostgreSQL deterministically
rejects at execute time; see `app/database/storable.py`) — plus a minimal
shape floor, all declared with the shared Annotated types from
`modules/shared/validation.py`. Note the **deliberate asymmetry** on email,
now expressed structurally: the strict rule lives only in the API schemas
(`UserCreate`/`UserUpdate` use `StrictEmail`, exactly pydantic's `EmailStr`
rule — a client gets a 422 and can fix it), while `UserData` uses the
permissive `FloorEmail` (storable + contains `@`, stored **verbatim**), so
the consumer path validates straight into the business model with no
bridging step.
Why: events are full-state announcements from an authoritative producer, and
rejected payloads are *acked away*. Reject one over email syntax and every
later event for that user carries the same email — the replica is frozen at
the previous version **forever**. Replication fidelity beats re-adjudicating
validity. To be precise about what the consumer floor still rejects: NUL and
NaN/Infinity (unstorable, period), values exceeding their column widths
(equally unstorable — the bounds mirror the `String(...)` columns), and a
minimal shape floor (non-blank name, an `@` in the email, `version >= 1`).
Nothing beyond that.

**Layer 3 — SQLSTATE classification** (`app/database/errors.py`). If
something unstorable still slips through to PostgreSQL, the resulting
exception is classified by `is_permanent_data_error`, which walks the driver
exception chain (`exc.orig`, then `__cause__` links) looking for a SQLSTATE
whose class is `22` — "data exception", deterministic by definition. It does
**not** rely on catching `sqlalchemy.exc.DataError`, and this is the part
most people get wrong: the asyncpg dialect leaves most class-22 server
errors untranslated, surfacing them as the generic `DBAPIError`. A
class-based `except DataError:` compiles, passes review, and **silently
never fires** — turning one bad value into an infinite requeue loop. An
integration test pins the real driver behavior so this can't regress
unnoticed.

One honest boundary: the permanent classification is a deliberate
**whitelist** (invalid envelope, unknown type, `ValidationError`, SQLSTATE
class 22) — everything else defaults to transient and requeues. A
deterministic error *outside* the whitelist — say, a class-23 constraint
violation introduced by a future schema change (a foreign key consumed data
can violate) — would requeue-loop. Today's schema has no such constraint; if
you add one, extend the classification in the same PR. And note there is no
dead-letter queue: a permanently rejected message is acked and gone, its
envelope identifiers in the logs being the only trace.

Where the classification *lives* matters as much as what it does. It sits in
`EventConsumer._handler_for`'s dispatch (`app/messaging/consumer.py`), **not**
in module handlers. Handlers just validate (let `ValidationError` propagate)
and write (let database errors propagate); dispatch decides ack vs requeue.
Every module you ever add is poison-safe by construction, with zero
ack/nack code of its own.

The whole taxonomy rests on RabbitClient's two-word contract
(`app/messaging/rabbit_client_adapter.py`): handler **return = ack**, handler
**raise = nack + requeue**. So:

| Failure | Classified as | Dispatch does |
|---|---|---|
| Invalid envelope, unknown event type, `ValidationError`, SQLSTATE class 22 | Permanent | log + `return` → acked away |
| DB down, timeout, serialization failure, `BatcherClosedError` | Transient | `raise` → nack → redelivered |

## The honest gap: commit-succeeds-publish-fails

One guarantee this service does **not** have today: an API write whose
transaction commits but whose event publish then fails (broker down, network
blip) has committed state with no event on the wire. Pretending otherwise
would be worse than the gap itself, so here is exactly how it behaves
(`SqlAlchemyUnitOfWork.commit` in `app/database/unit_of_work.py`):

- The publish failure is caught, **logged loudly with the event id, event
  type, and correlation id** (never the payload — logs are value-free by
  policy), and the request does *not* fail — the commit already happened; the
  client's write is real and must not be reported as an error.
- Cancellation mid-publish (client disconnect, shutdown) is a separate,
  nastier case: it is a `BaseException`, not an `Exception`, and would
  otherwise vanish silently. It is logged **CRITICAL with the ids of every
  not-yet-published staged event**, then re-raised. The commit stands; the
  loss is never silent.
- Recovery is **asymmetric** until the Outbox lands: a replayed create
  re-announces the stored state event (see
  [API path](#api-path-idempotency-and-concurrency)), so lost `user.created`
  events are recoverable by client retry. A lost `user.updated` has **no**
  re-announce path — it stays lost until a later update supersedes it with
  newer full state. Be honest about the retry's trigger, though: a client
  retries only when *it* saw a failure or timeout. When the publish fails
  after the client already received its success response, nothing prompts a
  retry — in that case creates and updates are equally lost until the Outbox
  lands, and the ERROR log is your only signal.

The designed fix is the transactional Outbox, and the architecture is already
shaped for it: events are staged in the UnitOfWork and delivered through the
`EventPublisher` port strictly after commit. An `OutboxPublisher` that
inserts staged events into an `outbox` table *on the same session*, plus a
relay process, closes the gap — and **only bootstrap changes**; no business
code is touched. Until then: the gap is known and bounded to the publish
step. A publish *failure* is always observable in the logs
([Operations](07-operations.md) covers what to watch for); the one silent
case is a hard kill inside the window, which no process can log — that case
is exactly what create's re-announce-on-retry recovers.

## Privacy note

Rejection logs never contain payload values. Pydantic's `str(exc)` embeds
`input_value=...` — the rejected names and emails — so every rejection path
goes through `validation_error_reason` (`app/messaging/cloudevents.py`),
which surfaces **field locations and error messages only**. Even the
`specversion` check is a `Literal` field precisely so an unsupported version
takes the same value-free path. Envelope identifiers (event id, type) **are**
logged — deliberately: they are operational metadata an operator needs to
act on a rejection, not user data.

## See it live

Every mechanism in this chapter is pinned, for **every** registered module,
by the parametrized contract suite — run it and read the test names:

```bash
.venv/bin/python -m pytest tests/module_contract -q
```

| Contract test | Guarantee demonstrated |
|---|---|
| `test_create_replay_returns_200_and_reannounces` | Idempotent create + ambiguous-commit recovery |
| `test_create_contradictory_returns_409` | Contradictory replay detection |
| `test_patch_expected_version_conflict_returns_409` | Row lock + optimistic concurrency |
| `test_duplicate_delivery_is_noop` | The inbox |
| `test_out_of_order_apply` / `test_within_batch_highest_version_wins` | The version guard |
| `test_event_payload_field_set_equals_data_model_fields` | Payload shape stability (no timestamp leaks) |

For the hands-on curl/broker version of the same experiments — watching the
log say `duplicates: 1, written: 0` yourself — see
[Adding a Module](05-adding-a-module.md) **Step 5**.
