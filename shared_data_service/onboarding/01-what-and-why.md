# What This Service Is (and Why)

This is the first real page of the guide, written for engineering managers and
new maintainers. No code here — just what the service is, what it promises,
what it costs to operate, and the one gap you should know about before anyone
else tells you.

## What it is

The Shared Data Service is the **authoritative storage service** for shared
application data. PostgreSQL is the system of record — if a fact isn't
committed there, it doesn't count. Data arrives on two paths:

1. **A REST API** for interactive clients — create, read, update, list.
2. **RabbitMQ CloudEvents** from other services — the same writes, arriving as
   messages instead of HTTP calls.

Both paths run the *same* business logic. And every successful API commit
announces the new state as an event on an outbound queue, so any interested
service can keep its own replica current without ever asking this one.

## Why it exists

The problem class: several services need one consistent view of shared
entities (users today; more later). The naive answers are all bad — every
service holding its own copy drifts; every service reading one shared database
couples them all to one schema and one failure domain; ad-hoc sync scripts rot.

This service replaces that with a single owner. One place writes and validates
the data; everyone else either calls the API or subscribes to the event stream
and maintains a replica. Producers and consumers never touch each other's
databases, and the event stream is designed so that replicas *cannot* drift
even when the network misbehaves.

## The guarantees, in business language

Each of these is a designed property with tests behind it, not an aspiration.
The mechanics live in the [Reliability Model](04-reliability-model.md).

| Guarantee | Plain English | How |
|---|---|---|
| Retries never duplicate data | A client that times out and retries a create gets the already-stored row back, not a second copy. | Idempotent create keyed on the client-supplied id — [Reliability Model](04-reliability-model.md) |
| Duplicate or out-of-order events can't corrupt state | An event delivered twice, late, or in the wrong order is recognized and skipped; the newest state always wins. | Inbox dedup table + versioned full-state events — [Reliability Model](04-reliability-model.md) |
| A crashed instance loses nothing committed | Messages are acknowledged only after their data is durable in PostgreSQL; a crash mid-work means redelivery, and redelivery is harmless (see the row above). | Ack strictly after commit, at-least-once delivery — [Reliability Model](04-reliability-model.md) |
| Bad messages can't wedge the pipeline | A malformed or unstorable message is logged (its identifiers, never its content) and removed from the queue; it never blocks the messages behind it in an endless retry loop. There is no holding pen — a rejected message is discarded, and the log line is how you find the misbehaving producer. | Permanent-vs-transient classification at the dispatch layer — [Reliability Model](04-reliability-model.md) |
| Concurrent edits can't silently overwrite each other | Two clients editing the same record at once is detected; the loser gets a conflict response, not silent data loss. | Row locks + optimistic version checks — [Reliability Model](04-reliability-model.md) |
| Any number of instances can run | Scaling out needs no leader election and no sticky sessions. | Stateless processes; all coordination state lives in PostgreSQL — [Reliability Model](04-reliability-model.md) |

## Operational maturity — what exists today

- **Liveness and readiness endpoints**, built so a dead event consumer can
  never look healthy: any consumer stop — crash *or* quiet exit — fires a
  critical log and flips readiness to false. There is no state where events
  silently stop flowing while health checks stay green.
- **JSON structured logs** with a correlation id on every line, propagated
  across service boundaries: an id arriving on an HTTP request or inside a
  consumed event follows the work through the API, the business layer, and
  back out on published events.
- **Unit and integration test suites** — unit tests run against fakes;
  integration tests run against real PostgreSQL and RabbitMQ and auto-skip
  when those aren't available. See [Testing](06-testing.md).
- **A measured benchmark suite** covering database throughput, API latency
  percentiles, end-to-end event latency, and one-process versus multi-process
  scaling, with results checked into the repo.
- **Horizontal scalability**, demonstrated by that benchmark: on the dev
  machine, four API processes deliver roughly eight times the write
  throughput of one.

This is the factual state of the repository today, not a roadmap. The
[Operations Runbook](07-operations.md) shows how to run and watch it.

## The one known gap

There is a narrow window in which a database commit succeeds but publishing
the resulting event fails (for instance, the broker drops at exactly that
moment): the data is safe, but that one announcement can be lost — the
failure is logged loudly with the event's identifiers, and the exposure is
bounded to that window. The permanent fix is already designed — a
transactional Outbox that stores events in the same database transaction as
the data, changing only the composition root — and until it lands, this
paragraph is where maintainers learn the gap exists, rather than in an
incident review.

## Cost of change

Adding a new module type is one new module file (its model, data shapes,
schemas, and routes in one place), one line in the module registry
(`ALL_SPECS`), one test-fixtures entry, and a generated database migration.
Nothing cross-cutting changes: no edits to the composition root, the API
assembly, the messaging layer, the transaction machinery, or existing
modules — they all iterate the registry. The whole procedure is templated
step-by-step in [Adding a Module](05-adding-a-module.md).

## Where next

[Setup](02-setup.md) gets it running locally;
[Architecture Tour](03-architecture-tour.md) walks the layers;
[Maintenance Contract](08-maintenance.md) states what future maintainers must
preserve.
