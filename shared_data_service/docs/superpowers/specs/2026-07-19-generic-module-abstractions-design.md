# Generic module abstractions for shared_data_service

**Status:** implemented in this branch. Written during an autonomous background
run; the draft PR is the review gate for this design.

## Problem

Every module carries a private copy of machinery that is not domain-specific:

- `repository.py`: idempotent `insert_if_absent`, version-guarded
  `upsert_if_newer_many`, whitelisted filter/sort/paginate `list` — all of it
  parameterized only by the table and its columns.
- `business.py`: the full service choreography — idempotent create with replay
  re-announce, optimistic update, batched idempotent event application — with
  only the field names differing per module.
- `events.py`: handler registration that validates a payload and submits a
  `(event_id, source, data)` item to a batcher.

The onboarding chapter "Adding a Module" is the proof: a second module is
~600 lines of copy-paste. Copy-paste is how the guarantees drift.

## Approaches considered

1. **Helper functions only** — extract `apply_list_query` etc. Least invasive,
   but the service choreography (the hardest code to get right) stays
   duplicated.
2. **Generic base classes + small per-module hooks** — repository, service and
   event-registration bases in `app/modules/shared/`; a module declares its
   model, whitelists, validation, and event building. Every method remains
   overridable ("editable if needed"). **Chosen.**
3. **Config-driven module framework** — declarative descriptor including
   container wiring. Too magical for a codebase that prizes explicitness;
   YAGNI.

## Design

New shared units (all in existing dependency direction; no new edges):

### `app/modules/shared/repository.py` — `VersionedRepository[M]`

Subclass declares `model`, `filterable_columns`, `sortable_columns`.
Provides `get`, `get_for_update`, `insert_if_absent`, `upsert_if_newer_many`
(version guard as SQL `WHERE`), and the two-query `list`. Row payloads are
derived from the table: every mapped column without a server default /
`onupdate` (i.e. everything except `created_at`/`updated_at`). `_row_values`
is overridable for models that need something else.

### `app/modules/shared/service.py` — `VersionedEntityService[M, D, C]`

Same constructor as today's `UserService`. Generic public API: `create`,
`update`, `get`, `list_page`, `apply_state_events`. A module subclass
declares `entity_name`, `created_event_type`, `updated_event_type`,
`default_sort`, `sortable_fields`, `filterable_fields`, and implements four
small hooks:

- `_new_entity(data) -> M` — ORM instance from full state (honors
  `data.version`; `create` then forces version 1).
- `_content_matches(entity, data) -> bool` — replay equality.
- `_build_event(event_type, entity) -> CloudEvent`.
- `_validate_data` / `_validate_changes` — business-floor validation
  (default no-op).

`_apply_changes` has a generic default (set every non-None dataclass field,
copying dicts) and is overridable. `StateEventItem[D]` replaces per-module
event-item dataclasses. Log messages keep their shape; per-entity log keys
become generic (`entity_id`, `written`) with `entity` naming the module.

### `app/modules/shared/events.py`

`build_state_event(event_type, payload, *, source)` (CloudEvent envelope +
correlation id) and `register_state_event_handlers(registry, batcher, *,
event_types, payload_model, data_type)` (validate → submit; ack/nack policy
stays in the dispatch layer, as before).

### The user module after the refactor

- `repository.py`: model + whitelists only.
- `business.py`: `UserData`/`UserChanges`, `UserService` subclass with hooks.
- `events.py`: payload schema + builder + one-call registration.
- `router.py`/`schemas.py`: unchanged apart from the generic service method
  names (`create`, `get`, `update`, `list_page`).

Container gains a `self._batchers` list so `start()`'s closed-batcher check
and `stop()`'s close loop no longer need widening per module.

## Behavior contract (unchanged)

All existing unit tests keep their assertions; only service method names are
renamed at call sites. Idempotent create/replay/conflict semantics, optimistic
versioning, batched event application (dedup, highest-version-wins,
out-of-order, stale-skip), publish-after-commit and never-republish are
byte-for-byte the same choreography, now written once.

## Error handling & testing

Same error taxonomy (`app/modules/shared/errors.py`), raised from the shared
base with `entity_name` in messages — strings preserved for the user module.
Unit suite must pass unmodified in behavior; integration suite unchanged.
Onboarding docs (notably 05-adding-a-module) are rewritten to the new, much
shorter recipe, per the maintenance contract.
