"""Generic state-event plumbing shared by all modules.

The strict-at-API / permissive-at-events asymmetry is structural: a
module's `*Data` model IS the event payload and carries only the
PERMISSIVE floor (see modules/shared/validation.py), while strictness
lives in the `*Create`/`*Update` API schemas. Consumed payloads therefore
validate straight into `Data` — no bypass machinery.
"""
from __future__ import annotations

import uuid

from app.logging.correlation import get_correlation_id
from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent, now_utc
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.spec import D, EntitySpec, M, StateData, StateEventItem, U


def build_state_event(
    event_type: str, payload: StateData, *, source: str
) -> CloudEvent:
    """Wrap a validated full-state payload in a CloudEvent envelope."""
    return CloudEvent(
        id=str(uuid.uuid4()),
        source=source,
        type=event_type,
        time=now_utc(),
        data=payload.model_dump(mode="json"),
        correlationid=get_correlation_id(),
    )


def register_entity_event_handlers(
    spec: EntitySpec[M, D, U],
    registry: EventHandlerRegistry,
    batcher: Batcher[StateEventItem[D]],
) -> None:
    """Register the created/updated full-state handler for a spec.

    The handler validates the payload into the spec's Data model
    (ValidationError propagates — EventConsumer's dispatch classifies it
    permanent and acks; only genuinely unstorable or shapeless data is
    rejected, values are stored VERBATIM) and submits to the greedy
    batcher; submit() returns — and the message is acked — only once the
    item's batch has committed. Storage rejections likewise propagate and
    are classified by dispatch, so no module owns ack/nack policy.

    For event types beyond created/updated, register a custom handler via
    `registry.register` directly (see the extra_event_handlers hook in
    modules/shared/wiring.py).
    """

    async def apply_state_event(event: CloudEvent) -> None:
        await batcher.submit(
            StateEventItem(
                event_id=event.id,
                source=event.source,
                data=spec.data.model_validate(event.data),
            )
        )

    registry.register(spec.created_event_type, apply_state_event)
    registry.register(spec.updated_event_type, apply_state_event)
