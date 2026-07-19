"""Generic state-event plumbing shared by all modules.

Modules keep what is genuinely theirs — the payload schema (permissive, see
modules/shared/validation.py) and the event-type names — and delegate the
envelope building and handler registration here.
"""
from __future__ import annotations

import uuid
from typing import Sequence, TypeVar

from pydantic import BaseModel

from app.logging.correlation import get_correlation_id
from app.messaging.batcher import Batcher
from app.messaging.cloudevents import CloudEvent, now_utc
from app.messaging.registry import EventHandlerRegistry
from app.modules.shared.service import StateEventItem

D = TypeVar("D", bound=BaseModel)


def build_state_event(
    event_type: str, payload: BaseModel, *, source: str
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


def register_state_event_handlers(
    registry: EventHandlerRegistry,
    batcher: Batcher[StateEventItem[D]],
    *,
    event_types: Sequence[str],
    payload_model: type[BaseModel],
    data_type: type[D],
) -> None:
    """Register one full-state handler for every event type.

    Handlers validate (ValidationError propagates — EventConsumer's dispatch
    classifies it permanent and acks) and submit to the greedy batcher;
    submit() returns — and the message is acked — only once the item's batch
    has committed. Storage rejections likewise propagate and are classified
    by dispatch, so no module owns ack/nack policy.

    `data_type` must be a pydantic model whose fields all exist on the
    validated payload; pass a custom handler via `registry.register` directly
    when a module needs a different mapping.
    """
    field_names = list(data_type.model_fields)

    async def apply_state_event(event: CloudEvent) -> None:
        payload = payload_model.model_validate(event.data)
        # model_construct, NOT __init__: the payload model has already run
        # this path's rules — the PERMISSIVE consumer floor. The business
        # Data model's own rules are the STRICT API-ingress ones (e.g.
        # StrictEmail), and re-running them here would re-adjudicate a
        # consumed event and freeze the replica — exactly the failure the
        # deliberate asymmetry in modules/shared/validation.py exists to
        # prevent. Values pass through VERBATIM.
        await batcher.submit(
            StateEventItem(
                event_id=event.id,
                source=event.source,
                data=data_type.model_construct(
                    **{name: getattr(payload, name) for name in field_names}
                ),
            )
        )

    for event_type in event_types:
        registry.register(event_type, apply_state_event)
