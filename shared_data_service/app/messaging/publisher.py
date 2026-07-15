"""EventPublisher implementations.

QueueEventPublisher — real delivery to the configured outbound queue (API graph).
NullEventPublisher — structurally guarantees the consumer graph never
republishes: consumer-side units of work are wired with this one.
"""
from __future__ import annotations

import logging

from app.messaging.cloudevents import CloudEvent
from app.messaging.protocols import MessagePublisher

logger = logging.getLogger(__name__)


class QueueEventPublisher:
    def __init__(self, bus: MessagePublisher, queue: str) -> None:
        self._bus = bus
        self._queue = queue

    async def publish_event(self, event: CloudEvent) -> None:
        await self._bus.publish(self._queue, event.to_bytes())
        logger.info(
            "event published",
            extra={"event_id": event.id, "event_type": event.type, "queue": self._queue},
        )


class NullEventPublisher:
    async def publish_event(self, event: CloudEvent) -> None:
        logger.debug(
            "event suppressed (consumer path never republishes)",
            extra={"event_id": event.id, "event_type": event.type},
        )
