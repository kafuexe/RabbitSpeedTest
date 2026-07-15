"""RabbitMQ consumer edge.

Decodes CloudEvents and dispatches to registered handlers. Failure taxonomy
under SimpleClient semantics (return = ack, raise = nack+requeue):

- PERMANENT (invalid envelope, unknown event type): log + return → the
  message is acked away, never poison-looped. "Unknown events are logged and
  rejected."
- TRANSIENT (DB down, timeouts): the exception propagates → requeue → retry.

Business-level permanent failures (bad payload, stale version, duplicate) are
handled inside the module handlers, which likewise return normally.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from app.logging.correlation import set_correlation_id
from app.messaging.cloudevents import CloudEvent, InvalidCloudEvent
from app.messaging.protocols import MessageConsumer, MessageHandler
from app.messaging.registry import EventHandlerRegistry

logger = logging.getLogger(__name__)


class EventConsumer:
    def __init__(
        self,
        bus: MessageConsumer,
        registry: EventHandlerRegistry,
        queues: Sequence[str],
        *,
        retry_delay: float = 5.0,
    ) -> None:
        self._bus = bus
        self._registry = registry
        self._queues = list(queues)
        self._retry_delay = retry_delay

    async def run(self) -> None:
        """Consume all configured queues concurrently until cancelled.

        Each queue is independently supervised: a failing queue (deleted,
        channel error, policy change) is logged and retried without touching
        the other queues, and never silently stays dead.
        """
        logger.info("consumer starting", extra={"queues": self._queues})
        tasks = [
            asyncio.create_task(self._consume_forever(q), name=f"consume:{q}")
            for q in self._queues
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _consume_forever(self, queue: str) -> None:
        handler = self._handler_for(queue)
        while True:
            try:
                await self._bus.consume(queue, handler)
                return  # cooperative stop (only fakes return; real consume parks)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "consume failed; retrying",
                    extra={"queue": queue, "retry_in_s": self._retry_delay},
                )
                await asyncio.sleep(self._retry_delay)

    def _handler_for(self, queue: str) -> MessageHandler:
        async def handle(body: bytes) -> None:
            try:
                event = CloudEvent.from_bytes(body)
            except InvalidCloudEvent as exc:
                logger.warning(
                    "invalid CloudEvent rejected",
                    extra={"queue": queue, "reason": str(exc)},
                )
                return

            set_correlation_id(event.correlationid or event.id)
            handler = self._registry.get(event.type)
            if handler is None:
                logger.warning(
                    "unknown event type rejected",
                    extra={"queue": queue, "event_type": event.type, "event_id": event.id},
                )
                return
            await handler(event)

        return handle
