"""RabbitMQ consumer edge.

Decodes CloudEvents and dispatches to registered handlers. Failure taxonomy
under RabbitClient semantics (return = ack, raise = nack+requeue):

- PERMANENT (invalid envelope, unknown event type, invalid payload, data the
  database deterministically rejects): log + return → the message is acked
  away, never poison-looped.
- TRANSIENT (DB down, timeouts, batcher shutting down): the exception
  propagates → requeue → retry.

The permanent/transient classification lives HERE, at the dispatch layer,
not in module handlers — so every module (current and future) is poison-safe
by construction: handlers just validate (raise ValidationError) and write
(raise whatever the database raises); dispatch decides ack vs requeue.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from pydantic import ValidationError

from app.database.errors import is_permanent_data_error
from app.logging.correlation import set_correlation_id
from app.messaging.cloudevents import (
    CloudEvent,
    InvalidCloudEvent,
    validation_error_reason,
)
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
                # event.type/id are envelope identifiers, logged as
                # operational metadata; payload values are never logged.
                logger.warning(
                    "unknown event type rejected",
                    extra={"queue": queue, "event_type": event.type, "event_id": event.id},
                )
                return
            try:
                await handler(event)
            except ValidationError as exc:
                # Invalid payload: permanent. Log without input values (PII).
                logger.warning(
                    "event payload rejected",
                    extra={"queue": queue, "event_type": event.type,
                           "event_id": event.id,
                           "reason": validation_error_reason(exc)},
                )
                return
            except Exception as exc:
                if is_permanent_data_error(exc):
                    # The database deterministically rejected the data
                    # (SQLSTATE class 22): retrying can never succeed, so
                    # ack it away instead of requeue-looping.
                    logger.warning(
                        "unstorable event rejected",
                        extra={"queue": queue, "event_type": event.type,
                               "event_id": event.id,
                               "reason": type(exc).__name__},
                    )
                    return
                raise  # transient → nack → redeliver

        return handle
