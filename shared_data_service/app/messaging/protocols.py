"""Messaging ports. Everything above the wire depends on these, never on
SimpleClient directly, so tests substitute fakes and the transport can change
without touching business code."""
from __future__ import annotations

from typing import Awaitable, Callable, Protocol

from app.messaging.cloudevents import CloudEvent

MessageHandler = Callable[[bytes], Awaitable[None]]


class MessagePublisher(Protocol):
    async def publish(self, queue: str, body: bytes) -> None: ...


class MessageConsumer(Protocol):
    async def consume(self, queue: str, handler: MessageHandler) -> None:
        """Run until cancelled. Handler return = ack; handler raise = requeue."""
        ...


class EventPublisher(Protocol):
    """Where committed domain events go. Swapping the implementation for an
    outbox writer is the designed extension point — business code never
    changes."""

    async def publish_event(self, event: CloudEvent) -> None: ...
