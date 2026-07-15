"""Event-type → handler registry. Modules register their handlers at
bootstrap; the consumer dispatches through it."""
from __future__ import annotations

from typing import Awaitable, Callable

from app.messaging.cloudevents import CloudEvent

EventHandler = Callable[[CloudEvent], Awaitable[None]]


class EventHandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, EventHandler] = {}

    def register(self, event_type: str, handler: EventHandler) -> None:
        if event_type in self._handlers:
            raise ValueError(f"handler already registered for {event_type!r}")
        self._handlers[event_type] = handler

    def get(self, event_type: str) -> EventHandler | None:
        return self._handlers.get(event_type)
