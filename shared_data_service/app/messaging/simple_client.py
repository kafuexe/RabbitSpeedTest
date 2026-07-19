"""The ONLY module that touches the existing SimpleClient (`SimpleRabbit`).

Adapts it to the MessagePublisher / MessageConsumer ports. Reconnects,
declares, confirms and ack/requeue semantics are all delegated to the client:
handler return = ack, handler raise = nack+requeue.
"""
from __future__ import annotations

try:
    # Monorepo checkout: prefer the canonical copy at the repo root.
    from simple_rabbit import ConsumerCancelledError, SimpleRabbit
except ImportError:
    # Standalone install (pip install ., Docker, CI): the byte-identical
    # vendored copy ships inside the app package. A unit test asserts the
    # two files never drift.
    from app.messaging._vendored_simple_rabbit import (  # noqa: F401
        ConsumerCancelledError,
        SimpleRabbit,
    )

from app.messaging.protocols import MessageHandler

# This module is the ONE import seam over the dual-sourced client: only one
# of the two module copies is live per process, so service code must take
# BOTH names from here — importing ConsumerCancelledError from either source
# module directly would silently not match in the other deployment shape.
__all__ = ["ConsumerCancelledError", "SimpleClientAdapter"]


class SimpleClientAdapter:
    def __init__(self, amqp_url: str, *, prefetch: int, persistent: bool) -> None:
        self._client = SimpleRabbit(amqp_url, prefetch=prefetch, durable=persistent)

    async def connect(self) -> None:
        await self._client.connect()

    async def close(self) -> None:
        await self._client.close()

    def is_connected(self) -> bool:
        return self._client.is_connected

    async def publish(self, queue: str, body: bytes) -> None:
        await self._client.publish(queue, body)

    async def consume(self, queue: str, handler: MessageHandler) -> None:
        await self._client.consume(queue, handler)
