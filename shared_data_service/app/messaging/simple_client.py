"""The ONLY module that touches the existing SimpleClient (`SimpleRabbit`).

Adapts it to the MessagePublisher / MessageConsumer ports. Reconnects,
declares, confirms and ack/requeue semantics are all delegated to the client:
handler return = ack, handler raise = nack+requeue.
"""
from __future__ import annotations

from simple_rabbit import SimpleRabbit

from app.messaging.protocols import MessageHandler


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
