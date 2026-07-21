"""The ONLY module that touches the `RabbitClient` library.

`hs_rabbit_client` is the `hs-rabbit-client` package from `../rabbit-client-python`,
wired as a uv path dependency. Adapts it to the MessagePublisher /
MessageConsumer ports. Reconnects, declares, confirms, ack/requeue
semantics and broker-cancel recovery are all delegated to the client:
handler return = ack, handler raise = nack+requeue, and a broker-side
Basic.Cancel (queue deleted) is recovered INSIDE the library (WARNING log,
short backoff, re-declare + resume) — it never surfaces here.
"""
from __future__ import annotations

from hs_rabbit_client import RabbitClient

from app.messaging.protocols import MessageHandler

# This module stays the ONE import seam over the client library, so the rest
# of the app never imports `hs_rabbit_client` directly.
__all__ = ["RabbitClientAdapter"]


class RabbitClientAdapter:
    def __init__(self, amqp_url: str, *, prefetch: int, persistent: bool) -> None:
        self._client = RabbitClient(amqp_url, prefetch=prefetch, durable=persistent)

    async def connect(self) -> None:
        await self._client.connect()

    async def close(self) -> None:
        await self._client.close()

    def is_connected(self) -> bool:
        return self._client.is_connected

    async def publish(self, queue: str, body: bytes) -> None:
        await self._client.publish(queue, body)

    async def consume(self, queue: str, handler: MessageHandler) -> None:
        # The port's contract is "park until cancelled". Since 0.2.0 the
        # client returns a Consumer handle instead of parking, so park on
        # wait() and — in the finally — cancel the broker-side consumer when
        # this coroutine's task is cancelled (CancelledError still
        # propagates after the finally).
        consumer = await self._client.consume(queue, handler)
        try:
            await consumer.wait()
        finally:
            await consumer.cancel()
