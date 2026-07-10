"""In-memory fake client for testing the harness without a broker."""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque

from benchmark.clients.base import BenchmarkClient


class FakeClient(BenchmarkClient):
    name = "fake"

    def __init__(self) -> None:
        self._queues: dict[str, deque[bytes]] = defaultdict(deque)

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def declare_queue(self, name: str) -> None:
        self._queues.setdefault(name, deque())

    async def purge_queue(self, name: str) -> None:
        self._queues[name].clear()

    async def delete_queue(self, name: str) -> None:
        self._queues.pop(name, None)

    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None:
        self._queues[routing_key].append(body)

    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None:
        q = self._queues[queue]
        if q:
            return q.popleft()
        await asyncio.sleep(0)
        return None

    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None:
        self._queues[routing_key].extend(bodies)

    async def consume_many(self, queue: str, count: int) -> int:
        q = self._queues[queue]
        consumed = 0
        while q and consumed < count:
            q.popleft()
            consumed += 1
        return consumed

    async def server_version(self) -> str | None:
        return "fake-1.0"
