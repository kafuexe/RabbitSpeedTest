"""Abstract benchmark client interface and payload generation."""
from __future__ import annotations

import abc


def generate_payloads(sizes: dict[str, int]) -> dict[str, bytes]:
    """Pre-generate a reusable byte payload for each size label."""
    return {label: b"x" * n for label, n in sizes.items()}


class BenchmarkClient(abc.ABC):
    """Uniform async interface both pika and aio-pika implement.

    Sync clients (pika) wrap blocking calls in a thread executor so the
    runner can drive every client through one async code path.
    """

    name: str = "base"

    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def declare_queue(self, name: str) -> None: ...

    @abc.abstractmethod
    async def purge_queue(self, name: str) -> None: ...

    @abc.abstractmethod
    async def delete_queue(self, name: str) -> None: ...

    @abc.abstractmethod
    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None: ...

    @abc.abstractmethod
    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None: ...

    @abc.abstractmethod
    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None: ...

    @abc.abstractmethod
    async def consume_many(self, queue: str, count: int) -> int: ...

    @abc.abstractmethod
    async def server_version(self) -> str | None: ...
