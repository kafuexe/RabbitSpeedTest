"""Abstract benchmark client interface and payload generation."""
from __future__ import annotations

import abc

# Drains return a short count after this much queue silence; callers verify totals.
CONSUME_INACTIVITY_TIMEOUT = 5.0


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

    # Note on `confirm`: it can only opt OUT of confirms. A client built with
    # publisher_confirms=False has no confirm channel, so confirm=True on it
    # is silently best-effort — construct with confirms on if you need them.
    @abc.abstractmethod
    async def publish(self, exchange: str, routing_key: str, body: bytes, *, confirm: bool) -> None: ...

    @abc.abstractmethod
    async def consume_one(self, queue: str, timeout: float = 5.0) -> bytes | None: ...

    @abc.abstractmethod
    async def publish_many(self, exchange: str, routing_key: str, bodies: list[bytes], *, confirm: bool) -> None: ...

    @abc.abstractmethod
    async def consume_many(self, queue: str, count: int) -> int: ...

    async def consume_many_get(self, queue: str, count: int) -> int:
        """Drain via repeated single-message gets (basic.get) instead of a
        push consumer. Kept separate so get-vs-push can be benchmarked
        side by side; real clients override with their native get loop.
        """
        consumed = 0
        while consumed < count:
            if await self.consume_one(queue) is None:
                break
            consumed += 1
        return consumed

    def clone(self) -> "BenchmarkClient":
        """Fresh, unconnected client with identical settings, for concurrent
        workers. Subclasses set self._url and self._clone_kwargs in their
        constructor (one listing of settings, no hand-mirrored clone methods);
        in-memory fakes override this to share state instead.
        """
        return type(self)(self._url, **self._clone_kwargs)

    @abc.abstractmethod
    async def queue_depth(self, name: str) -> int: ...

    @abc.abstractmethod
    async def server_version(self) -> str | None: ...
