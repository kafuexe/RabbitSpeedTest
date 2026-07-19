"""Fast, verified queue preload for consume-side benchmark setup.

Setup is untimed but still costs wall-clock: publishing with confirms pays a
broker round-trip per message (pika drops to ~3k msg/s). Publish without
confirms instead, then poll the broker's queue depth until every message is
reported queued — same delivery guarantee for the benchmark, ~10x faster.
"""
from __future__ import annotations

import asyncio
import time

from benchmark.clients.base import BenchmarkClient
from benchmark.config import BenchmarkConfig

_PRELOAD_TIMEOUT = 60.0


async def preload(client: BenchmarkClient, config: BenchmarkConfig, bodies: list[bytes]) -> None:
    await client.purge_queue(config.queue_name)
    await client.publish_many(config.exchange, config.routing_key, bodies, confirm=False)
    target = len(bodies)
    deadline = time.monotonic() + _PRELOAD_TIMEOUT
    while True:
        depth = await client.queue_depth(config.queue_name)
        if depth >= target:
            return
        if time.monotonic() > deadline:
            raise RuntimeError(f"preload stalled: {depth}/{target} messages queued")
        await asyncio.sleep(0.02)
