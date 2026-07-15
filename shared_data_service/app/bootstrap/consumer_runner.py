"""Headless consumer entrypoint (no HTTP) for consumer-only instances."""
from __future__ import annotations

import asyncio
import logging

from app.bootstrap.container import Container
from app.config.settings import Settings

logger = logging.getLogger(__name__)


async def run_consumer(settings: Settings) -> None:
    container = Container(settings)
    await container.start()
    try:
        await container.start_consumer()
    except asyncio.CancelledError:
        logger.info("consumer cancelled, shutting down")
        raise
    finally:
        await container.stop()


def main() -> None:
    try:
        asyncio.run(run_consumer(Settings()))
    except KeyboardInterrupt:
        pass
