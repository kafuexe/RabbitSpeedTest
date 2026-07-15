"""Application settings, loaded from environment variables (prefix SDS_)."""
from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SDS_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://sds:sds@localhost:5434/shared_data"
    amqp_url: str = "amqp://guest:guest@localhost:5672/"

    consume_queues: list[str] = ["shared-data.events.in"]
    publish_queue: str = "shared-data.events.out"
    prefetch: int = 500
    # Upper bound for the greedy consumer micro-batch (one transaction per
    # batch). The batcher never waits to fill it, so it adds no latency.
    consumer_batch_size: int = 200
    persistent_messages: bool = True
    event_source: str = "urn:sds:shared-data-service"

    service_mode: Literal["api", "consumer", "both"] = "both"
    api_host: str = "127.0.0.1"
    api_port: int = 8080

    log_level: str = "INFO"
    max_page_size: int = 200
    db_pool_size: int = 10
    db_max_overflow: int = 20
