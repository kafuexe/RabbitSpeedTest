"""Application settings, loaded from environment variables (prefix SDS_)."""
from __future__ import annotations

from pathlib import Path
from typing import Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Later files win; real environment variables beat every file.
    # deploy/*.env is the on-prem contract; .env stays the local-dev override.
    model_config = SettingsConfigDict(
        env_prefix="SDS_",
        env_file=("deploy/config.env", "deploy/secrets.env", ".env"),
        extra="ignore",
    )

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

    # Optional TLS: path to an internal CA bundle. Empty = plaintext.
    amqp_ca_file: str = ""
    db_ca_file: str = ""

    @model_validator(mode="after")
    def _consumer_modes_need_queues(self) -> "Settings":
        # SDS_CONSUME_QUEUES='[]' with a consuming mode would otherwise start,
        # consume nothing, and exit 0 looking successful.
        if self.service_mode in ("consumer", "both") and not self.consume_queues:
            raise ValueError(
                "consume_queues must not be empty when service_mode is "
                f"{self.service_mode!r}"
            )
        return self

    @model_validator(mode="after")
    def _ca_files_must_exist(self) -> "Settings":
        # A typo'd CA path must fail at startup, not at first TLS connect.
        for name in ("amqp_ca_file", "db_ca_file"):
            value = getattr(self, name)
            if value and not Path(value).is_file():
                raise ValueError(f"{name}: CA bundle not found: {value!r}")
        return self

    @property
    def effective_amqp_url(self) -> str:
        """amqp_url with the CA bundle attached as aio-pika's ``cafile``
        URL query parameter — RabbitClient itself stays untouched."""
        if not self.amqp_ca_file:
            return self.amqp_url
        parts = urlsplit(self.amqp_url)
        query = dict(parse_qsl(parts.query))
        query["cafile"] = self.amqp_ca_file
        return urlunsplit(parts._replace(query=urlencode(query)))
