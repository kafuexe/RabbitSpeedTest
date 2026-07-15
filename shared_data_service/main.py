"""Service entrypoint.

SDS_SERVICE_MODE selects what this instance runs:
  api      — REST API only
  consumer — RabbitMQ consumer only (no HTTP)
  both     — API + consumer in one process (default; dev-friendly)
"""
from __future__ import annotations

import uvicorn

from app.bootstrap.consumer_runner import main as run_consumer_main
from app.config.settings import Settings


def main() -> None:
    settings = Settings()
    if settings.service_mode == "consumer":
        run_consumer_main()
        return
    uvicorn.run(
        "app.bootstrap.api_app:create_app_from_env",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,  # our structured logging owns the root logger
    )


if __name__ == "__main__":
    main()
