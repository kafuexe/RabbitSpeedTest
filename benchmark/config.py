"""Benchmark configuration with defaults / JSON / env / override merge."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict

MESSAGE_SIZES: dict[str, int] = {
    "256B": 256,
    "1KB": 1024,
    "10KB": 10240,
    "100KB": 102400,
}


@dataclass
class BenchmarkConfig:
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    management_url: str | None = "http://guest:guest@localhost:15672"
    queue_name: str = "benchmark_queue"
    exchange: str = ""
    routing_key: str = "benchmark_queue"
    message_count: int = 50_000
    message_sizes: dict[str, int] = field(default_factory=lambda: dict(MESSAGE_SIZES))
    iterations: int = 10
    warmup_iterations: int = 5
    concurrency_levels: list[int] = field(default_factory=lambda: [1, 2, 4, 8, 16, 32])
    publisher_confirms: bool = True
    prefetch: int = 100
    clients: list[str] = field(default_factory=lambda: ["pika", "aio-pika"])
    output_dir: str = "results"
    latency_sample_count: int = 1000

    @classmethod
    def default(cls) -> "BenchmarkConfig":
        return cls()

    @classmethod
    def load(
        cls,
        json_path: str | None = None,
        overrides: dict | None = None,
    ) -> "BenchmarkConfig":
        data: dict = asdict(cls.default())
        if json_path and os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as fh:
                data.update(json.load(fh))
        env_url = os.environ.get("RABBITMQ_URL")
        if env_url:
            data["amqp_url"] = env_url
        env_mgmt = os.environ.get("RABBITMQ_MANAGEMENT_URL")
        if env_mgmt:
            data["management_url"] = env_mgmt
        if overrides:
            data.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**data)

    def to_dict(self) -> dict:
        return asdict(self)
