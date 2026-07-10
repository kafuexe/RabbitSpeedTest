import json
import os
from benchmark.config import BenchmarkConfig, MESSAGE_SIZES


def test_default_config():
    c = BenchmarkConfig.default()
    assert c.amqp_url == "amqp://guest:guest@localhost:5672/"
    assert c.message_count == 50_000
    assert c.concurrency_levels == [1, 2, 4, 8, 16, 32]
    assert c.publisher_confirms is True
    assert c.iterations == 10 and c.warmup_iterations == 5
    assert set(c.message_sizes) == set(MESSAGE_SIZES)
    assert c.clients == ["pika", "aio-pika"]


def test_load_merges_json_then_env_then_overrides(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg.json"
    cfg_file.write_text(json.dumps({"message_count": 100, "queue_name": "from_json"}))
    monkeypatch.setenv("RABBITMQ_URL", "amqp://env-host/")
    c = BenchmarkConfig.load(str(cfg_file), overrides={"message_count": 7})
    assert c.message_count == 7          # override wins
    assert c.queue_name == "from_json"   # json applied
    assert c.amqp_url == "amqp://env-host/"  # env applied over default


def test_to_dict_roundtrip():
    c = BenchmarkConfig.default()
    d = c.to_dict()
    assert d["message_count"] == 50_000
    assert isinstance(d["concurrency_levels"], list)
