import json
from benchmark.config import BenchmarkConfig, MESSAGE_SIZES


def test_default_config():
    c = BenchmarkConfig.default()
    assert c.amqp_url == "amqp://guest:guest@localhost:5672/"
    assert c.message_count == 50_000
    assert c.concurrency_levels == [1, 2, 4, 8, 16, 32]
    assert c.publisher_confirms is True
    assert c.iterations == 10 and c.warmup_iterations == 2
    assert c.latency_sample_count >= 500
    assert c.durable is False  # non-durable queue + transient messages by default
    assert c.prefetch is None  # None -> each client's own default (hybrid: tuned)
    assert c.pipeline_batch is None
    assert set(c.message_sizes) == set(MESSAGE_SIZES)
    assert c.clients == ["pika", "aio-pika"]


def test_load_merges_json_then_env_then_overrides(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg.json"
    cfg_file.write_text(json.dumps({
        "message_count": 100,
        "queue_name": "from_json",
        "amqp_url": "amqp://from-json/"
    }))
    monkeypatch.setenv("RABBITMQ_URL", "amqp://env-host/")
    c = BenchmarkConfig.load(
        str(cfg_file),
        overrides={"message_count": 7, "amqp_url": "amqp://from-override/"}
    )
    assert c.message_count == 7          # override wins
    assert c.queue_name == "from_json"   # json applied
    assert c.amqp_url == "amqp://from-override/"  # override beats env beats json


def test_env_beats_json_for_url(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg.json"
    cfg_file.write_text(json.dumps({"amqp_url": "amqp://from-json/"}))
    monkeypatch.setenv("RABBITMQ_URL", "amqp://env-host/")
    c = BenchmarkConfig.load(str(cfg_file))
    assert c.amqp_url == "amqp://env-host/"  # env beats json


def test_to_dict_roundtrip():
    c = BenchmarkConfig.default()
    d = c.to_dict()
    assert d["message_count"] == 50_000
    assert isinstance(d["concurrency_levels"], list)
