"""Settings floor: misconfigurations must fail at startup, not run silently."""
from urllib.parse import parse_qsl, urlsplit

import pytest
from pydantic import ValidationError

from app.config.settings import Settings


def test_consuming_modes_reject_empty_queue_list():
    # SDS_CONSUME_QUEUES='[]' used to start, consume nothing, and exit 0
    # looking successful.
    for mode in ("consumer", "both"):
        with pytest.raises(ValidationError):
            Settings(service_mode=mode, consume_queues=[])


def test_api_mode_allows_empty_queue_list():
    assert Settings(service_mode="api", consume_queues=[]).consume_queues == []


def test_missing_ca_file_fails_at_startup(tmp_path):
    with pytest.raises(ValidationError):
        Settings(amqp_ca_file=str(tmp_path / "missing.pem"))
    with pytest.raises(ValidationError):
        Settings(db_ca_file=str(tmp_path / "missing.pem"))


def test_effective_amqp_url_without_ca_is_unchanged():
    s = Settings()
    assert s.effective_amqp_url == s.amqp_url


def test_effective_amqp_url_appends_cafile(tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("dummy")
    s = Settings(
        amqp_url="amqps://u:p@broker.internal:5671/vh?heartbeat=30",
        amqp_ca_file=str(ca),
    )
    parts = urlsplit(s.effective_amqp_url)
    assert parts.scheme == "amqps"
    assert parts.netloc == "u:p@broker.internal:5671"
    assert dict(parse_qsl(parts.query)) == {"heartbeat": "30", "cafile": str(ca)}


def test_deploy_env_files_load_with_dotenv_priority(tmp_path, monkeypatch):
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "config.env").write_text(
        "SDS_LOG_LEVEL=WARNING\nSDS_API_PORT=9000\n"
    )
    (tmp_path / "deploy" / "secrets.env").write_text(
        "SDS_AMQP_URL=amqp://u:p@broker:5672/vh\n"
    )
    (tmp_path / ".env").write_text("SDS_API_PORT=9100\n")
    monkeypatch.chdir(tmp_path)
    s = Settings()
    assert s.log_level == "WARNING"
    assert s.amqp_url == "amqp://u:p@broker:5672/vh"
    # .env is the local-dev override; it wins over the deploy files.
    assert s.api_port == 9100


def test_real_env_vars_beat_env_files(tmp_path, monkeypatch):
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "config.env").write_text("SDS_LOG_LEVEL=WARNING\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SDS_LOG_LEVEL", "ERROR")
    assert Settings().log_level == "ERROR"
