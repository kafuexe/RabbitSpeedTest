"""Settings floor: misconfigurations must fail at startup, not run silently."""
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
