import pytest

from app.messaging.cloudevents import CloudEvent, InvalidCloudEvent, now_utc


def test_roundtrip():
    event = CloudEvent(
        id="e-1", source="urn:test", type="user.created",
        time=now_utc(), data={"id": "abc", "version": 1}, correlationid="c-1",
    )
    parsed = CloudEvent.from_bytes(event.to_bytes())
    assert parsed == event


def test_missing_required_attribute_rejected():
    with pytest.raises(InvalidCloudEvent):
        CloudEvent.from_bytes(b'{"specversion": "1.0", "source": "s", "type": "t"}')


def test_unsupported_specversion_rejected():
    with pytest.raises(InvalidCloudEvent):
        CloudEvent.from_bytes(b'{"specversion": "0.3", "id": "1", "source": "s", "type": "t"}')


def test_missing_specversion_rejected_on_the_wire():
    # CloudEvents 1.0 REQUIRES specversion; the field default exists only
    # for internal construction. Regression: the default must not fill in a
    # missing key when parsing wire bytes.
    with pytest.raises(InvalidCloudEvent):
        CloudEvent.from_bytes(b'{"id": "1", "source": "s", "type": "t"}')
    # Internal construction keeps the ergonomic default.
    assert CloudEvent(id="1", source="s", type="t").specversion == "1.0"


def test_non_json_rejected():
    with pytest.raises(InvalidCloudEvent):
        CloudEvent.from_bytes(b"\xff\xfenot json")


def test_non_object_json_rejected():
    with pytest.raises(InvalidCloudEvent):
        CloudEvent.from_bytes(b'[1, 2, 3]')


def test_defaults():
    e = CloudEvent(id="1", source="s", type="t")
    assert e.specversion == "1.0"
    assert e.datacontenttype == "application/json"
    assert e.data == {}


def test_oversized_attributes_rejected_as_invalid_envelope():
    # id/source are inbox PK columns (String(255)); an oversized value must
    # be rejected at the envelope (log+ack), never poison the inbox INSERT.
    for field, value in (("id", "x" * 256), ("source", "s" * 256), ("type", "t" * 256)):
        payload = {"specversion": "1.0", "id": "1", "source": "s", "type": "t"}
        payload[field] = value
        import json
        with pytest.raises(InvalidCloudEvent):
            CloudEvent.from_bytes(json.dumps(payload).encode())
    # 255 exactly is fine
    assert CloudEvent(id="x" * 255, source="s" * 255, type="t").id == "x" * 255


def test_nul_bytes_in_envelope_rejected_as_invalid_envelope():
    # A NUL travels through JSON as the perfectly-valid escape \\u0000, passes
    # length checks, then fails the processed_events INSERT on every
    # redelivery — it must die at the envelope instead.
    import json
    for field in ("id", "source", "type", "correlationid"):
        payload = {"specversion": "1.0", "id": "1", "source": "s", "type": "t"}
        payload[field] = "a\x00b"
        with pytest.raises(InvalidCloudEvent):
            CloudEvent.from_bytes(json.dumps(payload).encode())


def test_unsupported_specversion_reason_contains_no_input_value():
    # Regression: the old pre-parse check interpolated the raw specversion
    # value into the rejection reason, which is logged verbatim.
    smuggled = "alice@example.com"
    body = b'{"specversion": "%s", "id": "1", "source": "s", "type": "t"}' % smuggled.encode()
    with pytest.raises(InvalidCloudEvent) as exc_info:
        CloudEvent.from_bytes(body)
    assert smuggled not in str(exc_info.value)
    assert "specversion" in str(exc_info.value)  # location kept


def test_invalid_envelope_reason_contains_no_input_values():
    # PII guard: the InvalidCloudEvent message must not embed the payload.
    secret = "topsecret@example.com"
    body = b'{"specversion": "1.0", "id": "", "source": "%s", "type": "t"}' % secret.encode()
    try:
        CloudEvent.from_bytes(body)
        raise AssertionError("should have raised")
    except InvalidCloudEvent as exc:
        assert secret not in str(exc)
        assert "id" in str(exc)  # location is kept for debuggability
