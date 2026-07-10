import pytest
from benchmark.clients.base import BenchmarkClient, generate_payloads
from benchmark.clients.fake_client import FakeClient


def test_generate_payloads_sizes():
    payloads = generate_payloads({"256B": 256, "1KB": 1024})
    assert len(payloads["256B"]) == 256
    assert len(payloads["1KB"]) == 1024
    assert isinstance(payloads["256B"], bytes)


async def test_fake_client_publish_consume_roundtrip():
    c = FakeClient()
    await c.connect()
    await c.declare_queue("q")
    await c.publish("", "q", b"hello", confirm=True)
    msg = await c.consume_one("q")
    assert msg == b"hello"
    assert await c.consume_one("q", timeout=0.01) is None
    await c.close()


async def test_fake_client_many():
    c = FakeClient()
    await c.connect()
    await c.declare_queue("q")
    await c.publish_many("", "q", [b"a", b"b", b"c"], confirm=True)
    assert await c.consume_many("q", 3) == 3


def test_fake_is_benchmark_client():
    assert issubclass(FakeClient, BenchmarkClient)
