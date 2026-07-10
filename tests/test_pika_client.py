import inspect
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.pika_client import PikaClient


def test_pika_client_is_benchmark_client():
    assert issubclass(PikaClient, BenchmarkClient)
    assert PikaClient("amqp://x/").name == "pika"


def test_pika_methods_are_coroutines():
    c = PikaClient("amqp://x/")
    for m in ["connect", "close", "declare_queue", "purge_queue", "delete_queue",
              "publish", "consume_one", "publish_many", "consume_many",
              "server_version"]:
        assert inspect.iscoroutinefunction(getattr(c, m)), m
