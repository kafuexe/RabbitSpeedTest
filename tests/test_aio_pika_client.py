import inspect
from benchmark.clients.base import BenchmarkClient
from benchmark.clients.aio_pika_client import AioPikaClient


def test_aio_pika_client_is_benchmark_client():
    assert issubclass(AioPikaClient, BenchmarkClient)
    assert AioPikaClient("amqp://x/").name == "aio-pika"


def test_aio_pika_methods_are_coroutines():
    c = AioPikaClient("amqp://x/")
    for m in ["connect", "close", "declare_queue", "purge_queue", "delete_queue",
              "publish", "consume_one", "publish_many", "consume_many", "server_version"]:
        assert inspect.iscoroutinefunction(getattr(c, m)), m
