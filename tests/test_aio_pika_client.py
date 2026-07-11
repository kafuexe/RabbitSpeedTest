from benchmark.clients.aio_pika_client import AioPikaClient
from benchmark.clients.base import BenchmarkClient
from tests.helpers import assert_client_methods_are_coroutines


def test_aio_pika_client_is_benchmark_client():
    assert issubclass(AioPikaClient, BenchmarkClient)
    assert AioPikaClient("amqp://x/").name == "aio-pika"


def test_aio_pika_methods_are_coroutines():
    assert_client_methods_are_coroutines(AioPikaClient("amqp://x/"))


def test_aio_pika_ctor_flags_and_clone():
    c = AioPikaClient("amqp://x/", prefetch=7, publisher_confirms=False, durable=True,
                      pipeline_batch=42)
    assert c._confirms is False and c._durable is True
    assert c._pipeline_batch == 42
    d = c.clone()
    assert d is not c and isinstance(d, AioPikaClient)
    assert d._url == c._url and d._prefetch == 7
    assert d._confirms is False and d._durable is True
    assert d._pipeline_batch == 42


def test_aio_pika_default_pipeline_batch():
    assert AioPikaClient("amqp://x/")._pipeline_batch == 500
