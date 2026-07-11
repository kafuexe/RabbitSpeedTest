from benchmark.clients.base import BenchmarkClient
from benchmark.clients.pika_client import PikaClient
from tests.helpers import assert_client_methods_are_coroutines


def test_pika_client_is_benchmark_client():
    assert issubclass(PikaClient, BenchmarkClient)
    assert PikaClient("amqp://x/").name == "pika"


def test_pika_methods_are_coroutines():
    assert_client_methods_are_coroutines(PikaClient("amqp://x/"))


def test_pika_ctor_flags_and_clone():
    c = PikaClient("amqp://x/", prefetch=7, publisher_confirms=False, durable=True)
    assert c._confirms is False and c._durable is True
    d = c.clone()
    assert d is not c and isinstance(d, PikaClient)
    assert d._url == c._url and d._prefetch == 7
    assert d._confirms is False and d._durable is True
