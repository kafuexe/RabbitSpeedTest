import math
from benchmark.statistics import summarize, StatSummary


def test_summarize_basic_percentiles():
    values = list(range(1, 101))  # 1..100 ns
    s = summarize(values)
    assert s.min_ns == 1
    assert s.max_ns == 100
    assert s.avg_ns == 50.5
    assert s.median_ns == 50.5
    assert math.isclose(s.p95_ns, 95.05, rel_tol=1e-6)
    assert math.isclose(s.p99_ns, 99.01, rel_tol=1e-6)
    assert s.n_success == 100
    assert s.n_failed == 0
    assert s.messages_per_sec is None


def test_summarize_empty_is_zeroed():
    s = summarize([], n_failed=3)
    assert s.avg_ns == 0.0 and s.max_ns == 0.0 and s.p99_ns == 0.0
    assert s.n_success == 0
    assert s.n_failed == 3


def test_summarize_empty_is_marked_failed():
    assert summarize([], n_failed=3).failed is True
    assert summarize([], n_failed=0).failed is True  # never produced a sample
    assert summarize([10, 20]).failed is False
    assert summarize([10, 20], n_failed=1).failed is False  # partial failure


def test_summarize_throughput_fields():
    s = summarize([10, 20, 30], total_duration_ns=1_000_000_000, message_count=500)
    assert s.total_duration_ns == 1_000_000_000
    assert math.isclose(s.messages_per_sec, 500.0, rel_tol=1e-9)


def test_summarize_stddev():
    s = summarize([2, 4, 4, 4, 5, 5, 7, 9])
    assert math.isclose(s.stddev_ns, 2.0, rel_tol=1e-9)  # population stddev
