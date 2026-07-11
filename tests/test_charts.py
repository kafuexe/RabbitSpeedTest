import plotly.graph_objects as go
from benchmark.reporting.charts import Charts
from tests.helpers import make_suite  # created below


def test_build_all_returns_expected_charts():
    suite = make_suite()
    charts = Charts()
    figs = charts.build_all(suite)
    for key in ["latency", "throughput", "consume_get_vs_push",
                "concurrent_publish", "concurrent_consume",
                "scaling_publish", "distribution_round_trip"]:
        assert key in figs
        assert isinstance(figs[key], go.Figure)


def test_charts_render_all_four_clients():
    suite = make_suite(clients=(("pika", 1.0), ("aio-pika", 0.7),
                                ("hybrid", 0.5), ("simple", 0.8)))
    figs = Charts().build_all(suite)
    for key in ("latency", "throughput", "consume_get_vs_push",
                "concurrent_publish", "concurrent_consume"):
        assert len(figs[key].data) == 4, f"{key} chart missing client traces"


def test_to_html_div_is_string():
    suite = make_suite()
    charts = Charts()
    html = charts.to_html_div(charts.latency_comparison(suite))
    assert isinstance(html, str) and "plotly" in html.lower()
