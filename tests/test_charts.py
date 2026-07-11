import plotly.graph_objects as go
from benchmark.reporting.charts import Charts
from tests.helpers import make_suite  # created below


def test_build_all_returns_expected_charts():
    suite = make_suite()
    charts = Charts()
    figs = charts.build_all(suite)
    for key in ["latency", "throughput", "concurrent_publish", "concurrent_consume",
                "scaling_publish", "distribution_round_trip"]:
        assert key in figs
        assert isinstance(figs[key], go.Figure)


def test_to_html_div_is_string():
    suite = make_suite()
    charts = Charts()
    html = charts.to_html_div(charts.latency_comparison(suite))
    assert isinstance(html, str) and "plotly" in html.lower()
