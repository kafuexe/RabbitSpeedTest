"""Plotly chart construction and rendering (HTML + PNG)."""
from __future__ import annotations

import plotly.graph_objects as go

from benchmark.results import BenchmarkResult, BenchmarkSuiteResult
from benchmark.runner import scaling_efficiency

_LATENCY_BENCHES = ["publish_latency", "consume_latency", "round_trip"]
_LATENCY_LABELS = {"publish_latency": "Publish", "consume_latency": "Consume", "round_trip": "Round-trip"}


def _clients(suite: BenchmarkSuiteResult) -> list[str]:
    seen: list[str] = []
    for r in suite.results:
        if r.client not in seen:
            seen.append(r.client)
    return seen


def _median_ms(suite: BenchmarkSuiteResult, client: str, benchmark: str) -> float:
    vals = [r.summary.median_ns for r in suite.results if r.client == client and r.benchmark == benchmark]
    return (sum(vals) / len(vals)) / 1_000_000 if vals else 0.0


def _mps(suite: BenchmarkSuiteResult, client: str, benchmark: str) -> float:
    vals = [r.summary.messages_per_sec or 0.0
            for r in suite.results if r.client == client and r.benchmark == benchmark]
    return sum(vals) / len(vals) if vals else 0.0


class Charts:
    def latency_comparison(self, suite: BenchmarkSuiteResult) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            fig.add_bar(name=client,
                        x=[_LATENCY_LABELS[b] for b in _LATENCY_BENCHES],
                        y=[_median_ms(suite, client, b) for b in _LATENCY_BENCHES])
        fig.update_layout(barmode="group", title="Latency comparison (median)",
                          yaxis_title="Latency (ms)", template="plotly_white")
        return fig

    def throughput_comparison(self, suite: BenchmarkSuiteResult) -> go.Figure:
        benches = ["publish_throughput", "consume_throughput"]
        labels = {"publish_throughput": "Publish", "consume_throughput": "Consume"}
        fig = go.Figure()
        for client in _clients(suite):
            fig.add_bar(name=client, orientation="h",
                        y=[labels[b] for b in benches],
                        x=[_mps(suite, client, b) for b in benches])
        fig.update_layout(barmode="group", title="Throughput comparison",
                          xaxis_title="Messages / sec", template="plotly_white")
        return fig

    def consume_get_vs_push(self, suite: BenchmarkSuiteResult) -> go.Figure:
        benches = ["consume_throughput", "consume_throughput_get"]
        labels = {"consume_throughput": "Push (basic.consume)",
                  "consume_throughput_get": "Get-loop (basic.get)"}
        fig = go.Figure()
        for client in _clients(suite):
            fig.add_bar(name=client,
                        x=[labels[b] for b in benches],
                        y=[_mps(suite, client, b) for b in benches])
        fig.update_layout(barmode="group", title="Consume throughput: push vs get-loop",
                          yaxis_title="Messages / sec", template="plotly_white")
        return fig

    def _concurrent_points(self, suite, client, benchmark) -> tuple[list[int], list[float]]:
        pts = sorted(
            (int(r.params["concurrency"]), r.summary.messages_per_sec or 0.0)
            for r in suite.results if r.client == client and r.benchmark == benchmark)
        return [p[0] for p in pts], [p[1] for p in pts]

    def concurrent_chart(self, suite: BenchmarkSuiteResult, benchmark: str) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            xs, ys = self._concurrent_points(suite, client, benchmark)
            fig.add_scatter(name=client, x=xs, y=ys, mode="lines+markers")
        fig.update_layout(title=f"{benchmark.replace('_', ' ').title()} scaling",
                          xaxis_title="Concurrent workers", yaxis_title="Messages / sec",
                          template="plotly_white")
        return fig

    def scaling_chart(self, suite: BenchmarkSuiteResult, benchmark: str) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            eff = scaling_efficiency(suite.results, benchmark, client)
            xs = sorted(eff)
            fig.add_scatter(name=client, x=xs, y=[eff[n] for n in xs], mode="lines+markers")
        fig.update_layout(title=f"Scaling efficiency: {benchmark.replace('_', ' ')}",
                          xaxis_title="Concurrent workers", yaxis_title="Efficiency (1.0 = linear)",
                          template="plotly_white")
        return fig

    def distribution_chart(self, suite: BenchmarkSuiteResult, benchmark: str) -> go.Figure:
        fig = go.Figure()
        for client in _clients(suite):
            values: list[float] = []
            for r in suite.results:
                if r.client == client and r.benchmark == benchmark:
                    values.extend(s.value_ns / 1_000_000 for s in r.samples if s.success)
            if values:
                fig.add_box(name=client, y=values, boxpoints="outliers")
        fig.update_layout(title=f"Latency distribution: {benchmark.replace('_', ' ')}",
                          yaxis_title="Latency (ms)", template="plotly_white")
        return fig

    def build_all(self, suite: BenchmarkSuiteResult) -> dict[str, go.Figure]:
        return {
            "latency": self.latency_comparison(suite),
            "throughput": self.throughput_comparison(suite),
            "consume_get_vs_push": self.consume_get_vs_push(suite),
            "concurrent_publish": self.concurrent_chart(suite, "concurrent_publish"),
            "concurrent_consume": self.concurrent_chart(suite, "concurrent_consume"),
            "scaling_publish": self.scaling_chart(suite, "concurrent_publish"),
            "scaling_consume": self.scaling_chart(suite, "concurrent_consume"),
            "distribution_round_trip": self.distribution_chart(suite, "round_trip"),
        }

    def to_html_div(self, fig: go.Figure) -> str:
        return fig.to_html(full_html=False, include_plotlyjs="cdn")

    def to_png_bytes(self, fig: go.Figure) -> bytes | None:
        try:
            return fig.to_image(format="png", width=900, height=500, scale=2)
        except Exception:
            return None  # kaleido/chrome unavailable
