"""HTML report rendering + pluggable PDF backend."""
from __future__ import annotations

import abc
import base64
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

from benchmark.reporting.charts import Charts
from benchmark.results import BenchmarkSuiteResult

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


class PdfBackend(abc.ABC):
    @abc.abstractmethod
    def available(self) -> bool: ...

    @abc.abstractmethod
    def render(self, html: str, out_path: str) -> bool: ...


class WeasyPrintBackend(PdfBackend):
    def available(self) -> bool:
        try:
            import weasyprint  # noqa: F401  (native libs load lazily)
            return True
        except Exception:
            return False

    def render(self, html: str, out_path: str) -> bool:
        try:
            import weasyprint
            weasyprint.HTML(string=html).write_pdf(out_path)
            return True
        except Exception as exc:
            print(f"[report] PDF generation skipped: {exc}")
            return False


def _ns_to_ms(ns: float) -> float:
    return round(ns / 1_000_000, 4)


def build_executive_summary(suite: BenchmarkSuiteResult) -> list[dict]:
    clients = []
    for r in suite.results:
        if r.client not in clients:
            clients.append(r.client)

    def median_for(client: str, benchmark: str) -> float:
        vals = [r.summary.median_ns for r in suite.results if r.client == client and r.benchmark == benchmark]
        return sum(vals) / len(vals) if vals else float("inf")

    def mps_for(client: str, benchmark: str) -> float:
        vals = [r.summary.messages_per_sec or 0.0
                for r in suite.results if r.client == client and r.benchmark == benchmark]
        return sum(vals) / len(vals) if vals else 0.0

    rows: list[dict] = []
    latency_cats = [("Publish latency", "publish_latency"), ("Consume latency", "consume_latency"),
                    ("Round-trip latency", "round_trip")]
    for label, bench in latency_cats:
        ranked = sorted(clients, key=lambda c: median_for(c, bench))
        best, worst = ranked[0], ranked[-1]
        b, w = median_for(best, bench), median_for(worst, bench)
        pct = ((w - b) / w * 100) if w else 0.0
        rows.append({"category": label, "winner": best,
                     "detail": f"{_ns_to_ms(b)} ms median, {pct:.1f}% faster than {worst}"})
    tp_cats = [("Publish throughput", "publish_throughput"), ("Consume throughput", "consume_throughput")]
    for label, bench in tp_cats:
        ranked = sorted(clients, key=lambda c: mps_for(c, bench), reverse=True)
        best, worst = ranked[0], ranked[-1]
        b, w = mps_for(best, bench), mps_for(worst, bench)
        pct = ((b - w) / w * 100) if w else 0.0
        rows.append({"category": label, "winner": best,
                     "detail": f"{b:,.0f} msgs/sec, {pct:.1f}% higher than {worst}"})
    return rows


def _tables(suite: BenchmarkSuiteResult) -> dict[str, list[dict]]:
    tables: dict[str, list[dict]] = {}
    for r in suite.results:
        tables.setdefault(r.benchmark, []).append({
            "client": r.client,
            "params": ", ".join(f"{k}={v}" for k, v in r.params.items()),
            "median": _ns_to_ms(r.summary.median_ns),
            "p95": _ns_to_ms(r.summary.p95_ns),
            "p99": _ns_to_ms(r.summary.p99_ns),
            "mps": f"{r.summary.messages_per_sec:,.0f}" if r.summary.messages_per_sec else "-",
            "failed": r.summary.n_failed,
        })
    return tables


def _appendix(suite: BenchmarkSuiteResult) -> list[dict]:
    rows: list[dict] = []
    for r in suite.results:
        rows.append({
            "client": r.client, "benchmark": r.benchmark,
            "params": ", ".join(f"{k}={v}" for k, v in r.params.items()),
            "avg": _ns_to_ms(r.summary.avg_ns), "median": _ns_to_ms(r.summary.median_ns),
            "min": _ns_to_ms(r.summary.min_ns), "max": _ns_to_ms(r.summary.max_ns),
            "stddev": _ns_to_ms(r.summary.stddev_ns),
            "p95": _ns_to_ms(r.summary.p95_ns), "p99": _ns_to_ms(r.summary.p99_ns),
        })
    return rows


def _conclusions(exec_summary: list[dict]) -> list[str]:
    return [f"{row['winner']} leads on {row['category'].lower()} ({row['detail']})." for row in exec_summary]


def generate_report(
    suite: BenchmarkSuiteResult, out_dir: str, *, pdf_backend: PdfBackend | None = None,
) -> dict[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    charts = Charts()
    figs = charts.build_all(suite)
    chart_divs = {name: charts.to_html_div(fig) for name, fig in figs.items()}

    # Static PNGs are only needed to embed in the PDF. Rendering them spawns a
    # headless browser (via kaleido), so only do it when a PDF will actually be
    # produced — the HTML report uses the interactive divs above.
    backend = pdf_backend if pdf_backend is not None else WeasyPrintBackend()
    pdf_enabled = backend.available()
    chart_pngs: dict[str, str] = {}
    if pdf_enabled:
        for name, fig in figs.items():
            png = charts.to_png_bytes(fig)
            if png is not None:
                chart_pngs[name] = "data:image/png;base64," + base64.b64encode(png).decode()

    exec_summary = build_executive_summary(suite)
    mem = suite.environment.total_memory_bytes
    env = Environment(loader=FileSystemLoader(_TEMPLATE_DIR), autoescape=select_autoescape(["html"]))
    template = env.get_template("report.html.j2")
    html = template.render(
        suite=suite,
        memory_gb=(round(mem / 1024**3, 1) if mem else "unknown"),
        exec_summary=exec_summary,
        chart_divs=chart_divs,
        chart_pngs=chart_pngs,
        tables=_tables(suite),
        appendix=_appendix(suite),
        conclusions=_conclusions(exec_summary),
    )

    html_path = os.path.join(out_dir, "report.html")
    with open(html_path, "w", encoding="utf-8") as fh:
        fh.write(html)

    pdf_path = ""
    if pdf_enabled:
        candidate = os.path.join(out_dir, "report.pdf")
        if backend.render(html, candidate):
            pdf_path = candidate
    else:
        print("[report] WeasyPrint unavailable; wrote HTML only.")

    return {"html": html_path, "pdf": pdf_path}
