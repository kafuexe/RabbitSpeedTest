import os
from benchmark.reporting.report_generator import (
    generate_report, build_executive_summary, WeasyPrintBackend,
)
from benchmark.results import BenchmarkResult, IterationSample
from benchmark.statistics import summarize
from tests.helpers import make_suite


def _failed_result(client: str, bench: str) -> BenchmarkResult:
    samples = [IterationSample(client, bench, i, 0, False, "boom", {"size": "1KB"})
               for i in range(2)]
    return BenchmarkResult(client, bench, {"size": "1KB"}, summarize([], n_failed=2), samples)


class _NoPdf(WeasyPrintBackend):
    def available(self) -> bool:
        return False


def test_executive_summary_picks_winners():
    rows = build_executive_summary(make_suite())
    assert any(r["category"].lower().startswith("publish") for r in rows)
    assert all("winner" in r for r in rows)


def test_generate_report_writes_html_and_handles_missing_pdf(tmp_path):
    out = generate_report(make_suite(), str(tmp_path), pdf_backend=_NoPdf())
    assert os.path.exists(out["html"])
    html = open(out["html"], encoding="utf-8").read()
    assert "Executive Summary" in html
    assert "plotly" in html.lower()
    assert out["pdf"] == ""  # gracefully skipped


def test_report_scales_to_four_clients(tmp_path):
    suite = make_suite(clients=(("pika", 1.0), ("aio-pika", 0.7),
                                ("hybrid", 0.5), ("simple", 0.8)))
    out = generate_report(suite, str(tmp_path), pdf_backend=_NoPdf())
    html = open(out["html"], encoding="utf-8").read()
    # Dynamic subtitle: built from the suite's clients, not hardcoded.
    assert "pika vs aio-pika vs hybrid vs simple" in html
    # Executive summary shows every client's number, not just winner/worst.
    rows = build_executive_summary(suite)
    for row in rows:
        breakdown = row["breakdown"]
        for name in ("pika", "aio-pika", "hybrid", "simple"):
            assert name in breakdown, f"{name} missing from '{row['category']}' breakdown"


def test_report_marks_failed_rows(tmp_path):
    suite = make_suite()
    suite.results.append(_failed_result("pika", "publish_throughput"))
    out = generate_report(suite, str(tmp_path), pdf_backend=_NoPdf())
    html = open(out["html"], encoding="utf-8").read()
    assert "FAILED" in html


def test_executive_summary_ignores_failed_rows():
    suite = make_suite()
    # pika's only publish_latency row failed: its 0.0 median must not "win".
    suite.results = [r for r in suite.results
                     if not (r.client == "pika" and r.benchmark == "publish_latency")]
    suite.results.append(_failed_result("pika", "publish_latency"))
    rows = build_executive_summary(suite)
    pub = next(r for r in rows if r["category"] == "Publish latency")
    assert pub["winner"] == "aio-pika"
