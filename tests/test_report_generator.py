import os
from benchmark.reporting.report_generator import (
    generate_report, build_executive_summary, WeasyPrintBackend,
)
from tests.helpers import make_suite


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
