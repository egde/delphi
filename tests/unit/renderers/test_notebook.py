from delphi.renderers.notebook import render_notebook
from delphi.confidence.result import ConfidenceResult


def _make_result(status="pass", passed=True):
    return {
        "test_name": "test_nulls",
        "table": "catalog.schema.t",
        "status": status,
        "confidence_result": ConfidenceResult(
            observed=0.003, ci_lower=0.001, ci_upper=0.005,
            confidence=0.95, method="wilson", sample_size=50000, passed=passed,
        ),
        "threshold": "< 0.01",
        "duration_ms": 1840,
        "evidence": [],
        "error": None,
        "suggestion": None,
    }


def test_render_notebook_returns_html():
    html = render_notebook([_make_result()], total_ms=2000)
    assert "<html>" in html
    assert "Delphi Test Results" in html


def test_render_notebook_contains_table():
    html = render_notebook([_make_result()])
    assert "test_nulls" in html
    assert "PASS" in html
    assert "0.0030" in html


def test_render_notebook_contains_chart():
    html = render_notebook([_make_result()])
    assert "plotly" in html.lower()
    assert "delphi-ci-chart" in html


def test_render_notebook_summary():
    results = [_make_result("pass"), _make_result("fail", passed=False)]
    html = render_notebook(results, total_ms=3500)
    assert "1 passed" in html
    assert "1 failed" in html
    assert "3.5s" in html
