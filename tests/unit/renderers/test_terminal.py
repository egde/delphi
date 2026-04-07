from delphi.renderers.terminal import render_terminal
from delphi.confidence.result import ConfidenceResult


def _make_result(passed=True, status="pass", **kwargs):
    defaults = {
        "test_name": "test_revenue_nulls",
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
    defaults.update(kwargs)
    return defaults


def test_render_terminal_pass():
    output = render_terminal([_make_result(passed=True, status="pass")])
    assert "PASS" in output


def test_render_terminal_fail():
    output = render_terminal([_make_result(
        passed=False, status="fail",
        evidence=[{"date": "2026-03-12", "revenue": "None"}],
    )])
    assert "FAIL" in output


def test_render_terminal_error():
    output = render_terminal([_make_result(
        status="error",
        error="Column 'revnue' not found",
        suggestion='Did you mean "revenue"?',
    )])
    assert "ERROR" in output
