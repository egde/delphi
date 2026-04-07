"""Tests for run history and drift detection."""

import json
import pytest
from delphi.history import save_run, load_baseline, load_history, detect_drift
from delphi.runner import TestResult
from delphi.confidence.result import ConfidenceResult


def _make_result(test_name="test_nulls", table="t", status="pass", observed=0.003):
    return TestResult(
        test_name=test_name,
        table=table,
        status=status,
        confidence_result=ConfidenceResult(
            observed=observed, ci_lower=observed - 0.001, ci_upper=observed + 0.001,
            confidence=0.95, method="wilson", sample_size=10000, passed=(status == "pass"),
        ),
        threshold="< 0.01",
        duration_ms=100,
    )


def test_save_and_load_baseline(tmp_path):
    """Save results and load baseline."""
    path = str(tmp_path / "history.jsonl")
    results = [_make_result()]
    run_id = save_run(results, history_path=path)
    assert run_id  # non-empty

    baseline = load_baseline("test_nulls", "t", history_path=path)
    assert baseline is not None
    assert baseline["test_name"] == "test_nulls"
    assert baseline["observed"] == pytest.approx(0.003)
    assert baseline["status"] == "pass"


def test_load_baseline_returns_most_recent_pass(tmp_path):
    """Baseline is the most recent passing result."""
    path = str(tmp_path / "history.jsonl")
    save_run([_make_result(observed=0.001)], history_path=path)
    save_run([_make_result(observed=0.002)], history_path=path)
    save_run([_make_result(status="fail", observed=0.05)], history_path=path)

    baseline = load_baseline("test_nulls", "t", history_path=path)
    assert baseline["observed"] == pytest.approx(0.002)  # Most recent pass, not the fail


def test_load_baseline_no_file(tmp_path):
    """Returns None when no history file."""
    baseline = load_baseline("test_nulls", "t", history_path=str(tmp_path / "nope.jsonl"))
    assert baseline is None


def test_load_history(tmp_path):
    """Load all history for a test."""
    path = str(tmp_path / "history.jsonl")
    save_run([_make_result(observed=0.001)], history_path=path)
    save_run([_make_result(observed=0.002)], history_path=path)
    save_run([_make_result(observed=0.003)], history_path=path)

    entries = load_history("test_nulls", "t", history_path=path)
    assert len(entries) == 3
    assert entries[-1]["observed"] == pytest.approx(0.003)


def test_detect_drift_significant():
    """Drift detected when shift exceeds threshold."""
    baseline = {"observed": 0.003, "ci_lower": 0.002, "ci_upper": 0.004}
    drift = detect_drift(current_observed=0.010, baseline=baseline, threshold=2.0)
    assert drift is not None
    assert drift["direction"] == "up"
    assert drift["shift_magnitude"] > 2.0


def test_detect_drift_not_significant():
    """No drift when shift is small."""
    baseline = {"observed": 0.003, "ci_lower": 0.002, "ci_upper": 0.004}
    drift = detect_drift(current_observed=0.0032, baseline=baseline, threshold=2.0)
    assert drift is None


def test_detect_drift_no_baseline():
    """No drift when baseline has no CI."""
    baseline = {"observed": 0.003, "ci_lower": None, "ci_upper": None}
    drift = detect_drift(current_observed=0.010, baseline=baseline)
    assert drift is None
