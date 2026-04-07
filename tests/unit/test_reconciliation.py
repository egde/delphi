"""Tests for reconciliation metrics — coverage, match_rate, mean_deviation."""

import pytest
from delphi.assertions.column import col
from delphi.assertions.expectation import Expectation
from delphi.runner import _compute_confidence
from delphi.confidence.result import ConfidenceResult


# --- DSL expression tests ---

def test_coverage_expression():
    """col(*keys).coverage(ref) produces correct Expectation."""
    from delphi.comparison.compare import ComparisonRef
    ref = ComparisonRef("expected_table")
    exp = col("ticker", "date").coverage(ref) > 0.99
    assert isinstance(exp, Expectation)
    assert exp.metric == "coverage"
    assert exp.threshold == 0.99
    assert exp.direction == "above"
    assert exp.key_columns == ["ticker", "date"]
    assert exp.compare_table == "expected_table"


def test_match_rate_exact_expression():
    """col("x").match_rate(ref, key=[...]) produces correct Expectation."""
    from delphi.comparison.compare import ComparisonRef
    ref = ComparisonRef("expected_table")
    exp = col("name").match_rate(ref, key=["ticker"]) > 0.99
    assert exp.metric == "match_rate"
    assert exp.column == "name"
    assert exp.key_columns == ["ticker"]
    assert exp.tolerance == 0.0


def test_match_rate_tolerance_expression():
    """match_rate with tolerance stores it."""
    from delphi.comparison.compare import ComparisonRef
    ref = ComparisonRef("expected_table")
    exp = col("close").match_rate(ref, key=["ticker", "date"], tolerance=0.01) > 0.95
    assert exp.tolerance == 0.01
    assert exp.key_columns == ["ticker", "date"]


def test_mean_deviation_expression():
    """col("x").mean_deviation(ref, key=[...]) produces correct Expectation."""
    from delphi.comparison.compare import ComparisonRef
    ref = ComparisonRef("expected_table")
    exp = col("close").mean_deviation(ref, key=["ticker", "date"]) < 0.005
    assert exp.metric == "mean_deviation"
    assert exp.direction == "below"
    assert exp.threshold == 0.005


# --- Confidence routing tests ---

def test_confidence_coverage():
    """Coverage metric routes to Wilson."""
    exp = Expectation(column=None, metric="coverage", threshold=0.99, direction="above", confidence=0.95)
    metrics = {"found": 990, "total": 1000, "missing": 10, "rate": 0.99}
    result = _compute_confidence(exp, metrics, sample_size=1000)
    assert result.method == "wilson"
    assert result.observed == pytest.approx(0.99, abs=0.001)


def test_confidence_match_rate():
    """Match rate routes to Wilson."""
    exp = Expectation(column="name", metric="match_rate", threshold=0.95, direction="above", confidence=0.95)
    metrics = {"matches": 970, "total": 1000, "rate": 0.97}
    result = _compute_confidence(exp, metrics, sample_size=1000)
    assert result.method == "wilson"
    assert result.observed == pytest.approx(0.97, abs=0.001)


def test_confidence_mean_deviation():
    """Mean deviation routes to t-distribution."""
    exp = Expectation(column="close", metric="mean_deviation", threshold=0.01, direction="below", confidence=0.95)
    metrics = {"mean_dev": 0.003, "std_dev": 0.001, "total": 1000}
    result = _compute_confidence(exp, metrics, sample_size=1000)
    assert result.method == "t"
    assert result.observed == pytest.approx(0.003)


# --- YAML parsing tests ---

def test_yaml_reconciliation():
    from delphi.dsl.yaml_loader import load_yaml_checks
    yaml_str = """
table: catalog.schema.target
compare_to: catalog.schema.expected
reconciliation:
  key: [ticker, date]
  checks:
    - coverage: "> 0.99"
    - column: name
      match_rate: "> 0.99"
    - column: close
      match_rate: "> 0.95"
      tolerance: 0.01
    - column: close
      mean_deviation: "< 0.005"
"""
    result = load_yaml_checks(yaml_str)
    assert len(result.expectations) == 4

    cov = result.expectations[0]
    assert cov.metric == "coverage"
    assert cov.key_columns == ["ticker", "date"]
    assert cov.compare_table == "catalog.schema.expected"

    mr_name = result.expectations[1]
    assert mr_name.metric == "match_rate"
    assert mr_name.column == "name"
    assert mr_name.tolerance == 0.0

    mr_close = result.expectations[2]
    assert mr_close.metric == "match_rate"
    assert mr_close.column == "close"
    assert mr_close.tolerance == 0.01

    md = result.expectations[3]
    assert md.metric == "mean_deviation"
    assert md.threshold == 0.005
    assert md.direction == "below"
