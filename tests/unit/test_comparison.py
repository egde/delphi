"""Tests for comparison metric computation and confidence."""

import pytest
from delphi.runner import _compute_confidence
from delphi.assertions.expectation import Expectation
from delphi.confidence.means import welch_t_confidence_interval


def test_welch_t_basic():
    """Welch's t-test CI for difference between two means."""
    result = welch_t_confidence_interval(
        mean1=100.0, std1=10.0, n1=1000,
        mean2=105.0, std2=10.0, n2=1000,
        confidence=0.95,
    )
    assert result.method == "welch_t"
    assert result.observed == pytest.approx(abs(100 - 105) / 105, abs=0.01)
    assert result.sample_size == 2000


def test_welch_t_threshold_pass():
    """Small difference passes threshold."""
    result = welch_t_confidence_interval(
        mean1=100.0, std1=10.0, n1=10000,
        mean2=101.0, std2=10.0, n2=10000,
        confidence=0.95,
        threshold=0.05, direction="below",
    )
    # ~1% difference, threshold is 5%
    assert result.passed == True


def test_compute_confidence_mean_diff():
    """Runner routes mean_diff to welch_t."""
    exp = Expectation(column="revenue", metric="mean_diff", threshold=0.1, direction="below", confidence=0.95)
    metrics = {
        "target_mean": 1000, "target_std": 50, "target_n": 5000,
        "expected_mean": 1010, "expected_std": 50, "expected_n": 5000,
    }
    result = _compute_confidence(exp, metrics, sample_size=5000)
    assert result.method == "welch_t"


def test_compute_confidence_distribution_shift():
    """Runner routes distribution_shift to KS."""
    exp = Expectation(column="revenue", metric="distribution_shift", threshold=0.5, direction="below", confidence=0.95)
    metrics = {"ks_statistic": 0.05, "p_value": 0.8, "target_n": 5000, "expected_n": 5000}
    result = _compute_confidence(exp, metrics, sample_size=5000)
    assert result.method == "ks"
    assert result.observed == pytest.approx(0.05)
    assert result.passed == True


def test_compute_confidence_row_count_ratio():
    """Runner routes row_count_ratio to exact."""
    exp = Expectation(column=None, metric="row_count_ratio", threshold_low=0.99, threshold_high=1.01, direction="between")
    metrics = {"ratio": 1.002, "target_count": 10000, "expected_count": 9980}
    result = _compute_confidence(exp, metrics, sample_size=10000)
    assert result.passed == True
    assert result.observed == pytest.approx(1.002)


def test_compute_confidence_null_rate_diff():
    """Runner routes null_rate_diff."""
    exp = Expectation(column="x", metric="null_rate_diff", threshold=0.05, direction="below")
    metrics = {"target_rate": 0.01, "expected_rate": 0.008, "diff": 0.002, "target_n": 5000, "expected_n": 5000}
    result = _compute_confidence(exp, metrics, sample_size=5000)
    assert result.passed == True


def test_compute_confidence_schema_match():
    """Runner routes schema_match."""
    exp = Expectation(column=None, metric="schema_match")
    metrics = {"match": True, "target_schema": {"a": "int"}, "expected_schema": {"a": "int"}}
    result = _compute_confidence(exp, metrics, sample_size=0)
    assert result.passed == True
    assert result.observed == 1.0


def test_compute_confidence_schema_mismatch():
    exp = Expectation(column=None, metric="schema_match")
    metrics = {"match": False, "target_schema": {"a": "int"}, "expected_schema": {"a": "string"}}
    result = _compute_confidence(exp, metrics, sample_size=0)
    assert result.passed == False
