"""Tests for comparison metric computation and confidence."""

from unittest.mock import MagicMock

import pytest
from delphi.runner import _compute_confidence
from delphi.engine.metrics import compute_comparison_metrics
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


def _mock_side(agg_row: dict):
    df = MagicMock()
    df.agg.return_value.collect.return_value = [agg_row]
    return df


def test_mean_diff_uses_single_agg_per_side():
    exp = Expectation(column="revenue", metric="mean_diff", threshold=0.05,
                      direction="below", compare_table="other")
    df_t = _mock_side({"mean__revenue": 100.0, "std__revenue": 5.0, "cnt": 1000})
    df_e = _mock_side({"mean__revenue": 98.0, "std__revenue": 4.0, "cnt": 900})

    results = compute_comparison_metrics(df_t, df_e, [exp])

    df_t.agg.assert_called_once()
    df_e.agg.assert_called_once()
    m = results["revenue:mean_diff"]
    assert m["target_mean"] == 100.0
    assert m["target_n"] == 1000
    assert m["expected_mean"] == 98.0
    assert m["expected_n"] == 900


def test_null_rate_diff_reads_fused_agg_row():
    exp = Expectation(column="email", metric="null_rate_diff", threshold=0.02,
                      direction="below", compare_table="other")
    df_t = _mock_side({"nr__email": 30, "cnt": 1000})   # 3% nulls
    df_e = _mock_side({"nr__email": 10, "cnt": 1000})   # 1% nulls

    results = compute_comparison_metrics(df_t, df_e, [exp])

    df_t.agg.assert_called_once()
    df_e.agg.assert_called_once()
    m = results["email:null_rate_diff"]
    assert m["target_rate"] == 0.03
    assert m["expected_rate"] == 0.01
    assert m["diff"] == pytest.approx(0.02)
    assert m["target_n"] == 1000
    assert m["expected_n"] == 1000


def test_multiple_comparison_metrics_one_agg_per_side():
    exps = [
        Expectation(column="revenue", metric="mean_diff", threshold=0.05,
                    direction="below", compare_table="other"),
        Expectation(column="email", metric="null_rate_diff", threshold=0.02,
                    direction="below", compare_table="other"),
    ]
    df_t = _mock_side({"mean__revenue": 100.0, "std__revenue": 5.0,
                       "nr__email": 20, "cnt": 1000})
    df_e = _mock_side({"mean__revenue": 98.0, "std__revenue": 4.0,
                       "nr__email": 10, "cnt": 1000})

    results = compute_comparison_metrics(df_t, df_e, exps)

    # both metrics computed from a SINGLE agg per side
    df_t.agg.assert_called_once()
    df_e.agg.assert_called_once()
    assert results["revenue:mean_diff"]["target_mean"] == 100.0
    assert results["email:null_rate_diff"]["target_rate"] == 0.02
