import pytest
from delphi.runner import _compute_confidence, _format_threshold, _exp_name
from delphi.assertions.expectation import Expectation
from delphi.confidence.result import ConfidenceResult


def test_compute_confidence_null_rate():
    exp = Expectation(column="x", metric="null_rate", threshold=0.05, direction="below", confidence=0.95)
    metrics = {"null_count": 10, "total": 1000}
    result = _compute_confidence(exp, metrics, sample_size=1000)
    assert isinstance(result, ConfidenceResult)
    assert result.method == "wilson"
    assert result.observed == pytest.approx(0.01, abs=0.001)


def test_compute_confidence_mean():
    exp = Expectation(column="x", metric="mean", threshold_low=90, threshold_high=110, direction="between", confidence=0.95)
    metrics = {"mean": 100.0, "std": 5.0, "total": 1000}
    result = _compute_confidence(exp, metrics, sample_size=1000)
    assert result.method == "t"


def test_compute_confidence_row_count():
    exp = Expectation(column=None, metric="row_count", threshold=100, direction="above")
    metrics = {"count": 5000}
    result = _compute_confidence(exp, metrics, sample_size=5000)
    assert result.method == "exact"
    assert result.passed == True


def test_compute_confidence_unknown_metric():
    exp = Expectation(column="x", metric="unknown_metric")
    with pytest.raises(ValueError, match="Unknown metric"):
        _compute_confidence(exp, {}, sample_size=100)


def test_format_threshold_below():
    exp = Expectation(column="x", metric="null_rate", threshold=0.01, direction="below")
    assert _format_threshold(exp) == "< 0.01"


def test_format_threshold_between():
    exp = Expectation(column="x", metric="mean", threshold_low=1000, threshold_high=5000, direction="between")
    assert _format_threshold(exp) == "between 1000 and 5000"


def test_exp_name():
    exp = Expectation(column="revenue", metric="null_rate")
    assert _exp_name("test_fn", exp) == "test_fn:revenue.null_rate"
