import pytest
from delphi.confidence.means import t_confidence_interval
from delphi.confidence.result import ConfidenceResult


def test_t_interval_basic():
    result = t_confidence_interval(sample_mean=100.0, sample_std=10.0, n=1000, confidence=0.95)
    assert isinstance(result, ConfidenceResult)
    assert result.method == "t"
    assert result.observed == pytest.approx(100.0)
    assert result.ci_lower < 100.0
    assert result.ci_upper > 100.0
    assert result.sample_size == 1000


def test_t_interval_narrow_with_large_n():
    small = t_confidence_interval(sample_mean=50.0, sample_std=10.0, n=100, confidence=0.95)
    large = t_confidence_interval(sample_mean=50.0, sample_std=10.0, n=10000, confidence=0.95)
    assert (large.ci_upper - large.ci_lower) < (small.ci_upper - small.ci_lower)


def test_t_threshold_between_pass():
    result = t_confidence_interval(
        sample_mean=3000.0, sample_std=100.0, n=10000, confidence=0.95,
        threshold_low=1000.0, threshold_high=5000.0,
    )
    assert result.passed == True


def test_t_threshold_between_fail():
    result = t_confidence_interval(
        sample_mean=4990.0, sample_std=100.0, n=100, confidence=0.95,
        threshold_low=1000.0, threshold_high=5000.0,
    )
    assert result.passed == False
