import pytest
from delphi.confidence.proportions import wilson_confidence_interval
from delphi.confidence.result import ConfidenceResult


def test_wilson_basic():
    result = wilson_confidence_interval(successes=300, n=10000, confidence=0.95)
    assert isinstance(result, ConfidenceResult)
    assert result.method == "wilson"
    assert result.observed == pytest.approx(0.03, abs=0.001)
    assert result.ci_lower < 0.03
    assert result.ci_upper > 0.03
    assert result.confidence == 0.95
    assert result.sample_size == 10000


def test_wilson_zero_rate():
    result = wilson_confidence_interval(successes=0, n=1000, confidence=0.95)
    assert result.observed == 0.0
    assert result.ci_lower == pytest.approx(0.0, abs=1e-10)
    assert result.ci_upper > 0.0


def test_wilson_all_successes():
    result = wilson_confidence_interval(successes=1000, n=1000, confidence=0.95)
    assert result.observed == 1.0
    assert result.ci_lower < 1.0
    assert result.ci_upper == 1.0


def test_wilson_threshold_pass():
    result = wilson_confidence_interval(
        successes=10, n=10000, confidence=0.95, threshold=0.01, direction="below"
    )
    assert result.passed == True


def test_wilson_threshold_fail():
    result = wilson_confidence_interval(
        successes=90, n=10000, confidence=0.95, threshold=0.01, direction="below"
    )
    assert result.passed == False
