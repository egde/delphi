import numpy as np
import pytest
from delphi.confidence.bootstrap import bootstrap_confidence_interval
from delphi.confidence.result import ConfidenceResult


def test_bootstrap_basic():
    rng = np.random.default_rng(42)
    data = rng.normal(loc=100, scale=10, size=1000)
    result = bootstrap_confidence_interval(data, statistic=np.median, confidence=0.95)
    assert isinstance(result, ConfidenceResult)
    assert result.method == "bootstrap"
    assert result.ci_lower < 100
    assert result.ci_upper > 100
    assert result.sample_size == 1000


def test_bootstrap_narrow_with_low_variance():
    data = np.full(1000, 50.0)
    result = bootstrap_confidence_interval(data, statistic=np.mean, confidence=0.95)
    assert result.ci_lower == pytest.approx(50.0)
    assert result.ci_upper == pytest.approx(50.0)


def test_bootstrap_threshold_pass():
    rng = np.random.default_rng(42)
    data = rng.normal(loc=5, scale=0.5, size=1000)
    result = bootstrap_confidence_interval(
        data, statistic=np.mean, confidence=0.95,
        threshold=10.0, direction="below",
    )
    assert result.passed is True
