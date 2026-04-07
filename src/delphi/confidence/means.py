"""t-distribution confidence interval for means."""

from __future__ import annotations

from scipy import stats

from delphi.confidence.result import ConfidenceResult


def t_confidence_interval(
    sample_mean: float,
    sample_std: float,
    n: int,
    confidence: float = 0.95,
    threshold: float | None = None,
    direction: str | None = None,
    threshold_low: float | None = None,
    threshold_high: float | None = None,
) -> ConfidenceResult:
    """Compute t-distribution confidence interval for a mean."""
    if n <= 1:
        return ConfidenceResult(
            observed=sample_mean, ci_lower=sample_mean, ci_upper=sample_mean,
            confidence=confidence, method="t", sample_size=n, passed=False,
        )

    se = sample_std / (n ** 0.5)
    t_crit = stats.t.ppf(1 - (1 - confidence) / 2, df=n - 1)
    margin = t_crit * se

    ci_lower = sample_mean - margin
    ci_upper = sample_mean + margin

    passed = _evaluate(ci_lower, ci_upper, threshold, direction, threshold_low, threshold_high)

    return ConfidenceResult(
        observed=sample_mean,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        confidence=confidence,
        method="t",
        sample_size=n,
        passed=passed,
    )


def _evaluate(
    ci_lower: float,
    ci_upper: float,
    threshold: float | None,
    direction: str | None,
    threshold_low: float | None,
    threshold_high: float | None,
) -> bool:
    if threshold_low is not None and threshold_high is not None:
        return ci_lower >= threshold_low and ci_upper <= threshold_high
    if threshold is not None and direction is not None:
        if direction == "below":
            return ci_upper < threshold
        if direction == "above":
            return ci_lower > threshold
    return True
