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


def welch_t_confidence_interval(
    mean1: float, std1: float, n1: int,
    mean2: float, std2: float, n2: int,
    confidence: float = 0.95,
    threshold: float | None = None,
    direction: str | None = None,
) -> ConfidenceResult:
    """Welch's t-test CI for the difference between two means.

    Returns a ConfidenceResult where observed = |mean1 - mean2| / |mean2|
    (fractional difference).
    """
    if n1 <= 1 or n2 <= 1 or mean2 == 0:
        diff = abs(mean1 - mean2) / abs(mean2) if mean2 != 0 else 0
        return ConfidenceResult(
            observed=diff, ci_lower=diff, ci_upper=diff,
            confidence=confidence, method="welch_t", sample_size=n1, passed=False,
        )

    se1 = (std1 ** 2) / n1
    se2 = (std2 ** 2) / n2
    se_diff = (se1 + se2) ** 0.5

    # Welch-Satterthwaite degrees of freedom
    if se1 + se2 > 0:
        df = (se1 + se2) ** 2 / ((se1 ** 2 / (n1 - 1)) + (se2 ** 2 / (n2 - 1)))
    else:
        df = n1 + n2 - 2

    diff = abs(mean1 - mean2)
    frac_diff = diff / abs(mean2)

    t_crit = stats.t.ppf(1 - (1 - confidence) / 2, df=max(df, 1))
    margin = t_crit * se_diff / abs(mean2) if mean2 != 0 else 0

    ci_lower = max(0, frac_diff - margin)
    ci_upper = frac_diff + margin

    passed = _evaluate(ci_lower, ci_upper, threshold, direction, None, None)

    return ConfidenceResult(
        observed=frac_diff, ci_lower=ci_lower, ci_upper=ci_upper,
        confidence=confidence, method="welch_t", sample_size=n1 + n2, passed=bool(passed),
    )
