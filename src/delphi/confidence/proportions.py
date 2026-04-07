"""Wilson score confidence interval for proportions."""

from __future__ import annotations

from scipy import stats

from delphi.confidence.result import ConfidenceResult


def wilson_confidence_interval(
    successes: int,
    n: int,
    confidence: float = 0.95,
    threshold: float | None = None,
    direction: str | None = None,
) -> ConfidenceResult:
    """Compute Wilson score interval for a proportion."""
    if n == 0:
        return ConfidenceResult(
            observed=0.0, ci_lower=0.0, ci_upper=0.0,
            confidence=confidence, method="wilson", sample_size=0, passed=False,
        )

    p_hat = successes / n
    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    z2 = z * z

    denominator = 1 + z2 / n
    center = (p_hat + z2 / (2 * n)) / denominator
    margin = (z / denominator) * ((p_hat * (1 - p_hat) / n) + (z2 / (4 * n * n))) ** 0.5

    ci_lower = max(0.0, center - margin)
    ci_upper = min(1.0, center + margin)

    passed = _evaluate_threshold(ci_lower, ci_upper, threshold, direction)

    return ConfidenceResult(
        observed=p_hat,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        confidence=confidence,
        method="wilson",
        sample_size=n,
        passed=passed,
    )


def _evaluate_threshold(
    ci_lower: float,
    ci_upper: float,
    threshold: float | None,
    direction: str | None,
) -> bool:
    """Evaluate pass/fail: entire CI must satisfy the threshold."""
    if threshold is None or direction is None:
        return True
    if direction == "below":
        return ci_upper < threshold
    if direction == "above":
        return ci_lower > threshold
    return True
