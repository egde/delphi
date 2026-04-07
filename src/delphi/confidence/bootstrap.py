"""Bootstrap confidence interval for arbitrary statistics."""

from __future__ import annotations

from collections.abc import Callable

import numpy as np

from delphi.confidence.result import ConfidenceResult


def bootstrap_confidence_interval(
    data: np.ndarray,
    statistic: Callable[[np.ndarray], float],
    confidence: float = 0.95,
    n_resamples: int = 1000,
    threshold: float | None = None,
    direction: str | None = None,
    seed: int | None = None,
) -> ConfidenceResult:
    """Compute bootstrap confidence interval using percentile method."""
    rng = np.random.default_rng(seed)
    observed = float(statistic(data))
    n = len(data)

    boot_stats = np.empty(n_resamples)
    for i in range(n_resamples):
        sample = rng.choice(data, size=n, replace=True)
        boot_stats[i] = statistic(sample)

    alpha = 1 - confidence
    ci_lower = float(np.percentile(boot_stats, 100 * alpha / 2))
    ci_upper = float(np.percentile(boot_stats, 100 * (1 - alpha / 2)))

    passed = True
    if threshold is not None and direction is not None:
        if direction == "below":
            passed = ci_upper < threshold
        elif direction == "above":
            passed = ci_lower > threshold

    return ConfidenceResult(
        observed=observed,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        confidence=confidence,
        method="bootstrap",
        sample_size=n,
        passed=passed,
    )
