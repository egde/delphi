"""Adaptive stratified sampling for Delta tables."""

from __future__ import annotations

from dataclasses import dataclass

from scipy import stats

from delphi.engine.prescan import PrescanResult


@dataclass
class SamplePlan:
    n: int
    fraction: float
    use_full_table: bool
    time_column: str | None = None


def compute_sample_size(
    prescan: PrescanResult,
    confidence: float = 0.95,
    sample_floor: int = 1000,
    sample_ceiling: int = 100000,
    margin_of_error: float = 0.01,
) -> SamplePlan:
    """Compute adaptive sample size based on desired confidence and margin of error."""
    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    n_ideal = int((z * z * 0.25) / (margin_of_error * margin_of_error)) + 1

    n = max(sample_floor, min(sample_ceiling, n_ideal))

    total = prescan.row_count
    if total <= n:
        return SamplePlan(n=total, fraction=1.0, use_full_table=True)

    return SamplePlan(n=n, fraction=n / total, use_full_table=False)


def sample_dataframe(spark, table: str, plan: SamplePlan, time_column: str | None = None):
    """Execute sampling against the table, returning a PySpark DataFrame.

    Uses Bernoulli fraction sampling (no full-table sort). A 10% headroom is
    applied so undershoot rarely drops below the sample floor; the actual
    observed row count is used as n downstream.
    """
    df = spark.table(table)

    if plan.use_full_table:
        return df

    fraction = min(1.0, plan.fraction * 1.10)
    return df.sample(withReplacement=False, fraction=fraction)
