"""PySpark-native column assertion expressions."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


class MetricAssertion:
    """A metric bound to a column — supports operator overloading."""

    def __init__(self, column: str, metric: str, metric_args: dict | None = None):
        self._column = column
        self._metric = metric
        self._metric_args = metric_args or {}

    def __lt__(self, threshold: float) -> Expectation:
        return Expectation(
            column=self._column, metric=self._metric,
            threshold=threshold, direction="below",
            metric_args=self._metric_args,
        )

    def __gt__(self, threshold: float) -> Expectation:
        return Expectation(
            column=self._column, metric=self._metric,
            threshold=threshold, direction="above",
            metric_args=self._metric_args,
        )

    def between(self, low: float, high: float) -> Expectation:
        return Expectation(
            column=self._column, metric=self._metric,
            threshold_low=low, threshold_high=high, direction="between",
            metric_args=self._metric_args,
        )


class ColumnAssertion:
    """Entry point from col('name') — provides metric properties."""

    def __init__(self, name: str):
        self._name = name

    @property
    def null_rate(self) -> MetricAssertion:
        return MetricAssertion(self._name, "null_rate")

    @property
    def uniqueness(self) -> MetricAssertion:
        return MetricAssertion(self._name, "uniqueness")

    @property
    def mean(self) -> MetricAssertion:
        return MetricAssertion(self._name, "mean")

    @property
    def min(self) -> MetricAssertion:
        return MetricAssertion(self._name, "min")

    @property
    def max(self) -> MetricAssertion:
        return MetricAssertion(self._name, "max")

    @property
    def stddev(self) -> MetricAssertion:
        return MetricAssertion(self._name, "stddev")

    def percentile(self, p: float) -> MetricAssertion:
        return MetricAssertion(self._name, "percentile", {"percentile": p})

    def mean_diff(self, compare_ref) -> MetricAssertion:
        ma = MetricAssertion(self._name, "mean_diff")
        ma._compare_table = compare_ref._table if hasattr(compare_ref, '_table') else str(compare_ref)
        return ma

    def distribution_shift(self, compare_ref) -> MetricAssertion:
        ma = MetricAssertion(self._name, "distribution_shift")
        ma._compare_table = compare_ref._table if hasattr(compare_ref, '_table') else str(compare_ref)
        return ma


def col(name: str) -> ColumnAssertion:
    """Create a column assertion expression — mirrors pyspark.sql.functions.col."""
    return ColumnAssertion(name)
