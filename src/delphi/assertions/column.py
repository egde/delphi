"""PySpark-native column assertion expressions."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


class MetricAssertion:
    """A metric bound to a column — supports operator overloading."""

    def __init__(self, column: str | None, metric: str, metric_args: dict | None = None):
        self._column = column
        self._metric = metric
        self._metric_args = metric_args or {}

    def __lt__(self, threshold: float) -> Expectation:
        return Expectation(
            column=self._column, metric=self._metric,
            threshold=threshold, direction="below",
            metric_args=self._metric_args,
            compare_table=getattr(self, "_compare_table", None),
            key_columns=getattr(self, "_key_columns", []),
            tolerance=getattr(self, "_tolerance", 0.0),
        )

    def __gt__(self, threshold: float) -> Expectation:
        return Expectation(
            column=self._column, metric=self._metric,
            threshold=threshold, direction="above",
            metric_args=self._metric_args,
            compare_table=getattr(self, "_compare_table", None),
            key_columns=getattr(self, "_key_columns", []),
            tolerance=getattr(self, "_tolerance", 0.0),
        )

    def between(self, low: float, high: float) -> Expectation:
        return Expectation(
            column=self._column, metric=self._metric,
            threshold_low=low, threshold_high=high, direction="between",
            metric_args=self._metric_args,
            compare_table=getattr(self, "_compare_table", None),
            key_columns=getattr(self, "_key_columns", []),
            tolerance=getattr(self, "_tolerance", 0.0),
        )


class ColumnAssertion:
    """Entry point from col('name') — provides metric properties.

    Supports single column: col("revenue")
    Or multi-column keys: col("ticker", "date")
    """

    def __init__(self, *names: str):
        self._names = list(names)
        self._name = names[0] if len(names) == 1 else None

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

    # --- Reconciliation methods ---

    def coverage(self, compare_ref) -> MetricAssertion:
        """Check what fraction of expected rows (from compare_ref) exist in the target.

        Uses self._names as the join key columns.
        """
        ma = MetricAssertion(None, "coverage")
        ma._compare_table = compare_ref._table if hasattr(compare_ref, '_table') else str(compare_ref)
        ma._key_columns = self._names
        return ma

    def match_rate(self, compare_ref, key: list[str] | None = None, tolerance: float = 0.0) -> MetricAssertion:
        """Check what fraction of matched rows have identical (or within-tolerance) values.

        Args:
            compare_ref: Reference table to compare against.
            key: Join key columns. Defaults to self._names if multi-column.
            tolerance: For numeric columns, max allowed relative deviation (0 = exact).
        """
        key_cols = key or self._names
        ma = MetricAssertion(self._name, "match_rate")
        ma._compare_table = compare_ref._table if hasattr(compare_ref, '_table') else str(compare_ref)
        ma._key_columns = key_cols
        ma._tolerance = tolerance
        return ma

    def mean_deviation(self, compare_ref, key: list[str] | None = None) -> MetricAssertion:
        """Average relative deviation for a numeric column across matched rows.

        Args:
            compare_ref: Reference table to compare against.
            key: Join key columns.
        """
        key_cols = key or self._names
        ma = MetricAssertion(self._name, "mean_deviation")
        ma._compare_table = compare_ref._table if hasattr(compare_ref, '_table') else str(compare_ref)
        ma._key_columns = key_cols
        return ma


def col(*names: str) -> ColumnAssertion:
    """Create a column assertion expression — mirrors pyspark.sql.functions.col.

    Single column: col("revenue")
    Multi-column key: col("ticker", "date") — used for reconciliation checks
    """
    return ColumnAssertion(*names)
