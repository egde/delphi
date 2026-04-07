"""Dataset-level metric functions — mirrors pyspark.sql.functions as F."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


class _DatasetMetric:
    """A dataset-level metric that supports operator overloading."""

    def __init__(self, metric: str, column: str | None = None, metric_args: dict | None = None):
        self._metric = metric
        self._column = column
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


def row_count() -> _DatasetMetric:
    return _DatasetMetric("row_count")


def approx_percentile(column: str, percentile: float) -> _DatasetMetric:
    return _DatasetMetric("percentile", column=column, metric_args={"percentile": percentile})


def row_count_ratio(compare_ref) -> _DatasetMetric:
    m = _DatasetMetric("row_count_ratio")
    m._compare_table = compare_ref._table if hasattr(compare_ref, '_table') else str(compare_ref)
    return m
