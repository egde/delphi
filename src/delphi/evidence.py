"""Collect violating rows as evidence for failed tests."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


def collect_evidence(
    df,
    expectation: Expectation,
    max_rows: int = 10,
    redact_columns: list[str] | None = None,
) -> list[dict]:
    """Filter the sampled DataFrame for rows violating the expectation."""
    violation_df = _build_violation_filter(df, expectation)
    rows = violation_df.limit(max_rows).toPandas().to_dict(orient="records")

    if redact_columns:
        for row in rows:
            for col_name in redact_columns:
                if col_name in row:
                    row[col_name] = "[REDACTED]"

    return rows


def _build_violation_filter(df, expectation: Expectation):
    """Build a filter expression for rows that violate the expectation."""
    col = expectation.column
    metric = expectation.metric

    if metric == "null_rate":
        return df.filter(df[col].isNull())
    elif metric in ("mean", "min", "max", "stddev"):
        return df.filter(df[col].isNotNull())
    elif metric == "uniqueness":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        window = F.count("*").over(Window.partitionBy(col))
        return df.withColumn("_dup_count", window).filter(F.col("_dup_count") > 1).drop("_dup_count")
    else:
        return df
