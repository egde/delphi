"""PySpark metric computation on sampled DataFrames."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


def compute_metrics(df, expectations: list[Expectation]) -> dict[str, dict]:
    """Compute all required metrics from a sampled DataFrame."""
    results = {}
    total_count = df.count()

    for exp in expectations:
        key = f"{exp.column}:{exp.metric}" if exp.column else exp.metric

        if exp.metric == "null_rate":
            null_count = df.filter(df[exp.column].isNull()).count()
            results[key] = {"null_count": null_count, "total": total_count}

        elif exp.metric == "uniqueness":
            distinct_count = df.select(exp.column).distinct().count()
            results[key] = {"distinct_count": distinct_count, "total": total_count}

        elif exp.metric == "mean":
            from pyspark.sql import functions as F
            row = df.agg(
                F.avg(exp.column).alias("mean"),
                F.stddev(exp.column).alias("std"),
            ).collect()[0]
            results[key] = {"mean": row["mean"], "std": row["std"], "total": total_count}

        elif exp.metric in ("min", "max"):
            from pyspark.sql import functions as F
            func = F.min if exp.metric == "min" else F.max
            row = df.agg(func(exp.column).alias("value")).collect()[0]
            results[key] = {"value": row["value"], "total": total_count}

        elif exp.metric == "stddev":
            from pyspark.sql import functions as F
            row = df.agg(F.stddev(exp.column).alias("value")).collect()[0]
            results[key] = {"value": row["value"], "total": total_count}

        elif exp.metric == "percentile":
            from pyspark.sql import functions as F
            p = exp.metric_args.get("percentile", 0.5)
            row = df.agg(
                F.percentile_approx(exp.column, p).alias("value")
            ).collect()[0]
            results[key] = {"value": row["value"], "total": total_count}

        elif exp.metric == "row_count":
            results[key] = {"count": total_count}

    return results
