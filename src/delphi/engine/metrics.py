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


COMPARISON_METRICS = {"mean_diff", "distribution_shift", "row_count_ratio", "null_rate_diff", "schema_match"}


def compute_comparison_metrics(
    df_target, df_expected, expectations: list[Expectation]
) -> dict[str, dict]:
    """Compute comparison metrics between two DataFrames."""
    results = {}
    target_count = df_target.count()
    expected_count = df_expected.count()

    for exp in expectations:
        if exp.metric not in COMPARISON_METRICS:
            continue

        key = f"{exp.column}:{exp.metric}" if exp.column else exp.metric

        if exp.metric == "mean_diff":
            from pyspark.sql import functions as F
            t_row = df_target.agg(
                F.avg(exp.column).alias("mean"), F.stddev(exp.column).alias("std"),
            ).collect()[0]
            e_row = df_expected.agg(
                F.avg(exp.column).alias("mean"), F.stddev(exp.column).alias("std"),
            ).collect()[0]
            t_mean, t_std = t_row["mean"] or 0, t_row["std"] or 0
            e_mean, e_std = e_row["mean"] or 0, e_row["std"] or 0
            diff = abs(t_mean - e_mean) / e_mean if e_mean != 0 else 0
            results[key] = {
                "target_mean": t_mean, "target_std": t_std, "target_n": target_count,
                "expected_mean": e_mean, "expected_std": e_std, "expected_n": expected_count,
                "diff": diff,
            }

        elif exp.metric == "distribution_shift":
            # Collect column values for KS test
            t_vals = [row[0] for row in df_target.select(exp.column).collect()]
            e_vals = [row[0] for row in df_expected.select(exp.column).collect()]
            from scipy.stats import ks_2samp
            stat, p_value = ks_2samp(t_vals, e_vals)
            results[key] = {
                "ks_statistic": stat, "p_value": p_value,
                "target_n": target_count, "expected_n": expected_count,
            }

        elif exp.metric == "row_count_ratio":
            ratio = target_count / expected_count if expected_count > 0 else 0
            results[key] = {"ratio": ratio, "target_count": target_count, "expected_count": expected_count}

        elif exp.metric == "null_rate_diff":
            t_nulls = df_target.filter(df_target[exp.column].isNull()).count()
            e_nulls = df_expected.filter(df_expected[exp.column].isNull()).count()
            t_rate = t_nulls / target_count if target_count > 0 else 0
            e_rate = e_nulls / expected_count if expected_count > 0 else 0
            results[key] = {
                "target_rate": t_rate, "expected_rate": e_rate,
                "diff": abs(t_rate - e_rate),
                "target_n": target_count, "expected_n": expected_count,
            }

        elif exp.metric == "schema_match":
            t_schema = {f.name: f.dataType.simpleString() for f in df_target.schema.fields}
            e_schema = {f.name: f.dataType.simpleString() for f in df_expected.schema.fields}
            results[key] = {"match": t_schema == e_schema, "target_schema": t_schema, "expected_schema": e_schema}

    return results
