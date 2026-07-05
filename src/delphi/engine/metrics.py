"""PySpark metric computation on sampled DataFrames."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


def compute_metrics(df, expectations: list[Expectation]) -> dict[str, dict]:
    """Compute all regular metrics in a single fused aggregation pass.

    Builds one aliased df.agg(...) over every expectation, runs it once, then
    demultiplexes the single result row back into the {col}:{metric} dict shape
    that runner._compute_confidence expects.
    """
    from pyspark.sql import functions as F

    exprs = []
    for i, exp in enumerate(expectations):
        c, m = exp.column, exp.metric
        if m == "null_rate":
            exprs.append(F.sum(F.col(c).isNull().cast("long")).alias(f"nr__{c}"))
        elif m == "uniqueness":
            exprs.append(F.approx_count_distinct(c).alias(f"uq__{c}"))
        elif m == "mean":
            exprs.append(F.avg(c).alias(f"mean__{c}"))
            exprs.append(F.stddev(c).alias(f"std__{c}"))
        elif m == "min":
            exprs.append(F.min(c).alias(f"min__{c}"))
        elif m == "max":
            exprs.append(F.max(c).alias(f"max__{c}"))
        elif m == "stddev":
            exprs.append(F.stddev(c).alias(f"sdev__{c}"))
        elif m == "percentile":
            p = exp.metric_args.get("percentile", 0.5)
            exprs.append(F.percentile_approx(c, p).alias(f"pct__{c}__{i}"))
        # row_count contributes no expression; it uses the shared count below

    # Shared total; also covers the row_count-only case.
    exprs.append(F.count(F.lit(1)).alias("cnt"))
    row = df.agg(*exprs).collect()[0]
    total_count = row["cnt"] or 0

    results = {}
    # NOTE: results are keyed by f"{col}:{metric}", so two expectations sharing the
    # same column+metric (e.g. two percentile checks on one column with different p)
    # collide at the result-dict level — the last one wins. This is a pre-existing
    # limitation of the {col}:{metric} key scheme (also present in runner lookup) and
    # is tracked as a follow-up; out of scope for this performance change.
    for i, exp in enumerate(expectations):
        c, m = exp.column, exp.metric
        key = f"{c}:{m}" if c else m
        if m == "null_rate":
            results[key] = {"null_count": row[f"nr__{c}"] or 0, "total": total_count}
        elif m == "uniqueness":
            results[key] = {"distinct_count": row[f"uq__{c}"] or 0, "total": total_count}
        elif m == "mean":
            results[key] = {"mean": row[f"mean__{c}"] or 0, "std": row[f"std__{c}"] or 0, "total": total_count}
        elif m == "min":
            results[key] = {"value": row[f"min__{c}"] or 0, "total": total_count}
        elif m == "max":
            results[key] = {"value": row[f"max__{c}"] or 0, "total": total_count}
        elif m == "stddev":
            results[key] = {"value": row[f"sdev__{c}"] or 0, "total": total_count}
        elif m == "percentile":
            results[key] = {"value": row[f"pct__{c}__{i}"] or 0, "total": total_count}
        elif m == "row_count":
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
