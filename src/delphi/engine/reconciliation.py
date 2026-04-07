"""Reconciliation engine — coverage, match rate, and deviation checks."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


RECONCILIATION_METRICS = {"coverage", "match_rate", "mean_deviation"}


def compute_reconciliation_metrics(
    df_target, df_expected, expectations: list[Expectation]
) -> dict[str, dict]:
    """Compute reconciliation metrics between target and expected DataFrames.

    Joins expected against target on key columns, then measures coverage,
    match rate, and deviation.
    """
    results = {}

    for exp in expectations:
        if exp.metric not in RECONCILIATION_METRICS:
            continue

        key = f"{exp.column}:{exp.metric}" if exp.column else exp.metric
        key_columns = exp.key_columns

        if exp.metric == "coverage":
            results[key] = _compute_coverage(df_target, df_expected, key_columns)

        elif exp.metric == "match_rate":
            results[key] = _compute_match_rate(
                df_target, df_expected, key_columns, exp.column, exp.tolerance
            )

        elif exp.metric == "mean_deviation":
            results[key] = _compute_mean_deviation(
                df_target, df_expected, key_columns, exp.column
            )

    return results


def _compute_coverage(df_target, df_expected, key_columns: list[str]) -> dict:
    """Compute fraction of expected rows found in target via left join."""
    from pyspark.sql import functions as F

    expected_count = df_expected.count()
    if expected_count == 0:
        return {"found": 0, "total": 0, "rate": 1.0}

    # Left anti join: expected rows NOT in target
    missing = df_expected.join(df_target, on=key_columns, how="left_anti")
    missing_count = missing.count()
    found_count = expected_count - missing_count

    return {
        "found": found_count,
        "total": expected_count,
        "missing": missing_count,
        "rate": found_count / expected_count,
    }


def _compute_match_rate(
    df_target, df_expected, key_columns: list[str],
    value_column: str, tolerance: float = 0.0,
) -> dict:
    """Compute fraction of matched rows where value column matches.

    If tolerance == 0: exact match.
    If tolerance > 0: abs(target - expected) / abs(expected) <= tolerance.
    """
    from pyspark.sql import functions as F

    # Inner join on keys
    joined = df_expected.alias("exp").join(
        df_target.alias("tgt"), on=key_columns, how="inner"
    )
    matched_count = joined.count()
    if matched_count == 0:
        return {"matches": 0, "total": 0, "rate": 0.0, "mismatches": []}

    exp_col = f"exp.{value_column}"
    tgt_col = f"tgt.{value_column}"

    if tolerance == 0:
        # Exact match
        exact_matches = joined.filter(
            F.col(exp_col).eqNullSafe(F.col(tgt_col))
        ).count()
    else:
        # Within tolerance (relative)
        exact_matches = joined.filter(
            (F.abs(F.col(tgt_col) - F.col(exp_col)) / F.abs(F.col(exp_col))) <= tolerance
        ).count()

    mismatch_count = matched_count - exact_matches

    # Collect sample mismatches for evidence
    mismatches = []
    if mismatch_count > 0:
        if tolerance == 0:
            mismatch_df = joined.filter(
                ~F.col(exp_col).eqNullSafe(F.col(tgt_col))
            )
        else:
            mismatch_df = joined.filter(
                (F.abs(F.col(tgt_col) - F.col(exp_col)) / F.abs(F.col(exp_col))) > tolerance
            )

        mismatch_rows = mismatch_df.select(
            *[F.col(f"exp.{k}").alias(k) for k in key_columns],
            F.col(exp_col).alias(f"expected_{value_column}"),
            F.col(tgt_col).alias(f"actual_{value_column}"),
        ).limit(10).toPandas().to_dict(orient="records")

        # Add deviation column for numeric types
        for row in mismatch_rows:
            exp_val = row.get(f"expected_{value_column}")
            act_val = row.get(f"actual_{value_column}")
            if exp_val is not None and act_val is not None:
                try:
                    dev = abs(float(act_val) - float(exp_val)) / abs(float(exp_val)) if float(exp_val) != 0 else 0
                    row["deviation"] = f"{dev:.2%}"
                except (ValueError, TypeError):
                    row["deviation"] = "N/A"

        mismatches = mismatch_rows

    return {
        "matches": exact_matches,
        "total": matched_count,
        "rate": exact_matches / matched_count,
        "mismatch_count": mismatch_count,
        "mismatches": mismatches,
    }


def _compute_mean_deviation(
    df_target, df_expected, key_columns: list[str], value_column: str,
) -> dict:
    """Compute average relative deviation for a numeric column."""
    from pyspark.sql import functions as F

    joined = df_expected.alias("exp").join(
        df_target.alias("tgt"), on=key_columns, how="inner"
    )
    matched_count = joined.count()
    if matched_count == 0:
        return {"mean_dev": 0.0, "total": 0}

    exp_col = f"exp.{value_column}"
    tgt_col = f"tgt.{value_column}"

    # Mean absolute relative deviation
    stats = joined.agg(
        F.avg(
            F.abs(F.col(tgt_col) - F.col(exp_col)) / F.abs(F.col(exp_col))
        ).alias("mean_dev"),
        F.max(
            F.abs(F.col(tgt_col) - F.col(exp_col)) / F.abs(F.col(exp_col))
        ).alias("max_dev"),
        F.stddev(
            F.abs(F.col(tgt_col) - F.col(exp_col)) / F.abs(F.col(exp_col))
        ).alias("std_dev"),
    ).collect()[0]

    return {
        "mean_dev": float(stats["mean_dev"] or 0),
        "max_dev": float(stats["max_dev"] or 0),
        "std_dev": float(stats["std_dev"] or 0),
        "total": matched_count,
    }
