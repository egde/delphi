"""Test runner — orchestrates the engine pipeline for each test."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from difflib import get_close_matches

from delphi.assertions.expectation import Expectation
from delphi.confidence.result import ConfidenceResult
from delphi.confidence.proportions import wilson_confidence_interval
from delphi.confidence.means import t_confidence_interval, welch_t_confidence_interval
from delphi.config import DelphiConfig
from delphi.engine.prescan import prescan_table
from delphi.engine.sampler import compute_sample_size, sample_dataframe
from delphi.engine.metrics import compute_metrics, compute_comparison_metrics, COMPARISON_METRICS
from delphi.evidence import collect_evidence
from delphi.detect.time_column import detect_time_column


@dataclass
class TestResult:
    test_name: str
    table: str
    status: str  # "pass", "fail", "error", "inconclusive"
    confidence_result: ConfidenceResult | None = None
    threshold: str = ""
    duration_ms: int = 0
    evidence: list[dict] = field(default_factory=list)
    error: str | None = None
    suggestion: str | None = None
    drift: dict | None = None
    baseline_observed: float | None = None


def run_expectations(
    spark,
    table: str,
    expectations: list[Expectation],
    config: DelphiConfig,
    test_name: str = "",
) -> list[TestResult]:
    """Run a batch of expectations against a table through the full pipeline."""
    results = []
    prescan = None

    try:
        prescan = prescan_table(spark, table)
        time_col = detect_time_column(
            prescan, config.time_column_names,
            explicit=config.time_column or None,
        )
        sample_plan = compute_sample_size(
            prescan, confidence=config.default_confidence,
            sample_floor=config.sample_floor,
            sample_ceiling=config.sample_ceiling,
        )
        sampled_df = sample_dataframe(spark, table, sample_plan, time_column=time_col)

        # Split expectations into regular and comparison
        regular_exps = [e for e in expectations if e.metric not in COMPARISON_METRICS]
        compare_exps = [e for e in expectations if e.metric in COMPARISON_METRICS]

        raw_metrics = compute_metrics(sampled_df, regular_exps)
        # row_count should use the full table count, not the sampled count
        if "row_count" in raw_metrics:
            raw_metrics["row_count"]["count"] = prescan.row_count

        # Handle comparison metrics — sample the comparison table too
        if compare_exps:
            compare_tables = {e.compare_table for e in compare_exps if e.compare_table}
            for comp_table in compare_tables:
                comp_prescan = prescan_table(spark, comp_table)
                comp_plan = compute_sample_size(
                    comp_prescan, confidence=config.default_confidence,
                    sample_floor=config.sample_floor,
                    sample_ceiling=config.sample_ceiling,
                )
                comp_df = sample_dataframe(spark, comp_table, comp_plan)
                comp_metrics = compute_comparison_metrics(sampled_df, comp_df, compare_exps)
                raw_metrics.update(comp_metrics)
    except Exception as e:
        for exp in expectations:
            results.append(TestResult(
                test_name=_exp_name(test_name, exp),
                table=table,
                status="error",
                error=str(e),
                suggestion=_suggest_fix(e, prescan, exp),
            ))
        return results

    for exp in expectations:
        start = time.monotonic()
        key = f"{exp.column}:{exp.metric}" if exp.column else exp.metric
        metric_data = raw_metrics.get(key, {})

        try:
            cr = _compute_confidence(exp, metric_data, sample_plan.n)
            status = "pass" if cr.passed else "fail"

            evidence = []
            if status == "fail" and config.evidence_rows > 0:
                evidence = collect_evidence(
                    sampled_df, exp,
                    max_rows=config.evidence_rows,
                    redact_columns=config.redact_columns,
                )

            elapsed_ms = int((time.monotonic() - start) * 1000)
            results.append(TestResult(
                test_name=_exp_name(test_name, exp),
                table=table,
                status=status,
                confidence_result=cr,
                threshold=_format_threshold(exp),
                duration_ms=elapsed_ms,
                evidence=evidence,
            ))
        except Exception as e:
            results.append(TestResult(
                test_name=_exp_name(test_name, exp),
                table=table,
                status="error",
                error=str(e),
            ))

    # Drift detection and history
    if config.enable_history:
        from delphi.history import save_run, load_baseline, detect_drift

        for r in results:
            if r.confidence_result:
                baseline = load_baseline(r.test_name, r.table, config.history_path)
                if baseline:
                    r.baseline_observed = baseline.get("observed")
                    drift = detect_drift(
                        r.confidence_result.observed, baseline, config.drift_threshold
                    )
                    r.drift = drift

        save_run(results, config.history_path)

    return results


def _compute_confidence(
    exp: Expectation, metrics: dict, sample_size: int
) -> ConfidenceResult:
    """Route to the appropriate confidence method based on metric type."""
    if exp.metric == "null_rate":
        return wilson_confidence_interval(
            successes=metrics["null_count"], n=metrics["total"],
            confidence=exp.confidence,
            threshold=exp.threshold, direction=exp.direction,
        )

    if exp.metric == "uniqueness":
        return wilson_confidence_interval(
            successes=metrics["distinct_count"], n=metrics["total"],
            confidence=exp.confidence,
            threshold=exp.threshold, direction=exp.direction,
        )

    if exp.metric == "mean":
        return t_confidence_interval(
            sample_mean=metrics["mean"], sample_std=metrics.get("std", 0) or 0,
            n=metrics["total"], confidence=exp.confidence,
            threshold=exp.threshold, direction=exp.direction,
            threshold_low=exp.threshold_low, threshold_high=exp.threshold_high,
        )

    if exp.metric in ("min", "max", "stddev", "percentile"):
        value = metrics.get("value", 0)
        passed = True
        if exp.direction == "below" and exp.threshold is not None:
            passed = value < exp.threshold
        elif exp.direction == "above" and exp.threshold is not None:
            passed = value > exp.threshold
        return ConfidenceResult(
            observed=float(value), ci_lower=float(value), ci_upper=float(value),
            confidence=exp.confidence, method="exact",
            sample_size=sample_size, passed=bool(passed),
        )

    if exp.metric == "row_count":
        count = metrics.get("count", 0)
        passed = True
        if exp.direction == "above" and exp.threshold is not None:
            passed = count > exp.threshold
        elif exp.direction == "below" and exp.threshold is not None:
            passed = count < exp.threshold
        return ConfidenceResult(
            observed=float(count), ci_lower=float(count), ci_upper=float(count),
            confidence=1.0, method="exact",
            sample_size=sample_size, passed=bool(passed),
        )

    # Comparison metrics
    if exp.metric == "mean_diff":
        return welch_t_confidence_interval(
            mean1=metrics["target_mean"], std1=metrics["target_std"], n1=metrics["target_n"],
            mean2=metrics["expected_mean"], std2=metrics["expected_std"], n2=metrics["expected_n"],
            confidence=exp.confidence,
            threshold=exp.threshold, direction=exp.direction,
        )

    if exp.metric == "distribution_shift":
        ks = metrics["ks_statistic"]
        passed = True
        if exp.direction == "below" and exp.threshold is not None:
            passed = ks < exp.threshold
        elif exp.direction == "above" and exp.threshold is not None:
            passed = ks > exp.threshold
        return ConfidenceResult(
            observed=float(ks), ci_lower=float(ks), ci_upper=float(ks),
            confidence=exp.confidence, method="ks",
            sample_size=metrics.get("target_n", 0), passed=bool(passed),
        )

    if exp.metric == "row_count_ratio":
        ratio = metrics.get("ratio", 0)
        passed = True
        if exp.direction == "between" and exp.threshold_low is not None and exp.threshold_high is not None:
            passed = exp.threshold_low <= ratio <= exp.threshold_high
        elif exp.direction == "below" and exp.threshold is not None:
            passed = ratio < exp.threshold
        elif exp.direction == "above" and exp.threshold is not None:
            passed = ratio > exp.threshold
        return ConfidenceResult(
            observed=float(ratio), ci_lower=float(ratio), ci_upper=float(ratio),
            confidence=1.0, method="exact",
            sample_size=metrics.get("target_count", 0), passed=bool(passed),
        )

    if exp.metric == "null_rate_diff":
        diff = metrics.get("diff", 0)
        passed = True
        if exp.direction == "below" and exp.threshold is not None:
            passed = diff < exp.threshold
        return ConfidenceResult(
            observed=float(diff), ci_lower=float(diff), ci_upper=float(diff),
            confidence=exp.confidence, method="exact",
            sample_size=metrics.get("target_n", 0), passed=bool(passed),
        )

    if exp.metric == "schema_match":
        match = metrics.get("match", False)
        return ConfidenceResult(
            observed=1.0 if match else 0.0,
            ci_lower=1.0 if match else 0.0,
            ci_upper=1.0 if match else 0.0,
            confidence=1.0, method="exact",
            sample_size=0, passed=bool(match),
        )

    raise ValueError(f"Unknown metric: {exp.metric}")


def _exp_name(test_name: str, exp: Expectation) -> str:
    parts = [test_name] if test_name else []
    if exp.column:
        parts.append(f"{exp.column}.{exp.metric}")
    else:
        parts.append(exp.metric)
    return ":".join(parts)


def _format_threshold(exp: Expectation) -> str:
    if exp.direction == "between":
        return f"between {exp.threshold_low} and {exp.threshold_high}"
    elif exp.direction == "below":
        return f"< {exp.threshold}"
    elif exp.direction == "above":
        return f"> {exp.threshold}"
    return ""


def _suggest_fix(error: Exception, prescan, exp: Expectation) -> str | None:
    err_str = str(error).lower()
    if "not found" in err_str or "does not exist" in err_str:
        if exp.column and prescan:
            matches = get_close_matches(exp.column, prescan.columns.keys(), n=1, cutoff=0.6)
            if matches:
                return f'Did you mean "{matches[0]}"?'
    return None
