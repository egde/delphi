# Single-Pass Sampling Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut regular-metric Spark jobs from N+1 to ~2 by fraction-sampling once, caching the sample, and computing every metric in a single fused `df.agg(...)` pass.

**Architecture:** Replace `orderBy(rand()).limit(n)` with `df.sample(fraction)` (no total sort). The runner caches the materialized sample and reuses it for both metrics and evidence (fixing a re-randomization bug). `compute_metrics` builds one aliased aggregation over all regular expectations and demultiplexes the single result row back into the existing `{col}:{metric}` dict, so `runner._compute_confidence` is untouched. Uniqueness uses HLL (`approx_count_distinct`) to stay inside the fused pass.

**Tech Stack:** Python 3.11+, PySpark (`pyspark.sql.functions`), pytest with mocked Spark (`unittest.mock.MagicMock`).

---

## File Structure

- `src/delphi/engine/sampler.py` — MODIFY `sample_dataframe` to fraction-sample.
- `src/delphi/engine/metrics.py` — REWRITE `compute_metrics` (fused agg) and `compute_comparison_metrics` (fused per-side agg).
- `src/delphi/runner.py` — MODIFY `run_expectations` to cache samples and unpersist in a `finally`.
- `tests/unit/engine/test_sampler.py` — ADD fraction-sampling test.
- `tests/unit/engine/test_metrics.py` — REWRITE mocks for the fused-agg call shape; ADD multi-metric fusion test.
- `tests/unit/test_runner.py` — ADD cache/unpersist lifecycle test.
- `tests/integration_delphi_dsl.py` — ADD before/after timing cell against `delphi.default.prices`.

---

## Task 1: Fraction-based sampling

**Files:**
- Modify: `src/delphi/engine/sampler.py:40-48`
- Test: `tests/unit/engine/test_sampler.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/engine/test_sampler.py`:

```python
import pytest
from unittest.mock import MagicMock
from delphi.engine.sampler import sample_dataframe


def test_sample_dataframe_uses_fraction_not_sort():
    df = MagicMock()
    spark = MagicMock()
    spark.table.return_value = df
    plan = SamplePlan(n=1000, fraction=0.001, use_full_table=False)

    sample_dataframe(spark, "t", plan)

    df.sample.assert_called_once()
    _, kwargs = df.sample.call_args
    assert kwargs["withReplacement"] is False
    # 10% headroom applied over plan.fraction
    assert kwargs["fraction"] == pytest.approx(0.0011)
    df.orderBy.assert_not_called()


def test_sample_dataframe_full_table_returns_df_unchanged():
    df = MagicMock()
    spark = MagicMock()
    spark.table.return_value = df
    plan = SamplePlan(n=100, fraction=1.0, use_full_table=True)

    assert sample_dataframe(spark, "t", plan) is df
    df.sample.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/engine/test_sampler.py::test_sample_dataframe_uses_fraction_not_sort -v`
Expected: FAIL — current code calls `df.orderBy(...)`, so `df.sample` is never called (AssertionError on `assert_called_once`).

- [ ] **Step 3: Write minimal implementation**

Replace `sample_dataframe` in `src/delphi/engine/sampler.py` (lines 40-48):

```python
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
```

Remove the now-unused `from pyspark.sql.functions import rand` import inside the function (it was local to the old body — confirm no module-level `rand` import remains).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/engine/test_sampler.py -v`
Expected: PASS (all sampler tests, including the two new ones).

- [ ] **Step 5: Commit**

```bash
git add src/delphi/engine/sampler.py tests/unit/engine/test_sampler.py
git commit -m "perf: fraction-based sampling to eliminate full-table sort"
```

---

## Task 2: Fused metric aggregation

**Files:**
- Modify: `src/delphi/engine/metrics.py:8-54` (rewrite `compute_metrics`)
- Test: `tests/unit/engine/test_metrics.py` (rewrite existing mocks + add fusion test)

**Alias scheme** (prefix encodes metric so multiple metrics on the same column never collide):
`nr__{col}` null_rate · `uq__{col}` uniqueness · `mean__{col}` + `std__{col}` mean · `min__{col}` · `max__{col}` · `sdev__{col}` stddev · `pct__{col}` percentile · `cnt` shared total count.

- [ ] **Step 1: Write the failing tests**

Replace the entire contents of `tests/unit/engine/test_metrics.py`:

```python
from unittest.mock import MagicMock
from delphi.engine.metrics import compute_metrics
from delphi.assertions.expectation import Expectation


def _mock_df(agg_row: dict):
    """A MagicMock df whose single .agg(...).collect()[0] returns agg_row."""
    df = MagicMock()
    df.agg.return_value.collect.return_value = [agg_row]
    return df


def test_compute_null_rate():
    expectations = [
        Expectation(column="revenue", metric="null_rate", threshold=0.01, direction="below"),
    ]
    df = _mock_df({"nr__revenue": 30, "cnt": 1000})

    results = compute_metrics(df, expectations)

    assert results["revenue:null_rate"]["null_count"] == 30
    assert results["revenue:null_rate"]["total"] == 1000
    # single fused aggregation, not per-metric filter/count
    df.agg.assert_called_once()
    df.filter.assert_not_called()


def test_compute_row_count_uses_shared_count():
    expectations = [
        Expectation(column=None, metric="row_count", threshold=1000, direction="above"),
    ]
    df = _mock_df({"cnt": 5000})

    results = compute_metrics(df, expectations)
    assert results["row_count"]["count"] == 5000


def test_multiple_metrics_single_agg():
    expectations = [
        Expectation(column="revenue", metric="null_rate", threshold=0.01, direction="below"),
        Expectation(column="revenue", metric="mean", threshold=100, direction="below"),
        Expectation(column="user_id", metric="uniqueness", threshold=0.9, direction="above"),
        Expectation(column="price", metric="min", threshold=0, direction="above"),
        Expectation(column="price", metric="max", threshold=999, direction="below"),
    ]
    df = _mock_df({
        "nr__revenue": 5, "mean__revenue": 42.0, "std__revenue": 3.0,
        "uq__user_id": 950, "min__price": 1.0, "max__price": 998.0, "cnt": 1000,
    })

    results = compute_metrics(df, expectations)

    # exactly ONE aggregation for the whole batch
    df.agg.assert_called_once()
    assert results["revenue:null_rate"] == {"null_count": 5, "total": 1000}
    assert results["revenue:mean"] == {"mean": 42.0, "std": 3.0, "total": 1000}
    assert results["user_id:uniqueness"] == {"distinct_count": 950, "total": 1000}
    assert results["price:min"]["value"] == 1.0
    assert results["price:max"]["value"] == 998.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/engine/test_metrics.py -v`
Expected: FAIL — current `compute_metrics` calls `df.count()` / `df.filter(...)`, so `df.agg.assert_called_once()` fails and the mocked agg row is never read.

- [ ] **Step 3: Write the implementation**

Replace `compute_metrics` (lines 8-54) in `src/delphi/engine/metrics.py`. Keep the `COMPARISON_METRICS` set and `compute_comparison_metrics` below it unchanged for now (Task 4 revisits the comparison function):

```python
def compute_metrics(df, expectations: list[Expectation]) -> dict[str, dict]:
    """Compute all regular metrics in a single fused aggregation pass.

    Builds one aliased df.agg(...) over every expectation, runs it once, then
    demultiplexes the single result row back into the {col}:{metric} dict shape
    that runner._compute_confidence expects.
    """
    from pyspark.sql import functions as F

    exprs = []
    for exp in expectations:
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
            exprs.append(F.percentile_approx(c, p).alias(f"pct__{c}"))
        # row_count contributes no expression; it uses the shared count below

    # Shared total; also covers the row_count-only case.
    exprs.append(F.count(F.lit(1)).alias("cnt"))
    row = df.agg(*exprs).collect()[0]
    total_count = row["cnt"] or 0

    results = {}
    for exp in expectations:
        c, m = exp.column, exp.metric
        key = f"{c}:{m}" if c else m
        if m == "null_rate":
            results[key] = {"null_count": row[f"nr__{c}"] or 0, "total": total_count}
        elif m == "uniqueness":
            results[key] = {"distinct_count": row[f"uq__{c}"] or 0, "total": total_count}
        elif m == "mean":
            results[key] = {"mean": row[f"mean__{c}"], "std": row[f"std__{c}"], "total": total_count}
        elif m == "min":
            results[key] = {"value": row[f"min__{c}"], "total": total_count}
        elif m == "max":
            results[key] = {"value": row[f"max__{c}"], "total": total_count}
        elif m == "stddev":
            results[key] = {"value": row[f"sdev__{c}"], "total": total_count}
        elif m == "percentile":
            results[key] = {"value": row[f"pct__{c}"], "total": total_count}
        elif m == "row_count":
            results[key] = {"count": total_count}

    return results
```

Note: the runner still overrides `row_count`'s `count` with `prescan.row_count` (runner.py:68-69), so the sampled `cnt` here is a harmless placeholder for that metric.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/engine/test_metrics.py -v`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add src/delphi/engine/metrics.py tests/unit/engine/test_metrics.py
git commit -m "perf: compute all regular metrics in one fused aggregation"
```

---

## Task 3: Cache the sample and unpersist in the runner

**Files:**
- Modify: `src/delphi/runner.py:44-159` (wrap `run_expectations` body)
- Test: `tests/unit/test_runner.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_runner.py`:

```python
from unittest.mock import MagicMock, patch
from delphi.runner import run_expectations
from delphi.config import DelphiConfig
from delphi.engine.sampler import SamplePlan
from delphi.engine.prescan import PrescanResult


def _prescan():
    return PrescanResult(
        table="t", row_count=1_000_000, partition_columns=[],
        clustering_columns=[], columns={},
    )


def test_run_expectations_caches_and_unpersists_sample():
    exp = Expectation(column="x", metric="null_rate", threshold=0.05, direction="below")
    sample_df = MagicMock()
    # enable_history=False so the runner skips file I/O (history branch).
    config = DelphiConfig(enable_history=False)

    with patch("delphi.runner.prescan_table", return_value=_prescan()), \
         patch("delphi.runner.detect_time_column", return_value=None), \
         patch("delphi.runner.compute_sample_size",
               return_value=SamplePlan(n=1000, fraction=0.001, use_full_table=False)), \
         patch("delphi.runner.sample_dataframe", return_value=sample_df), \
         patch("delphi.runner.compute_metrics",
               return_value={"x:null_rate": {"null_count": 10, "total": 1000}}):
        results = run_expectations(MagicMock(), "t", [exp], config)

    sample_df.cache.assert_called_once()
    sample_df.unpersist.assert_called_once()
    assert results[0].status in ("pass", "fail")
```

`DelphiConfig` is a dataclass with all-default fields; `DelphiConfig(enable_history=False)` is valid as written. Because `null_count=10 / total=1000 = 0.01 < 0.05`, the expectation passes, so the failure-only evidence branch is never entered (no evidence collection against the MagicMock).

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_runner.py::test_run_expectations_caches_and_unpersists_sample -v`
Expected: FAIL — current runner never calls `.cache()`/`.unpersist()` (AssertionError on `cache.assert_called_once`).

- [ ] **Step 3: Implement caching + unpersist**

In `src/delphi/runner.py`, make three edits inside `run_expectations`:

(a) Immediately after `sampled_df = sample_dataframe(...)` (line 59), add:

```python
        sampled_df.cache()
```

(b) Introduce a tracking variable so the `finally` can reach it. Change the top of the function (after `results = []` / `prescan = None`, around line 46-47) to also initialize:

```python
    results = []
    prescan = None
    sampled_df = None
```

(c) Wrap the existing body from the first `try:` through the `return results` at the end of the function in an outer `try/finally` that unpersists. Concretely, keep all existing logic, and add a `finally` at the very end of the function that runs before every return path:

```python
    try:
        # ... entire existing body of run_expectations, unchanged ...
        # (setup try/except, per-exp loop, history block)
        return results
    finally:
        if sampled_df is not None:
            try:
                sampled_df.unpersist()
            except Exception:
                pass
```

Because a `finally` runs on every `return` (including the early `return results` in the setup `except` block), both exit paths unpersist. Ensure `sampled_df` is assigned to the outer-scope variable (not a new local) — it already is, since it is a plain assignment inside the same function.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_runner.py -v`
Expected: PASS (existing runner tests plus the new lifecycle test).

- [ ] **Step 5: Commit**

```bash
git add src/delphi/runner.py tests/unit/test_runner.py
git commit -m "perf: cache sampled DataFrame and unpersist after use"
```

---

## Task 4: Fuse comparison metrics and cache comparison samples

**Files:**
- Modify: `src/delphi/engine/metrics.py` (`compute_comparison_metrics`, lines ~60-122)
- Modify: `src/delphi/runner.py` (comparison block, lines ~72-83)
- Test: `tests/unit/test_comparison.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_comparison.py` (match the existing import style in that file):

```python
from unittest.mock import MagicMock
from delphi.engine.metrics import compute_comparison_metrics
from delphi.assertions.expectation import Expectation


def _mock_side(agg_row: dict):
    df = MagicMock()
    df.agg.return_value.collect.return_value = [agg_row]
    return df


def test_mean_diff_uses_single_agg_per_side():
    exp = Expectation(column="revenue", metric="mean_diff", threshold=0.05,
                      direction="below", compare_table="other")
    df_t = _mock_side({"mean__revenue": 100.0, "std__revenue": 5.0, "cnt": 1000})
    df_e = _mock_side({"mean__revenue": 98.0, "std__revenue": 4.0, "cnt": 900})

    results = compute_comparison_metrics(df_t, df_e, [exp])

    df_t.agg.assert_called_once()
    df_e.agg.assert_called_once()
    m = results["revenue:mean_diff"]
    assert m["target_mean"] == 100.0
    assert m["target_n"] == 1000
    assert m["expected_mean"] == 98.0
    assert m["expected_n"] == 900
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_comparison.py::test_mean_diff_uses_single_agg_per_side -v`
Expected: FAIL — current `compute_comparison_metrics` calls `.count()` separately, so the mocked agg row shape does not match and/or `agg` is called differently.

- [ ] **Step 3: Implement fused per-side comparison aggregation**

In `src/delphi/engine/metrics.py`, rewrite `compute_comparison_metrics` so each side computes its aggregates in one pass. Replace the function body with:

```python
def compute_comparison_metrics(
    df_target, df_expected, expectations: list[Expectation]
) -> dict[str, dict]:
    """Compute comparison metrics between two DataFrames, one agg pass per side."""
    from pyspark.sql import functions as F

    def _side_aggs(df, exps):
        exprs = []
        for exp in exps:
            c, m = exp.column, exp.metric
            if m == "mean_diff":
                exprs.append(F.avg(c).alias(f"mean__{c}"))
                exprs.append(F.stddev(c).alias(f"std__{c}"))
            elif m == "null_rate_diff":
                exprs.append(F.sum(F.col(c).isNull().cast("long")).alias(f"nr__{c}"))
        exprs.append(F.count(F.lit(1)).alias("cnt"))
        return df.agg(*exprs).collect()[0]

    relevant = [e for e in expectations if e.metric in COMPARISON_METRICS]
    t_row = _side_aggs(df_target, relevant)
    e_row = _side_aggs(df_expected, relevant)
    target_count = t_row["cnt"] or 0
    expected_count = e_row["cnt"] or 0

    results = {}
    for exp in relevant:
        c, m = exp.column, exp.metric
        key = f"{c}:{m}" if c else m

        if m == "mean_diff":
            t_mean, t_std = t_row[f"mean__{c}"] or 0, t_row[f"std__{c}"] or 0
            e_mean, e_std = e_row[f"mean__{c}"] or 0, e_row[f"std__{c}"] or 0
            diff = abs(t_mean - e_mean) / e_mean if e_mean != 0 else 0
            results[key] = {
                "target_mean": t_mean, "target_std": t_std, "target_n": target_count,
                "expected_mean": e_mean, "expected_std": e_std, "expected_n": expected_count,
                "diff": diff,
            }

        elif m == "null_rate_diff":
            t_nulls, e_nulls = t_row[f"nr__{c}"] or 0, e_row[f"nr__{c}"] or 0
            t_rate = t_nulls / target_count if target_count > 0 else 0
            e_rate = e_nulls / expected_count if expected_count > 0 else 0
            results[key] = {
                "target_rate": t_rate, "expected_rate": e_rate,
                "diff": abs(t_rate - e_rate),
                "target_n": target_count, "expected_n": expected_count,
            }

        elif m == "row_count_ratio":
            ratio = target_count / expected_count if expected_count > 0 else 0
            results[key] = {"ratio": ratio, "target_count": target_count, "expected_count": expected_count}

        elif m == "distribution_shift":
            t_vals = [r[0] for r in df_target.select(c).collect()]
            e_vals = [r[0] for r in df_expected.select(c).collect()]
            from scipy.stats import ks_2samp
            stat, p_value = ks_2samp(t_vals, e_vals)
            results[key] = {
                "ks_statistic": stat, "p_value": p_value,
                "target_n": target_count, "expected_n": expected_count,
            }

        elif m == "schema_match":
            t_schema = {f.name: f.dataType.simpleString() for f in df_target.schema.fields}
            e_schema = {f.name: f.dataType.simpleString() for f in df_expected.schema.fields}
            results[key] = {"match": t_schema == e_schema, "target_schema": t_schema, "expected_schema": e_schema}

    return results
```

Then in `src/delphi/runner.py`, cache the comparison sample. In the comparison block, change the line `comp_df = sample_dataframe(spark, comp_table, comp_plan)` to:

```python
                comp_df = sample_dataframe(spark, comp_table, comp_plan)
                comp_df.cache()
                cached_dfs.append(comp_df)
```

Add `cached_dfs = []` next to `sampled_df = None` at the top of the function, append `sampled_df` to it right after `sampled_df.cache()` (from Task 3), and change the `finally` to unpersist every entry:

```python
    finally:
        for _df in cached_dfs:
            try:
                _df.unpersist()
            except Exception:
                pass
```

(Replace the single-`sampled_df` unpersist from Task 3 with this loop; drop the standalone `if sampled_df is not None` guard since the list is empty when nothing was cached.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_comparison.py tests/unit/test_runner.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delphi/engine/metrics.py src/delphi/runner.py tests/unit/test_comparison.py
git commit -m "perf: fuse comparison-metric aggregation and cache comparison samples"
```

---

## Task 5: Full unit suite + performance validation harness

**Files:**
- Modify: `tests/integration_delphi_dsl.py` (add a timing comparison cell)

- [ ] **Step 1: Run the full unit suite**

Run: `uv run pytest tests/unit/ -v`
Expected: PASS — all prior tests plus the new ones. If any test elsewhere asserted the old `orderBy`/per-metric behavior, update it to the fused-agg shape (do not delete assertions; translate them).

- [ ] **Step 2: Add a performance-timing cell to the integration notebook**

Append to `tests/integration_delphi_dsl.py` a cell that runs a multi-expectation batch against `delphi.default.prices` (3.2M rows) and prints wall-clock time. Match the file's existing cell/decorator style; the body should resemble:

```python
# --- Performance smoke: multi-metric batch should be a single-digit-seconds run ---
import time
from delphi import col, functions as F, datatest

@datatest(table="delphi.default.prices")
def perf_batch(dt):
    dt.expect(col("close").null_rate < 0.01)
    dt.expect(col("close").mean.between(0, 1_000_000))
    dt.expect(col("symbol").uniqueness > 0.0)
    dt.expect(col("volume").min > -1)
    dt.expect(F.row_count() > 1000)

_start = time.monotonic()
perf_batch.run()
print(f"perf_batch wall time: {time.monotonic() - _start:.2f}s")
```

Adjust column names (`close`, `symbol`, `volume`) to the actual schema of `delphi.default.prices` if they differ — inspect via `DESCRIBE delphi.default.prices` first. The assertion of success is qualitative: a multi-expectation batch that previously issued N sorts now completes in a single-digit-seconds range because it fires one fused aggregation over one cached sample.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_delphi_dsl.py
git commit -m "test: add multi-metric performance smoke to integration notebook"
```

---

## Self-Review Notes

- **Spec coverage:** fraction sampling (Task 1), fused regular aggregation w/ HLL uniqueness (Task 2), cache + unpersist + evidence reuse (Task 3), fused comparison aggregation + comparison-sample caching (Task 4), unit-green + free-tier perf validation (Task 5). Reconciliation and prescan explicitly out of scope per spec — no tasks, intentionally.
- **Alias consistency:** `nr__`, `uq__`, `mean__`, `std__`, `min__`, `max__`, `sdev__`, `pct__`, `cnt` are used identically in the build and demux loops of Task 2, and `mean__`/`std__`/`nr__`/`cnt` identically across both sides in Task 4.
- **API preserved:** every result dict keeps its existing keys (`null_count`/`total`, `distinct_count`/`total`, `mean`/`std`/`total`, `value`/`total`, `count`, and all comparison keys), so `runner._compute_confidence` needs no changes.
