# Databricks notebook source
# MAGIC %md
# MAGIC # Delphi v0.5.0 Integration Tests
# MAGIC Tests the full Delphi library against live Delta tables

# COMMAND ----------

# MAGIC %pip install /Volumes/delphi/default/libs/dbx_delphi-0.5.0-py3-none-any.whl --force-reinstall --quiet
dbutils.library.restartPython()

# COMMAND ----------

import delphi
print(f"Delphi version: {delphi.__version__}")
assert delphi.__version__ == "0.5.0"

from delphi import col, datatest, compare
from delphi import functions as F
from delphi.config import DelphiConfig
from delphi.runner import run_expectations
from delphi.assertions.expectation import Expectation
from delphi.engine.prescan import prescan_table
from delphi.engine.sampler import compute_sample_size
from delphi.detect.time_column import detect_time_column
from delphi.renderers.terminal import render_terminal
from delphi.renderers.ci import render_json
from delphi.renderers.notebook import render_notebook
from delphi.history import save_run, load_baseline, detect_drift

print("All imports OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Prescan

result = prescan_table(spark, "delphi.default.prices")
print(f"Rows: {result.row_count:,}, Columns: {list(result.columns.keys())}")
assert result.row_count > 0 and "close" in result.columns
print("PASS: prescan")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Time column detection

time_col = detect_time_column(result)
assert time_col == "date"
print(f"PASS: time column = {time_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Full DSL pipeline

@datatest("delphi.default.prices")
def test_prices(dt):
    dt.expect(col("close").null_rate < 0.01)
    dt.expect(col("close").mean.between(0, 100000), confidence=0.99)
    dt.expect(F.row_count() > 1_000_000)

ds = test_prices()
config = DelphiConfig(sample_ceiling=10000, enable_history=False)
results = run_expectations(spark, ds.table, ds.expectations, config, test_name="dsl_test")
for r in results:
    cr = r.confidence_result
    print(f"  {r.test_name}: {r.status.upper()} observed={cr.observed:.6f} method={cr.method}")
    assert r.status == "pass", f"{r.test_name} failed"
print("PASS: full DSL pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Terminal renderer with total time

result_dicts = [{
    "test_name": r.test_name, "table": r.table, "status": r.status,
    "confidence_result": r.confidence_result, "threshold": r.threshold,
    "duration_ms": r.duration_ms, "evidence": r.evidence,
    "error": r.error, "suggestion": r.suggestion, "drift": r.drift,
} for r in results]

output = render_terminal(result_dicts, total_ms=4200)
assert "4.2s" in output
print("PASS: terminal renderer with total time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: JSON renderer with total time

import json
parsed = json.loads(render_json(result_dicts, total_ms=3500))
assert parsed["total_duration_ms"] == 3500
assert len(parsed["results"]) == 3
print("PASS: JSON renderer with total time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Notebook renderer

html = render_notebook(result_dicts, total_ms=2500)
assert "<html>" in html and "plotly" in html.lower()
displayHTML(html)
print("PASS: notebook renderer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: View support

view_prescan = prescan_table(spark, "delphi.default.v_prices")
assert view_prescan.is_view is True

@datatest("delphi.default.v_prices")
def test_view(dt):
    dt.expect(col("close").null_rate < 0.01)

ds_v = test_view()
vr = run_expectations(spark, ds_v.table, ds_v.expectations, DelphiConfig(sample_ceiling=10000, enable_history=False), test_name="view")
assert vr[0].status == "pass"
print("PASS: view support")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Run history + drift detection

import tempfile, os
with tempfile.TemporaryDirectory() as tmpdir:
    hp = os.path.join(tmpdir, "history.jsonl")
    c1 = DelphiConfig(sample_ceiling=5000, enable_history=True, history_path=hp)
    r1 = run_expectations(spark, "delphi.default.prices",
        [Expectation(column="close", metric="null_rate", threshold=0.01, direction="below")],
        c1, test_name="hist_test")
    assert r1[0].drift is None
    c2 = DelphiConfig(sample_ceiling=5000, enable_history=True, history_path=hp)
    r2 = run_expectations(spark, "delphi.default.prices",
        [Expectation(column="close", metric="null_rate", threshold=0.01, direction="below")],
        c2, test_name="hist_test")
    assert r2[0].baseline_observed is not None
    print(f"  Run 1: drift=None, Run 2: baseline={r2[0].baseline_observed:.6f}")
print("PASS: run history + drift detection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: Comparison metrics (mean_diff)

comp_exp = [Expectation(column="close", metric="mean_diff", threshold=0.10, direction="below",
            compare_table="delphi.default.prices")]
cr = run_expectations(spark, "delphi.default.prices", comp_exp,
    DelphiConfig(sample_ceiling=5000, enable_history=False), test_name="comp")
assert cr[0].confidence_result.method == "welch_t"
print(f"  mean_diff observed={cr[0].confidence_result.observed:.6f}")
print("PASS: comparison metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10: Reconciliation — coverage
# MAGIC
# MAGIC Use a small sample of prices as the "expected" dataset, then check coverage against the full table.

# Create a small expected subset (first 100 rows by ticker+date)
expected_df = spark.table("delphi.default.prices").limit(100)
expected_df.createOrReplaceTempView("expected_subset")

from delphi.engine.reconciliation import compute_reconciliation_metrics

cov_exp = [Expectation(column=None, metric="coverage", key_columns=["ticker", "date"],
           compare_table="expected_subset")]
cov_metrics = compute_reconciliation_metrics(
    spark.table("delphi.default.prices"), expected_df, cov_exp)

cov_key = "coverage"
print(f"  Coverage: {cov_metrics[cov_key]['found']}/{cov_metrics[cov_key]['total']} = {cov_metrics[cov_key]['rate']:.4f}")
assert cov_metrics[cov_key]["rate"] >= 0.99, f"Coverage too low: {cov_metrics[cov_key]['rate']}"
print("PASS: reconciliation coverage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 11: Reconciliation — match_rate (exact)
# MAGIC
# MAGIC Same subset — all values should match exactly since it's from the same table.

mr_exp = [Expectation(column="close", metric="match_rate", key_columns=["ticker", "date"],
          tolerance=0.0)]
mr_metrics = compute_reconciliation_metrics(
    spark.table("delphi.default.prices"), expected_df, mr_exp)

mr_key = "close:match_rate"
print(f"  Exact match rate: {mr_metrics[mr_key]['matches']}/{mr_metrics[mr_key]['total']} = {mr_metrics[mr_key]['rate']:.4f}")
assert mr_metrics[mr_key]["rate"] >= 0.99
print("PASS: reconciliation exact match_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 12: Reconciliation — match_rate with tolerance

mr_tol_exp = [Expectation(column="close", metric="match_rate", key_columns=["ticker", "date"],
              tolerance=0.01)]
mr_tol_metrics = compute_reconciliation_metrics(
    spark.table("delphi.default.prices"), expected_df, mr_tol_exp)

print(f"  Tolerance match rate: {mr_tol_metrics[mr_key]['rate']:.4f}")
assert mr_tol_metrics[mr_key]["rate"] >= 0.99
print("PASS: reconciliation match_rate with tolerance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 13: Reconciliation — mean_deviation

md_exp = [Expectation(column="close", metric="mean_deviation", key_columns=["ticker", "date"])]
md_metrics = compute_reconciliation_metrics(
    spark.table("delphi.default.prices"), expected_df, md_exp)

md_key = "close:mean_deviation"
print(f"  Mean deviation: {md_metrics[md_key]['mean_dev']:.6f}")
assert md_metrics[md_key]["mean_dev"] < 0.01  # Same data, should be ~0
print("PASS: reconciliation mean_deviation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 14: Reconciliation — full DSL pipeline

@datatest("delphi.default.prices")
def test_recon(dt):
    expected = compare("expected_subset")
    keys = ["ticker", "date"]
    dt.expect(col(*keys).coverage(expected) > 0.99)
    dt.expect(col("close").match_rate(expected, key=keys) > 0.99)
    dt.expect(col("close").mean_deviation(expected, key=keys) < 0.01)

ds_recon = test_recon()
recon_config = DelphiConfig(sample_ceiling=5000, enable_history=False)
recon_results = run_expectations(spark, ds_recon.table, ds_recon.expectations, recon_config, test_name="recon_dsl")

for r in recon_results:
    cr = r.confidence_result
    print(f"  {r.test_name}: {r.status.upper()} observed={cr.observed:.4f} method={cr.method}")
    assert r.status == "pass", f"{r.test_name} failed: {r.error or cr}"
print("PASS: reconciliation full DSL pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All 14 integration tests passed:
# MAGIC 1. Prescan
# MAGIC 2. Time column detection
# MAGIC 3. Full DSL pipeline
# MAGIC 4. Terminal renderer with total time
# MAGIC 5. JSON renderer with total time
# MAGIC 6. Notebook renderer (plotly)
# MAGIC 7. View support
# MAGIC 8. Run history + drift detection
# MAGIC 9. Comparison metrics (mean_diff)
# MAGIC 10. Reconciliation — coverage
# MAGIC 11. Reconciliation — exact match_rate
# MAGIC 12. Reconciliation — match_rate with tolerance
# MAGIC 13. Reconciliation — mean_deviation
# MAGIC 14. Reconciliation — full DSL pipeline

print("ALL 14 INTEGRATION TESTS PASSED - v0.5.0")
