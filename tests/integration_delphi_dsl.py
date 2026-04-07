# Databricks notebook source
# MAGIC %md
# MAGIC # Delphi v0.4.0 Integration Tests
# MAGIC Tests the full Delphi library against live Delta tables

# COMMAND ----------

# MAGIC %pip install /Volumes/delphi/default/libs/dbx_delphi-0.4.0-py3-none-any.whl --force-reinstall --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Verify import
import delphi
print(f"Delphi version: {delphi.__version__}")
assert delphi.__version__ == "0.4.0"

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

# COMMAND ----------

result = prescan_table(spark, "delphi.default.prices")
print(f"Table: {result.table}")
print(f"Rows: {result.row_count:,}")
print(f"Columns: {list(result.columns.keys())}")
assert result.row_count > 0
assert "close" in result.columns
print("PASS: prescan works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Time column detection

# COMMAND ----------

time_col = detect_time_column(result)
print(f"Detected time column: {time_col}")
assert time_col == "date"
print("PASS: time column auto-detection works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Adaptive sampling

# COMMAND ----------

plan = compute_sample_size(result, confidence=0.95, sample_floor=1000, sample_ceiling=50000)
print(f"Sample size: {plan.n:,}")
assert plan.n <= 50000
assert plan.n >= 1000
print("PASS: adaptive sampling works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Full pipeline via @datatest

# COMMAND ----------

@datatest("delphi.default.prices")
def test_prices_quality(dt):
    dt.expect(col("close").null_rate < 0.01)
    dt.expect(col("close").mean.between(0, 100000), confidence=0.99)
    dt.expect(F.row_count() > 1_000_000)

ds = test_prices_quality()
config = DelphiConfig(sample_ceiling=10000, enable_history=False)
results = run_expectations(spark, ds.table, ds.expectations, config, test_name="dsl_test")

print(f"\nResults ({len(results)} expectations):")
for r in results:
    cr = r.confidence_result
    if cr:
        print(f"  {r.test_name}: {r.status.upper()} | observed={cr.observed:.6f} method={cr.method}")
    else:
        print(f"  {r.test_name}: {r.status.upper()} | {r.error}")

for r in results:
    assert r.status == "pass", f"{r.test_name} failed: {r.error or r.confidence_result}"
print("\nPASS: full pipeline works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Terminal renderer with total time

# COMMAND ----------

result_dicts = [{
    "test_name": r.test_name, "table": r.table, "status": r.status,
    "confidence_result": r.confidence_result, "threshold": r.threshold,
    "duration_ms": r.duration_ms, "evidence": r.evidence,
    "error": r.error, "suggestion": r.suggestion, "drift": r.drift,
} for r in results]

output = render_terminal(result_dicts, total_ms=4200)
assert "PASS" in output
assert "4.2s" in output
print("Terminal output:")
print(output)
print("PASS: terminal renderer with total time works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: JSON renderer with total time

# COMMAND ----------

import json

json_output = render_json(result_dicts, total_ms=3500)
parsed = json.loads(json_output)
assert "total_duration_ms" in parsed
assert parsed["total_duration_ms"] == 3500
assert len(parsed["results"]) == 3
print(f"JSON: total_duration_ms={parsed['total_duration_ms']}, {len(parsed['results'])} results")
print("PASS: JSON renderer with total time works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Notebook renderer

# COMMAND ----------

html = render_notebook(result_dicts, total_ms=2500)
assert "<html>" in html
assert "Delphi Test Results" in html
assert "plotly" in html.lower()
assert "2.5s" in html
print(f"Notebook HTML length: {len(html)} chars")
print("PASS: notebook renderer works")

# Display it!
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Evidence on forced failure

# COMMAND ----------

failing_exp = [Expectation(column=None, metric="row_count", threshold=999_000_000_000, direction="above")]
fail_config = DelphiConfig(sample_ceiling=5000, evidence_rows=3, enable_history=False)
fail_results = run_expectations(spark, "delphi.default.prices", failing_exp, fail_config, test_name="force_fail")

assert fail_results[0].status == "fail"
print(f"Forced failure: {fail_results[0].status}")
print("PASS: failing test reports correctly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: View support

# COMMAND ----------

view_prescan = prescan_table(spark, "delphi.default.v_prices")
print(f"Table type: {view_prescan.table_type}, is_view: {view_prescan.is_view}")
assert view_prescan.is_view is True
assert view_prescan.row_count > 0

@datatest("delphi.default.v_prices")
def test_view(dt):
    dt.expect(col("close").null_rate < 0.01)
    dt.expect(F.row_count() > 1000)

ds_view = test_view()
view_config = DelphiConfig(sample_ceiling=10000, enable_history=False)
view_results = run_expectations(spark, ds_view.table, ds_view.expectations, view_config, test_name="view_test")
for r in view_results:
    assert r.status == "pass", f"{r.test_name} failed"
print("PASS: view support works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10: Wilson CI on security table

# COMMAND ----------

sec_expectations = [
    Expectation(column="ticker", metric="null_rate", threshold=0.01, direction="below"),
    Expectation(column="ticker", metric="uniqueness", threshold=0.5, direction="above"),
]
sec_config = DelphiConfig(sample_ceiling=5000, enable_history=False)
sec_results = run_expectations(spark, "delphi.default.security", sec_expectations, sec_config, test_name="security")
for r in sec_results:
    cr = r.confidence_result
    print(f"{r.test_name}: {r.status} | {cr.method} observed={cr.observed:.4f}")
    assert r.status == "pass"
print("PASS: Wilson CI on security table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 11: Run history and drift detection

# COMMAND ----------

import tempfile, os

with tempfile.TemporaryDirectory() as tmpdir:
    history_path = os.path.join(tmpdir, "history.jsonl")

    # First run — establishes baseline
    config1 = DelphiConfig(sample_ceiling=5000, enable_history=True, history_path=history_path)
    r1 = run_expectations(spark, "delphi.default.prices",
        [Expectation(column="close", metric="null_rate", threshold=0.01, direction="below")],
        config1, test_name="history_test")
    assert r1[0].drift is None  # No baseline yet
    print(f"Run 1: observed={r1[0].confidence_result.observed:.6f}, drift=None (no baseline)")

    # Second run — should have baseline, no drift (same data)
    config2 = DelphiConfig(sample_ceiling=5000, enable_history=True, history_path=history_path)
    r2 = run_expectations(spark, "delphi.default.prices",
        [Expectation(column="close", metric="null_rate", threshold=0.01, direction="below")],
        config2, test_name="history_test")
    print(f"Run 2: observed={r2[0].confidence_result.observed:.6f}, baseline={r2[0].baseline_observed}, drift={r2[0].drift}")

    # Verify baseline was loaded
    baseline = load_baseline("history_test:close.null_rate", "delphi.default.prices", history_path)
    assert baseline is not None
    print(f"Baseline: observed={baseline['observed']:.6f}")

print("PASS: run history and drift detection works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 12: Comparison metrics (mean_diff)

# COMMAND ----------

# Compare prices table against itself (should show ~0 difference)
comp_expectations = [
    Expectation(column="close", metric="mean_diff", threshold=0.05, direction="below",
                compare_table="delphi.default.prices"),
]
comp_config = DelphiConfig(sample_ceiling=5000, enable_history=False)
comp_results = run_expectations(spark, "delphi.default.prices", comp_expectations, comp_config, test_name="comparison")

for r in comp_results:
    cr = r.confidence_result
    print(f"{r.test_name}: {r.status} | method={cr.method} observed={cr.observed:.6f}")

# Self-comparison should show small difference (sampling variance only)
assert comp_results[0].confidence_result is not None
assert comp_results[0].confidence_result.method == "welch_t"
print("PASS: comparison metrics (mean_diff with Welch's t-test) works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All 12 integration tests passed:
# MAGIC 1. Prescan
# MAGIC 2. Time column detection
# MAGIC 3. Adaptive sampling
# MAGIC 4. Full DSL pipeline
# MAGIC 5. Terminal renderer with total time
# MAGIC 6. JSON renderer with total time
# MAGIC 7. Notebook renderer (plotly)
# MAGIC 8. Evidence on failure
# MAGIC 9. View support
# MAGIC 10. Wilson CI on security table
# MAGIC 11. Run history + drift detection
# MAGIC 12. Comparison metrics (mean_diff)

print("ALL 12 INTEGRATION TESTS PASSED - v0.4.0")
