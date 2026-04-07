# Databricks notebook source
# MAGIC %md
# MAGIC # Delphi DSL Integration Tests
# MAGIC Tests the full Delphi library against live Delta tables

# COMMAND ----------

# MAGIC %pip install /Volumes/delphi/default/libs/dbx_delphi-0.2.0-py3-none-any.whl --force-reinstall --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Verify import
import delphi
print(f"Delphi version: {delphi.__version__}")

from delphi import col, datatest, compare
from delphi import functions as F
from delphi.config import DelphiConfig
from delphi.runner import run_expectations
from delphi.assertions.expectation import Expectation
from delphi.engine.prescan import prescan_table
from delphi.engine.sampler import compute_sample_size
from delphi.detect.time_column import detect_time_column
from delphi.confidence.proportions import wilson_confidence_interval
from delphi.confidence.means import t_confidence_interval
from delphi.renderers.terminal import render_terminal
from delphi.renderers.ci import render_json

print("All imports OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Prescan

# COMMAND ----------

result = prescan_table(spark, "delphi.default.prices")
print(f"Table: {result.table}")
print(f"Rows: {result.row_count:,}")
print(f"Files: {result.num_files}")
print(f"Columns: {list(result.columns.keys())}")
print(f"Partitions: {result.partition_columns}")

assert result.row_count > 0
assert "close" in result.columns
assert "ticker" in result.columns
assert "date" in result.columns
print("PASS: prescan works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Time column detection

# COMMAND ----------

time_col = detect_time_column(result)
print(f"Detected time column: {time_col}")
# 'date' should be detected (it's a date-type column with a well-known name)
assert time_col == "date", f"Expected 'date', got '{time_col}'"
print("PASS: time column auto-detection works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Adaptive sampling

# COMMAND ----------

plan = compute_sample_size(result, confidence=0.95, sample_floor=1000, sample_ceiling=50000)
print(f"Sample size: {plan.n:,}")
print(f"Fraction: {plan.fraction:.6f}")
print(f"Full table: {plan.use_full_table}")

assert plan.n <= 50000
assert plan.n >= 1000
assert plan.use_full_table == False
print("PASS: adaptive sampling computes correct size")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: DSL expression tree

# COMMAND ----------

# Build expectations using the PySpark-native DSL
exp1 = col("close").null_rate < 0.01
exp2 = col("close").mean.between(0, 10000)
exp3 = col("ticker").uniqueness > 0.0001
exp4 = F.row_count() > 1_000_000

print(f"exp1: {exp1.column}.{exp1.metric} {exp1.direction} {exp1.threshold}")
print(f"exp2: {exp2.column}.{exp2.metric} {exp2.direction} [{exp2.threshold_low}, {exp2.threshold_high}]")
print(f"exp3: {exp3.column}.{exp3.metric} {exp3.direction} {exp3.threshold}")
print(f"exp4: {exp4.metric} {exp4.direction} {exp4.threshold}")

assert exp1.column == "close" and exp1.metric == "null_rate" and exp1.direction == "below"
assert exp2.direction == "between"
assert exp3.metric == "uniqueness"
assert exp4.column is None and exp4.metric == "row_count"
print("PASS: DSL expression tree builds correctly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: @datatest decorator

# COMMAND ----------

@datatest("delphi.default.prices")
def test_prices_quality(dt):
    dt.expect(col("close").null_rate < 0.01)
    dt.expect(col("close").mean.between(0, 10000), confidence=0.99)
    dt.expect(F.row_count() > 1_000_000)

ds = test_prices_quality()
print(f"Table: {ds.table}")
print(f"Expectations: {len(ds.expectations)}")
for exp in ds.expectations:
    c = exp.column or "(dataset)"
    print(f"  {c}.{exp.metric} {exp.direction} conf={exp.confidence}")

assert ds.table == "delphi.default.prices"
assert len(ds.expectations) == 3
assert ds.expectations[1].confidence == 0.99
print("PASS: @datatest decorator collects expectations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Full pipeline via run_expectations

# COMMAND ----------

config = DelphiConfig(sample_ceiling=10000, evidence_rows=5)
results = run_expectations(
    spark, "delphi.default.prices", ds.expectations, config, test_name="dsl_test"
)

print(f"\nResults ({len(results)} expectations):")
for r in results:
    cr = r.confidence_result
    if cr:
        print(f"  {r.test_name}: {r.status.upper()} | observed={cr.observed:.6f} CI=[{cr.ci_lower:.6f}, {cr.ci_upper:.6f}] method={cr.method}")
    else:
        print(f"  {r.test_name}: {r.status.upper()} | {r.error}")

# All should pass
for r in results:
    assert r.status == "pass", f"{r.test_name} failed: {r.error or r.confidence_result}"
print("\nPASS: full pipeline runs all expectations successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Terminal renderer output

# COMMAND ----------

result_dicts = [{
    "test_name": r.test_name, "table": r.table, "status": r.status,
    "confidence_result": r.confidence_result, "threshold": r.threshold,
    "duration_ms": r.duration_ms, "evidence": r.evidence,
    "error": r.error, "suggestion": r.suggestion,
} for r in results]

output = render_terminal(result_dicts)
assert "PASS" in output
print("Terminal output:")
print(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: JSON renderer output

# COMMAND ----------

import json

json_output = render_json(result_dicts)
parsed = json.loads(json_output)
assert len(parsed) == 3
assert all(r["status"] == "pass" for r in parsed)
print(f"JSON output ({len(parsed)} results):")
for r in parsed:
    print(f"  {r['test']}: {r['status']} observed={r['observed']}")
print("\nPASS: JSON renderer works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: Evidence collection on forced failure

# COMMAND ----------

# Create an expectation that will fail — row_count > 999 billion
failing_exp = [Expectation(column=None, metric="row_count", threshold=999_000_000_000, direction="above")]
fail_config = DelphiConfig(sample_ceiling=5000, evidence_rows=3)
fail_results = run_expectations(spark, "delphi.default.prices", failing_exp, fail_config, test_name="force_fail")

assert fail_results[0].status == "fail"
print(f"Forced failure: {fail_results[0].status}")
print(f"Observed: {fail_results[0].confidence_result.observed:,.0f}")
print(f"Threshold: > 999,000,000,000")
print("PASS: failing test correctly reports fail status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10: Wilson CI on security table

# COMMAND ----------

sec_expectations = [
    Expectation(column="ticker", metric="null_rate", threshold=0.01, direction="below"),
    Expectation(column="ticker", metric="uniqueness", threshold=0.5, direction="above"),
]
sec_config = DelphiConfig(sample_ceiling=5000)
sec_results = run_expectations(spark, "delphi.default.security", sec_expectations, sec_config, test_name="security")

for r in sec_results:
    cr = r.confidence_result
    print(f"{r.test_name}: {r.status} | {cr.method} observed={cr.observed:.4f} CI=[{cr.ci_lower:.4f}, {cr.ci_upper:.4f}]")
    assert r.status == "pass", f"{r.test_name} failed unexpectedly"

print("\nPASS: Wilson CI works on security table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 11: View support — prescan detects view

# COMMAND ----------

view_prescan = prescan_table(spark, "delphi.default.v_prices")
print(f"Table type: {view_prescan.table_type}")
print(f"Is view: {view_prescan.is_view}")
print(f"Row count: {view_prescan.row_count:,}")
print(f"Columns: {list(view_prescan.columns.keys())}")
print(f"Num files: {view_prescan.num_files}")
print(f"Partition columns: {view_prescan.partition_columns}")

assert view_prescan.is_view is True, f"Expected view, got table_type={view_prescan.table_type}"
assert view_prescan.row_count > 0
assert view_prescan.num_files == 0
assert view_prescan.partition_columns == []
assert len(view_prescan.columns) > 0
print("PASS: prescan correctly detects view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 12: View support — full DSL pipeline on view

# COMMAND ----------

@datatest("delphi.default.v_prices")
def test_view_quality(dt):
    dt.expect(col("close").null_rate < 0.01)
    dt.expect(col("close").mean.between(0, 100000), confidence=0.99)
    dt.expect(F.row_count() > 1000)

ds_view = test_view_quality()
view_config = DelphiConfig(sample_ceiling=10000)
view_results = run_expectations(spark, ds_view.table, ds_view.expectations, view_config, test_name="view_dsl")

print(f"\nView results ({len(view_results)} expectations):")
for r in view_results:
    cr = r.confidence_result
    if cr:
        print(f"  {r.test_name}: {r.status.upper()} | observed={cr.observed:.6f} method={cr.method}")
    else:
        print(f"  {r.test_name}: {r.status.upper()} | {r.error}")

for r in view_results:
    assert r.status == "pass", f"{r.test_name} failed: {r.error or r.confidence_result}"
print("\nPASS: full DSL pipeline works on view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All 12 integration tests passed:
# MAGIC 1. Prescan metadata extraction
# MAGIC 2. Time column auto-detection
# MAGIC 3. Adaptive sample size computation
# MAGIC 4. PySpark-native DSL expression tree
# MAGIC 5. @datatest decorator
# MAGIC 6. Full pipeline (prescan -> sample -> metrics -> confidence -> result)
# MAGIC 7. Terminal renderer
# MAGIC 8. JSON renderer
# MAGIC 9. Evidence collection on failure
# MAGIC 10. Wilson CI on security table
# MAGIC 11. View support — prescan detects view
# MAGIC 12. View support — full DSL pipeline on view

print("ALL 12 INTEGRATION TESTS PASSED")
