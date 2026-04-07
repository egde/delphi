# Databricks notebook source
# MAGIC %md
# MAGIC # Delphi Integration Tests
# MAGIC Runs against `delphi.default.prices` and `delphi.default.security`

# COMMAND ----------

# Smoke tests - table access
prices = spark.table("delphi.default.prices")
security = spark.table("delphi.default.security")

prices_count = prices.count()
security_count = security.count()
print(f"prices: {prices_count:,} rows")
print(f"security: {security_count:,} rows")
assert prices_count > 0, "prices table is empty"
assert security_count > 0, "security table is empty"
print("PASS: tables readable")

# COMMAND ----------

# Schema checks
prices_cols = [f.name for f in prices.schema.fields]
security_cols = [f.name for f in security.schema.fields]

assert "ticker" in prices_cols, f"ticker not in prices: {prices_cols}"
assert "date" in prices_cols, f"date not in prices: {prices_cols}"
assert "close" in prices_cols, f"close not in prices: {prices_cols}"
assert "ticker" in security_cols, f"ticker not in security: {security_cols}"
assert "name" in security_cols, f"name not in security: {security_cols}"
assert "sector" in security_cols, f"sector not in security: {security_cols}"
print(f"PASS: prices columns: {prices_cols}")
print(f"PASS: security columns: {security_cols}")

# COMMAND ----------

# Prescan equivalent - DESCRIBE DETAIL
detail = spark.sql("DESCRIBE DETAIL delphi.default.prices").collect()[0]
print(f"numFiles: {detail.numFiles}")
print(f"sizeInBytes: {detail.sizeInBytes}")
print(f"partitionColumns: {detail.partitionColumns}")
print("PASS: DESCRIBE DETAIL works")

# COMMAND ----------

# Null rate check on prices.close
from pyspark.sql import functions as F

total = prices.count()
null_count = prices.filter(F.col("close").isNull()).count()
null_rate = null_count / total
print(f"close null_rate: {null_rate:.6f} ({null_count}/{total})")
assert null_rate < 0.01, f"null_rate {null_rate} exceeds threshold 0.01"
print("PASS: close null_rate < 0.01")

# COMMAND ----------

# Sampling test - verify rand() ordering works
sample_size = 10000
sampled = prices.orderBy(F.rand(seed=42)).limit(sample_size)
sampled_count = sampled.count()
print(f"Sampled {sampled_count} rows from {total}")
assert sampled_count == sample_size, f"Expected {sample_size}, got {sampled_count}"
print("PASS: sampling works")

# COMMAND ----------

# Metric computation on sample
stats = sampled.agg(
    F.avg("close").alias("mean_close"),
    F.stddev("close").alias("std_close"),
    F.min("close").alias("min_close"),
    F.max("close").alias("max_close"),
).collect()[0]

print(f"mean(close):   {stats.mean_close:.4f}")
print(f"stddev(close): {stats.std_close:.4f}")
print(f"min(close):    {stats.min_close:.4f}")
print(f"max(close):    {stats.max_close:.4f}")
assert stats.mean_close > 0, "mean should be positive"
print("PASS: metric computation works")

# COMMAND ----------

# Uniqueness check on security.ticker
distinct_tickers = security.select("ticker").distinct().count()
total_securities = security.count()
uniqueness = distinct_tickers / total_securities
print(f"ticker uniqueness: {uniqueness:.4f} ({distinct_tickers}/{total_securities})")
assert uniqueness > 0.99, f"uniqueness {uniqueness} below 0.99"
print("PASS: ticker uniqueness > 0.99")

# COMMAND ----------

# Wilson CI computation (pure Python, no Spark needed)
from scipy import stats as scipy_stats

p_hat = null_rate
n = total
z = scipy_stats.norm.ppf(0.975)
z2 = z * z
denom = 1 + z2 / n
center = (p_hat + z2 / (2 * n)) / denom
margin = (z / denom) * ((p_hat * (1 - p_hat) / n) + (z2 / (4 * n * n))) ** 0.5
ci_lower = max(0, center - margin)
ci_upper = min(1, center + margin)

print(f"Wilson CI for null_rate: [{ci_lower:.6f}, {ci_upper:.6f}]")
print(f"Threshold: < 0.01")
print(f"CI upper ({ci_upper:.6f}) < 0.01: {ci_upper < 0.01}")
print("PASS: confidence interval computation works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC All integration tests passed. The Delphi engine pipeline works:
# MAGIC - Table access and schema validation
# MAGIC - DESCRIBE DETAIL (prescan)
# MAGIC - Sampling via rand()
# MAGIC - Metric computation (null_rate, mean, stddev, min, max, uniqueness)
# MAGIC - Wilson confidence interval
