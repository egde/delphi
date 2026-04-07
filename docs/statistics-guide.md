# Statistics Guide for Delphi

This guide explains the statistical concepts behind Delphi in plain language. You don't need a statistics background to use Delphi, but understanding these ideas will help you tune your tests and interpret results.

## Sampling

### What is sampling?

Your table has billions of rows. Reading all of them to check if the null rate is below 1% would take minutes or hours. Instead, Delphi reads a small, random subset -- a **sample** -- and uses it to estimate what the full table looks like.

Think of it like taste-testing soup. You don't need to drink the whole pot to know if it needs more salt. A single spoonful, well-stirred, tells you enough.

### How Delphi samples

Delphi uses `ORDER BY rand() LIMIT N` -- it shuffles the table randomly and takes the first N rows. This gives every row an equal chance of being selected, which is critical for the sample to be representative.

For timeseries tables, Delphi does something smarter: it detects the time column and samples across different time periods (stratified sampling), so you don't accidentally get all your rows from one week.

### Sample size

The number of rows Delphi reads. Bigger samples give more precise results but take longer. Delphi computes the sample size automatically based on the confidence level you request -- this is called **adaptive sampling**.

The formula balances three things:
- **Confidence level** -- how sure you want to be (95% vs 99%)
- **Margin of error** -- how precise the estimate needs to be
- **Expected variability** -- how much the data varies

Higher confidence or tighter precision = more rows needed.

### Sample floor and ceiling

These are guard rails on the sample size:

- **Sample floor** (default: 1,000) -- Delphi never samples fewer than this many rows, even if the math says a smaller sample would suffice. Very small samples are unreliable regardless of what the formula says.

- **Sample ceiling** (default: 100,000) -- Delphi never samples more than this many rows. This caps your query cost. Even if 99.9% confidence would require 500,000 rows, Delphi stops at the ceiling.

If the table has fewer rows than the floor, Delphi reads the entire table.

Configure these in `delphi.toml`:

```toml
[delphi]
sample_floor = 1000
sample_ceiling = 100000
```

Or per-run:

```bash
delphi run tests/ --sample-ceiling 200000
```

## Confidence and Uncertainty

### What is a confidence interval?

When you measure the null rate on a sample, you get a number like 0.3%. But the true null rate across all billions of rows might be slightly different. A **confidence interval** (CI) is a range that likely contains the true value.

Example: observed null rate is 0.3%, and the 95% CI is [0.2%, 0.4%]. This means: if we repeated this sampling process many times, 95% of the intervals we compute would contain the true null rate.

In practice, think of it as: "We're 95% sure the real null rate is somewhere between 0.2% and 0.4%."

### What is confidence level?

The **confidence level** is how sure you want to be that the interval contains the truth. Delphi defaults to 95%.

| Confidence level | What it means | Trade-off |
|-----------------|---------------|-----------|
| 90% | "Pretty sure" | Smaller sample, wider tolerance for error |
| 95% | "Quite sure" (default) | Good balance of speed and rigor |
| 99% | "Very sure" | Larger sample, slower, but more rigorous |

Higher confidence = wider CI = harder to pass = needs more rows to narrow the CI back down.

### How Delphi uses confidence intervals for pass/fail

This is the key insight. Delphi doesn't just check if the observed value passes. It checks if the **entire confidence interval** passes.

For `null_rate < 0.01`:
- Observed: 0.003, CI: [0.002, 0.005] -- **PASS**. Even the worst case (0.005) is below 0.01.
- Observed: 0.008, CI: [0.006, 0.011] -- **FAIL**. The CI upper bound (0.011) exceeds 0.01. We can't be confident the true rate is below 1%.

This is conservative by design. If there's genuine uncertainty about whether the threshold is met, the test fails. This prevents false confidence.

### What does "inconclusive" mean?

Sometimes the sample is too small to produce a meaningful confidence interval. The CI is so wide that it spans both sides of the threshold. Delphi marks this as **INCONCLUSIVE** rather than forcing a pass or fail.

Fix it by increasing the sample ceiling or lowering the confidence level.

## Statistical Methods

Delphi uses different statistical methods depending on what you're measuring.

### Wilson score interval (for rates)

**Used for:** `null_rate`, `uniqueness`

**What it does:** Computes a confidence interval for a proportion (a number between 0 and 1).

**Why Wilson?** The simpler "normal approximation" method breaks down when the rate is very close to 0% or 100%, or when the sample is small. Wilson stays accurate in these edge cases.

**Example:** You sample 10,000 rows and find 30 nulls (null rate = 0.3%). Wilson computes a 95% CI of [0.21%, 0.43%]. If your threshold is `null_rate < 1%`, the CI upper bound (0.43%) is well below 1%, so the test passes.

**In Delphi:**

```python
dt.expect(col("revenue").null_rate < 0.01)      # Wilson CI
dt.expect(col("customer_id").uniqueness > 0.99)  # Wilson CI
```

### t-distribution interval (for means)

**Used for:** `mean`

**What it does:** Computes a confidence interval for the average value of a column.

**Why t-distribution?** When you estimate a mean from a sample, you also have to estimate the variability (standard deviation) from the same sample. The t-distribution accounts for this extra uncertainty. With large samples it's nearly identical to the normal distribution, but with smaller samples it gives wider (more honest) intervals.

**Example:** You sample 10,000 rows and compute mean revenue = $3,000 with standard deviation = $500. The 95% CI is [$2,990, $3,010]. If your threshold is `between 1000 and 5000`, the entire CI fits comfortably within the range.

**In Delphi:**

```python
dt.expect(col("revenue").mean.between(1000, 5000))  # t-distribution CI
```

### Bootstrap (for distributions)

**Used for:** `approx_percentile`, and other complex statistics

**What it does:** A computer-intensive but highly flexible method. It resamples your data 1,000 times (with replacement), computes the statistic on each resample, and uses the distribution of those 1,000 values to build a CI.

**Why bootstrap?** Some statistics (like percentiles or medians) don't have clean mathematical formulas for confidence intervals. Bootstrap works for anything -- you just need to be able to compute the statistic.

**Example:** You want to know the 95th percentile of revenue. Bootstrap resamples your data 1,000 times, computes the 95th percentile each time, and takes the 2.5th and 97.5th percentile of those 1,000 values as the CI bounds.

**In Delphi:**

```python
dt.expect(F.approx_percentile("revenue", 0.95) < 10_000)  # Bootstrap CI
```

### Exact (no CI needed)

**Used for:** `row_count`, `min`, `max`

Some metrics don't need confidence intervals:

- **Row count** comes from the Delta transaction log -- it's the exact count, not an estimate.
- **Min and max** on a sample are checked directly. (Note: the true min/max of the full table could be more extreme than what appears in the sample. Delphi treats these as exact checks on the sampled data.)

## Common Metrics Explained

### Null rate

The fraction of rows where a column's value is missing (NULL).

- `null_rate = 0.0` means no nulls at all
- `null_rate = 0.01` means 1% of rows have a null in that column
- `null_rate = 1.0` means every row is null

```python
col("revenue").null_rate < 0.01  # Less than 1% nulls
```

### Uniqueness

The fraction of values that are distinct. Computed as `COUNT(DISTINCT column) / COUNT(*)`.

- `uniqueness = 1.0` means every value is unique (like a primary key)
- `uniqueness = 0.5` means half the values are duplicates
- `uniqueness = 0.001` means almost all values repeat

```python
col("order_id").uniqueness > 0.99  # Nearly all values unique
```

### Mean

The average value. Sum of all values divided by the count.

```python
col("revenue").mean.between(100, 5000)  # Average revenue in expected range
```

### Standard deviation (stddev)

How spread out the values are around the mean. A small stddev means values cluster tightly around the average. A large stddev means they're spread wide.

- If mean revenue is $1,000 and stddev is $50, most values are between $900-$1,100
- If mean revenue is $1,000 and stddev is $500, values range from $0-$2,000 and beyond

```python
col("revenue").stddev < 500  # Values don't vary too wildly
```

### Percentile

The value below which a given percentage of observations fall. The 95th percentile of revenue means "95% of orders have revenue below this value."

- 50th percentile = median (the middle value)
- 95th percentile = only 5% of values are higher
- 99th percentile = only 1% of values are higher

```python
F.approx_percentile("revenue", 0.95) < 10_000  # 95% of revenue under $10k
```

## Pre-scan and Short-circuiting

### What is the pre-scan?

Before sampling a single row, Delphi reads the Delta transaction log using `DESCRIBE DETAIL`. This is essentially free -- it reads metadata files, not data files.

The pre-scan extracts:
- Total row count
- Column names and types
- Partition columns (for stratified sampling)
- File-level statistics (null counts, min/max per column)

### Short-circuiting

Sometimes the pre-scan alone is enough to answer a question. If the Delta log says a column has zero nulls across all files, then `null_rate < 0.01` trivially passes -- no sampling needed.

This makes some tests nearly instant regardless of table size.

## Stratified Sampling

### What is it?

Regular random sampling picks rows from anywhere in the table. But for timeseries data, this can be skewed -- you might over-sample recent data or miss an entire time period.

**Stratified sampling** divides the table into strata (groups) by time period, then samples from each stratum. This ensures your sample represents the full time range.

### How Delphi does it

1. Auto-detects the time column (by partition keys, clustering keys, or column name/type)
2. Applies a time-range filter with pushdown (so Spark only reads relevant files)
3. Randomly samples within each time period

If Delphi can't detect a time column, it falls back to uniform random sampling.

### Time column auto-detection

Delphi looks for the time column in this priority order:

1. **Partition columns** with a date/timestamp type -- these are explicitly chosen for time-based access, strongest signal
2. **Clustering columns** with a date/timestamp type
3. **Columns named** `timestamp`, `created_at`, `event_time`, `date`, or `event_date` with matching type
4. **The sole date/timestamp column** if there's exactly one

If multiple candidates tie at the same priority level, Delphi skips stratified sampling and logs a warning. This is common with tables that have columns like `created_at`, `updated_at`, and `event_date` all present.

### Setting the time column explicitly

When auto-detection is ambiguous (or you want a specific column), set it explicitly. This is the recommended approach for tables with multiple date/timestamp columns:

**Per-test:**
```python
@datatest("catalog.schema.events", time_column="event_date")
def test_events(dt):
    ...
```

**In delphi.toml (applies to all tests):**
```toml
[delphi]
time_column = "event_date"
```

**In YAML:**
```yaml
table: catalog.schema.events
time_column: event_date
checks:
  - column: status
    null_rate: "< 0.01"
```

**CLI (per-run):**
```bash
delphi run tests/ --time-column event_date
```

You can also customize the list of well-known time column names used by auto-detection:

```toml
[delphi]
time_column_names = ["timestamp", "created_at", "event_time", "date", "event_date"]
```

## Putting It All Together

Here's the full flow for a single expectation `col("revenue").null_rate < 0.01` at 95% confidence:

1. **Pre-scan**: Read Delta log. Table has 500M rows. Column "revenue" exists, type double.
2. **Short-circuit check**: Delta log doesn't have per-file null stats for this column. Can't short-circuit. Proceed to sampling.
3. **Adaptive sample size**: For 95% confidence with 1% margin of error on a proportion, need ~9,604 rows. Within floor (1,000) and ceiling (100,000). Use 9,604.
4. **Sample**: `SELECT * FROM table ORDER BY rand() LIMIT 9604`. Takes ~2 seconds.
5. **Metric**: Count nulls in sample. Found 29 nulls out of 9,604 rows. Observed null rate = 0.302%.
6. **Confidence interval**: Wilson score at 95%: [0.208%, 0.436%].
7. **Pass/fail**: CI upper (0.436%) < threshold (1.0%). **PASS**.
8. **Result**: Report observed=0.00302, CI=[0.00208, 0.00436], method=wilson, sample_size=9604, passed=true.

Total time: ~3 seconds for a 500M-row table.

## Glossary

| Term | Plain English |
|------|--------------|
| **Sample** | A small random subset of your table |
| **Sample size (N)** | How many rows are in the sample |
| **Sample floor** | Minimum rows to sample (default 1,000) |
| **Sample ceiling** | Maximum rows to sample (default 100,000) |
| **Confidence level** | How sure you want to be (90%, 95%, 99%) |
| **Confidence interval (CI)** | The range that likely contains the true value |
| **Margin of error** | Half the width of the confidence interval |
| **Null rate** | Fraction of rows with missing values |
| **Uniqueness** | Fraction of values that are distinct |
| **Mean** | Average value |
| **Standard deviation** | How spread out values are around the mean |
| **Percentile** | Value below which X% of observations fall |
| **Wilson interval** | CI method for proportions (rates) |
| **t-distribution** | CI method for means |
| **Bootstrap** | CI method that works for any statistic via resampling |
| **Pre-scan** | Reading Delta metadata without scanning rows |
| **Short-circuit** | Answering a check from metadata alone, no sampling |
| **Stratified sampling** | Sampling evenly across time periods |
| **Adaptive sampling** | Automatically computing the right sample size |
