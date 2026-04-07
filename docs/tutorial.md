# Delphi Tutorial

This tutorial walks through using Delphi to test Delta Lake tables on Databricks. By the end you'll know how to write data tests in Python and YAML, run them from the CLI, read the output, and integrate with CI/CD.

## Prerequisites

- Python 3.10+
- A Databricks workspace with Unity Catalog
- A table to test (we'll use examples throughout)

Install Delphi:

```bash
pip install dbx-delphi
```

## Part 1: Setup

### Connect to Databricks

Run the interactive setup:

```bash
delphi setup
```

You'll be prompted for:
1. Your workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
2. Your cluster ID (find it under Compute in the Databricks UI)
3. Authentication method (token, OAuth, or environment variables)
4. Default catalog and schema (optional)

This creates a `delphi.toml` file in your project. It's added to `.gitignore` automatically since it may contain credentials.

**Alternative: environment variables**

If you prefer not to use `delphi.toml` (common in CI/CD):

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi_your_token
export DATABRICKS_CLUSTER_ID=0123-456789-abcdef
```

### Verify the connection

```bash
delphi setup --verify
```

### Inspect a table

Before writing tests, explore what you're working with:

```bash
delphi inspect catalog.schema.my_table
```

This shows row count, file count, columns and types, partition keys, and the auto-detected time column -- all without scanning any rows (it reads Delta file stats only).

## Part 2: Your First Test

### The Python DSL

Create a file `tests/test_orders.py`:

```python
from delphi import datatest, col

@datatest("catalog.schema.orders")
def test_order_quality(dt):
    # Null rate on revenue should be below 1%
    dt.expect(col("revenue").null_rate < 0.01)
```

That's it. Run it:

```bash
delphi run tests/test_orders.py
```

You'll see output like:

```
           Delphi Test Results
 ┌──────────┬───────────────────┬──────────┬───────────┬──────────────────┬────────────┬────────┬───────┐
 │ Status   │ Test              │ Observed │ Threshold │ CI               │ Confidence │ Sample │ Time  │
 ├──────────┼───────────────────┼──────────┼───────────┼──────────────────┼────────────┼────────┼───────┤
 │ PASS     │ test_order_qual...│   0.0030 │ < 0.01    │ [0.0022, 0.0040]│ 95%        │ 9,604  │ 12ms  │
 └──────────┴───────────────────┴──────────┴───────────┴──────────────────┴────────────┴────────┴───────┘
```

### What just happened?

1. Delphi read the Delta file stats (free, no row scan)
2. Computed an adaptive sample size (9,604 rows for 95% confidence)
3. Sampled the table using `ORDER BY rand() LIMIT N`
4. Counted nulls in the `revenue` column
5. Computed a Wilson confidence interval: [0.0022, 0.0040]
6. Since the **entire** CI is below 0.01, the test passes

## Part 3: Building Up Tests

### Multiple expectations per test

Group related checks. They share one sample for efficiency:

```python
@datatest("catalog.schema.orders")
def test_order_quality(dt):
    dt.expect(col("revenue").null_rate < 0.01)
    dt.expect(col("revenue").mean.between(100, 5000))
    dt.expect(col("revenue").min > 0)
    dt.expect(col("customer_id").uniqueness > 0.80)
```

All four checks run against the same sampled DataFrame -- one Spark job, not four.

### Higher confidence

Default is 95%. For critical checks, raise it:

```python
dt.expect(col("revenue").null_rate < 0.01, confidence=0.99)
```

Higher confidence = larger sample size = narrower CI = stricter test. At 99% confidence, Delphi samples more rows to reduce the margin of error.

### Dataset-level checks

Use the `functions` module for table-wide metrics:

```python
from delphi import functions as F

@datatest("catalog.schema.orders")
def test_order_volume(dt):
    dt.expect(F.row_count() > 1_000_000)
    dt.expect(F.approx_percentile("revenue", 0.95) < 10_000)
```

`row_count()` uses the pre-scan total (no sampling needed). `approx_percentile` runs on the sampled data.

### Available metrics

| Metric | Example | What it measures |
|--------|---------|-----------------|
| `null_rate` | `col("x").null_rate < 0.01` | Fraction of nulls |
| `uniqueness` | `col("x").uniqueness > 0.99` | Fraction of distinct values |
| `mean` | `col("x").mean.between(10, 100)` | Average value |
| `min` | `col("x").min > 0` | Minimum value |
| `max` | `col("x").max < 1000` | Maximum value |
| `stddev` | `col("x").stddev < 50` | Standard deviation |
| `row_count` | `F.row_count() > 1M` | Total rows (from pre-scan) |
| `approx_percentile` | `F.approx_percentile("x", 0.95) < 100` | Approximate percentile |

### Operators

Each metric supports these comparisons:

```python
col("x").null_rate < 0.01            # below threshold
col("x").uniqueness > 0.99           # above threshold
col("x").mean.between(100, 500)      # within range
```

## Part 4: YAML Checks

For analysts or config-driven pipelines, write checks in YAML:

```yaml
# checks/orders.yaml
table: catalog.schema.orders
checks:
  - column: revenue
    null_rate: "< 0.01"
  - column: revenue
    mean: "between 100 and 5000"
  - column: customer_id
    uniqueness: "> 0.80"
```

Run:

```bash
delphi run checks/orders.yaml
```

YAML produces the exact same results as the Python DSL -- it's parsed into the same internal objects.

### YAML threshold syntax

```yaml
null_rate: "< 0.01"              # below
uniqueness: "> 0.99"             # above
mean: "between 100 and 5000"     # range
```

### YAML with custom confidence

```yaml
checks:
  - column: revenue
    null_rate: "< 0.01"
    confidence: 0.99
```

## Part 5: Understanding Results

### Pass vs Fail

A test **passes** when the entire confidence interval satisfies the threshold.

For `null_rate < 0.01`:
- Observed: 0.003, CI: [0.002, 0.004] -- **PASS** (CI upper 0.004 < 0.01)
- Observed: 0.008, CI: [0.006, 0.011] -- **FAIL** (CI upper 0.011 > 0.01)

This is conservative by design. If there's statistical uncertainty about whether the threshold is met, the test fails. You can:
- Increase `--sample-ceiling` to narrow the CI
- Lower the confidence level
- Relax the threshold

### Evidence rows

When a test fails, Delphi shows sample violating rows:

```
 FAIL  test_nulls    null_rate=0.032  threshold=<0.01  CI=[0.028, 0.036]

   Evidence (5 of 1,612 violating rows):
   ┌────────────┬─────────┬────────────┐
   │ date       │ revenue │ region     │
   ├────────────┼─────────┼────────────┤
   │ 2026-03-12 │ NULL    │ EMEA       │
   │ 2026-03-12 │ NULL    │ NA         │
   └────────────┴─────────┴────────────┘
```

These come from the already-sampled data (no extra scan). Control with:

```bash
delphi run tests/ --evidence-rows 20   # show more
delphi run tests/ --no-evidence        # suppress
```

### Redacting sensitive columns

In `delphi.toml`:

```toml
[delphi]
redact_columns = ["ssn", "email", "phone"]
```

Redacted columns show `[REDACTED]` in evidence output.

## Part 6: CI/CD Integration

### JSON output

```bash
delphi run tests/ --output json
```

Returns a JSON array with one entry per expectation:

```json
[
  {
    "test": "test_nulls:revenue.null_rate",
    "table": "catalog.schema.orders",
    "status": "pass",
    "confidence": 0.95,
    "observed": 0.003,
    "ci_lower": 0.002,
    "ci_upper": 0.004,
    "sample_size": 9604,
    "method": "wilson",
    "duration_ms": 1840
  }
]
```

### JUnit XML for GitHub Actions / Jenkins

```bash
delphi run tests/ --output ci
```

This produces both JSON output (stdout) and a `delphi-results.xml` JUnit file.

### GitHub Actions example

```yaml
# .github/workflows/data-quality.yml
name: Data Quality
on:
  schedule:
    - cron: '0 6 * * *'  # daily at 6am
  workflow_dispatch:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync
      - run: uv run delphi run tests/ --output ci
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      - uses: dorny/test-reporter@v1
        if: always()
        with:
          name: Delphi Results
          path: delphi-results.xml
          reporter: java-junit
```

### Exit codes

- `0` -- all tests pass
- `1` -- any test failed or errored

Use this in CI to gate deployments.

## Part 7: Dataset Comparison

Compare a target table against a reference:

```python
from delphi import datatest, col, compare
from delphi import functions as F

@datatest("catalog.schema.orders_v2")
def test_migration(dt):
    baseline = compare("catalog.schema.orders_v1")
    dt.expect(col("revenue").mean_diff(baseline) < 0.05)
    dt.expect(F.row_count_ratio(baseline).between(0.99, 1.01))
```

Or in YAML:

```yaml
table: catalog.schema.orders_v2
compare_to: catalog.schema.orders_v1
comparisons:
  - column: revenue
    mean_diff: "< 0.05"
  - row_count_ratio: "between 0.99 and 1.01"
```

## Part 8: Multiple Environments

### Named profiles

Configure separate environments in `delphi.toml`:

```toml
[delphi.connection]
host = "https://prod.cloud.databricks.com"
cluster_id = "prod-cluster"
auth_type = "env"

[delphi.connection.profiles.staging]
host = "https://staging.cloud.databricks.com"
cluster_id = "staging-cluster"
auth_type = "env"
```

Run against staging:

```bash
delphi run tests/ --profile staging
```

### Tuning sampling

Override defaults for the run:

```bash
delphi run tests/ --confidence 0.99        # stricter
delphi run tests/ --sample-ceiling 200000   # larger samples
```

Or set in `delphi.toml`:

```toml
[delphi]
default_confidence = 0.95
sample_floor = 1000
sample_ceiling = 100000
```

## Part 9: Running on Databricks

If you're running Delphi inside a Databricks notebook (e.g., as a scheduled job):

```python
# Install from a UC volume or PyPI
%pip install dbx-delphi

from delphi import datatest, col
from delphi.config import DelphiConfig
from delphi.runner import run_expectations

@datatest("catalog.schema.orders")
def test_quality(dt):
    dt.expect(col("revenue").null_rate < 0.01)

# Execute directly using the notebook's spark session
ds = test_quality()
config = DelphiConfig(sample_ceiling=50000)
results = run_expectations(spark, ds.table, ds.expectations, config)

for r in results:
    cr = r.confidence_result
    print(f"{r.test_name}: {r.status} | observed={cr.observed:.4f} CI=[{cr.ci_lower:.4f}, {cr.ci_upper:.4f}]")
    assert r.status == "pass", f"{r.test_name} failed"
```

## Part 10: Project Layout

A typical Delphi project:

```
my-project/
  delphi.toml           # connection + defaults (gitignored)
  tests/
    test_orders.py       # Python tests
    test_customers.py
  checks/
    orders.yaml          # YAML checks
    customers.yaml
  .github/
    workflows/
      data-quality.yml   # CI pipeline
```

## What's Next

- **Constraint auto-suggestion** -- Delphi analyzes your table and suggests checks (coming in v2)
- **Drift detection** -- Compare current run against historical baselines (coming in v2)
- **pytest plugin** -- Run Delphi tests with `pytest` directly (coming in v2)
- **Notebook renderer** -- Interactive plotly charts in Databricks notebooks (coming soon)
