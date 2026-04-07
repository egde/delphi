# Delphi

Probabilistic data test framework for Databricks. Test terabyte-scale Delta Lake tables in seconds using statistical sampling and confidence intervals instead of exhaustive scans.

```python
from delphi import datatest, col
from delphi import functions as F

@datatest("catalog.schema.revenue")
def test_revenue_quality(dt):
    dt.expect(col("revenue").null_rate < 0.01)
    dt.expect(col("revenue").mean.between(1000, 5000), confidence=0.99)
    dt.expect(col("customer_id").uniqueness > 0.99)
    dt.expect(F.row_count() > 1_000_000)
```

## Why Delphi?

Full row-level scans are infeasible on large Delta tables. Delphi samples intelligently and uses statistical confidence intervals to determine pass/fail, giving you fast, reliable data quality checks with quantified uncertainty.

- **Fast** -- Adaptive sampling reads thousands of rows, not billions
- **Statistically rigorous** -- Wilson, t-distribution, and bootstrap confidence intervals
- **PySpark-native** -- `col()`, operator overloading, and `functions as F` feel like PySpark
- **Two-layer API** -- Python DSL for engineers, YAML for analysts
- **Multi-runtime** -- Terminal, notebook, CI/CD (JSON + JUnit XML), and agentic output
- **Databricks-first** -- Delta file stats for free pre-scan, Unity Catalog native

## Install

```bash
pip install dbx-delphi
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add dbx-delphi
```

Requires Python 3.10+ and a Databricks workspace with Unity Catalog.

## Quick Start

### 1. Configure connection

```bash
delphi setup
```

This walks you through connecting to your Databricks workspace. Alternatively, set environment variables:

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

### 2. Write a test

```python
# tests/test_revenue.py
from delphi import datatest, col
from delphi import functions as F

@datatest("catalog.schema.revenue")
def test_nulls(dt):
    dt.expect(col("revenue").null_rate < 0.01)

@datatest("catalog.schema.revenue")
def test_distribution(dt):
    dt.expect(col("revenue").mean.between(1000, 5000), confidence=0.99)
    dt.expect(col("revenue").stddev < 2000)
    dt.expect(F.row_count() > 100_000)
```

### 3. Run

```bash
delphi run tests/
```

## DSL Reference

### Column Metrics

Use `col("name")` to start a column expression, then chain a metric:

```python
from delphi import col

col("revenue").null_rate < 0.01       # Null rate below 1%
col("revenue").mean.between(100, 500) # Mean within range
col("revenue").min > 0                # Minimum above 0
col("revenue").max < 1_000_000        # Maximum below 1M
col("revenue").stddev < 100           # Standard deviation below 100
col("id").uniqueness > 0.99           # 99%+ distinct values
```

Available metrics: `null_rate`, `uniqueness`, `mean`, `min`, `max`, `stddev`

### Dataset-Level Functions

```python
from delphi import functions as F

F.row_count() > 1_000_000                       # Minimum row count
F.approx_percentile("revenue", 0.95) < 10_000   # 95th percentile cap
```

### Confidence Levels

Every expectation defaults to 95% confidence. Override per-expectation:

```python
dt.expect(col("revenue").null_rate < 0.01)                  # 95% (default)
dt.expect(col("revenue").mean.between(100, 500), confidence=0.99)  # 99%
```

A test passes only when the **entire** confidence interval satisfies the threshold. This is conservative -- if the CI straddles the threshold, the test fails.

### Time Column for Sampling

Delphi auto-detects the time column for stratified sampling (partition keys > clustering keys > well-known names like `date`, `timestamp`, `created_at`). When your table has multiple date/timestamp columns and auto-detection is ambiguous, set it explicitly:

**Per-test (decorator):**
```python
@datatest("catalog.schema.events", time_column="event_date")
def test_events(dt):
    dt.expect(col("status").null_rate < 0.01)
```

**In delphi.toml (global):**
```toml
[delphi]
time_column = "event_date"
```

**CLI (per-run):**
```bash
delphi run tests/ --time-column event_date
```

### YAML Checks

For analysts who prefer configuration over code:

```yaml
# checks/revenue.yaml
table: catalog.schema.revenue
time_column: event_date  # optional: explicit time column for sampling
checks:
  - column: revenue
    null_rate: "< 0.01"
  - column: revenue
    mean: "between 1000 and 5000"
  - column: customer_id
    uniqueness: "> 0.99"
```

Confidence defaults to 0.95 in YAML. Override per-check:

```yaml
  - column: revenue
    mean: "between 1000 and 5000"
    confidence: 0.99
```

Run YAML checks:

```bash
delphi run checks/revenue.yaml
```

### Dataset Comparison

Compare a table against a reference:

```python
from delphi import datatest, col, compare
from delphi import functions as F

@datatest("catalog.schema.output")
def test_matches_expected(dt):
    expected = compare("catalog.schema.expected")
    dt.expect(col("revenue").mean_diff(expected) < 0.05)
    dt.expect(F.row_count_ratio(expected).between(0.99, 1.01))
```

## CLI

```
delphi setup                          # Interactive connection setup
delphi setup --verify                 # Test current connection
delphi setup --profile staging        # Configure a named profile

delphi run tests/                     # Run all tests in directory
delphi run tests/test_revenue.py      # Run specific file
delphi run checks/revenue.yaml        # Run YAML checks
delphi run tests/ --profile staging   # Use named profile
delphi run tests/ --output json       # JSON output
delphi run tests/ --confidence 0.99   # Override confidence
delphi run tests/ --sample-ceiling 200000
delphi run tests/ --evidence-rows 20  # More evidence rows
delphi run tests/ --no-evidence       # Suppress evidence
delphi run tests/ --time-column event_date  # Explicit time column

delphi inspect catalog.schema.table   # Table profile (no sampling)

delphi --version
```

## Configuration

Create `delphi.toml` in your project root (or use `delphi setup`):

```toml
[delphi]
default_confidence = 0.95
sample_floor = 1000
sample_ceiling = 100000
evidence_rows = 10
redact_columns = ["ssn", "email"]
connection_retries = 3
connection_timeout = 300
time_column = "event_date"  # optional: explicit time column for sampling

# Serverless (recommended)
[delphi.connection]
host = "https://your-workspace.cloud.databricks.com"
serverless = true
auth_type = "env"
default_catalog = "main"
default_schema = "default"
# budget_policy_id = "policy-abc-123"  # optional: usage/budget policy for serverless

# Classic cluster (alternative)
# [delphi.connection]
# host = "https://your-workspace.cloud.databricks.com"
# cluster_id = "0123-456789-abcdef"
# auth_type = "env"
```

### Named Profiles

```toml
[delphi.connection.profiles.staging]
host = "https://staging.cloud.databricks.com"
serverless = true
auth_type = "env"
```

### Authentication

| Method | `auth_type` | How |
|--------|------------|-----|
| Environment variables | `env` | `DATABRICKS_HOST` + `DATABRICKS_TOKEN` |
| Personal Access Token | `pat` | Token stored in `delphi.toml` |
| OAuth (U2M) | `oauth` | Browser-based flow |
| Databricks SDK unified auth | (any) | Auto-discovers from env, `~/.databrickscfg`, or cloud identity |

## How It Works

Delphi runs a three-stage pipeline for each test:

```
Table ref --> Pre-scan --> Sample --> Metrics --> Confidence --> Result
```

1. **Pre-scan** -- Reads Delta file stats (`DESCRIBE DETAIL`) for free. Column-level null counts, min/max, row count. Short-circuits trivially passing checks without scanning a single row.

2. **Adaptive Sampling** -- Computes the minimum sample size needed for the desired confidence and margin of error. Floors at 1,000 rows, caps at 100,000. For timeseries tables, auto-detects the time column and applies stratified sampling.

3. **Metric Computation** -- Runs PySpark aggregations on the sampled DataFrame. Multiple expectations on the same table share one sample.

4. **Confidence Intervals** -- Routes each metric to the appropriate statistical method:

   | Metric type | Method |
   |-------------|--------|
   | Rates (null_rate, uniqueness) | Wilson score interval |
   | Means | t-distribution |
   | Distributions, percentiles | Bootstrap (B=1000) |
   | Row count, min, max | Exact (no CI needed) |

5. **Evidence** -- On failure, collects up to 10 violating rows from the already-sampled data (no extra scan). Sensitive columns can be redacted.

## Output Formats

Delphi auto-detects your environment:

| Environment | Renderer | Details |
|-------------|----------|---------|
| Terminal | `rich` | Color tables, confidence bars |
| CI/CD | JSON + JUnit XML | `delphi-results.xml` for GitHub Actions, Jenkins |
| Notebook | `plotly` (coming soon) | Inline charts |
| Programmatic | Structured dict | For agentic/orchestration use |

Override with `--output terminal|ci|json`.

## Error Handling

Every error includes a suggestion:

```
 FAIL  test_nulls    null_rate=0.032  threshold=<0.01  CI=[0.028, 0.036]

 ERROR test_typo     Column "revnue" not found
                     -> Did you mean "revenue"?

 INCONCLUSIVE test_x Sample size (847) too small for confidence=0.99
                     -> Increase ceiling or lower confidence to 0.95
```

Connection errors retry up to 3 times with exponential backoff (configurable).

## Documentation

- [Tutorial](docs/tutorial.md) -- Step-by-step guide from setup to CI/CD
- [Statistics Guide](docs/statistics-guide.md) -- Plain-language explanation of confidence intervals, sampling methods, and every statistical concept used in Delphi
- [Databricks Connect Guide](docs/databricks-connect-guide.md) -- Serverless vs cluster, version matching, and troubleshooting

## Development

```bash
git clone https://github.com/egde/delphi.git
cd delphi
uv sync

# Run unit tests (no Databricks needed)
uv run pytest tests/unit/ -v

# Run integration tests (requires Databricks credentials)
uv run pytest tests/integration/ -v -m integration
```

## License

MIT
