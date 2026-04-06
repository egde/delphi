# Delphi: Probabilistic Data Test Framework for Databricks — Design Spec

## Context

Data engineers and analysts need to test terabyte-scale Delta Lake tables on Databricks with response times in seconds. Full row-level comparison is infeasible at this scale. Delphi uses probabilistic sampling and statistical confidence measures instead of exhaustive scans, supporting TDD workflows, CI/CD pipelines, notebook exploration, and agentic workflows.

No existing framework combines probabilistic confidence bounds + Spark-native execution + multi-runtime rendering + a clean DSL. This is a greenfield open-source project (`dbx-delphi` on PyPI, `import delphi`).

**DSL evolution note:** The brainstorming doc sketched a fluent builder API (`ds.column("revenue").null_rate.is_below(0.01, confidence=0.95)`). During design refinement, we chose a PySpark-native style instead (`col("revenue").null_rate < 0.01`) because it mirrors idioms PySpark users already know — `col()` references, operator overloading, and a `functions as F` namespace. This makes the DSL feel like a natural extension of PySpark rather than a separate API to learn.

---

## Decisions

| Decision | Choice |
|----------|--------|
| Architecture | Pure PySpark + Delta-native (no PyDeequ/JVM dependency) |
| Sample strategy | Adaptive, seeded from Delta file stats, pilot sample fallback |
| Time column | Auto-detect from partition/clustering keys + column types |
| Catalog | Unity Catalog only (v1) |
| Distribution | Open-source from day one |
| Project structure | Monolith, `uv`-managed, src layout |
| DSL style | PySpark-native (`col()`, operator overloading, `functions as F`) |
| Python | >= 3.10 (Databricks Runtime 13.3+) |

---

## Project Layout

```
delphi/
├── pyproject.toml              # uv-managed
├── uv.lock
├── delphi.toml                 # optional project-level defaults
├── src/
│   └── delphi/
│       ├── __init__.py
│       ├── engine/
│       │   ├── prescan.py      # Delta file stats reader (DESCRIBE DETAIL)
│       │   ├── sampler.py      # Adaptive stratified sampling
│       │   └── metrics.py      # PySpark aggregate metric runners
│       ├── confidence/
│       │   ├── proportions.py  # Wilson / Agresti-Coull
│       │   ├── means.py        # t-distribution
│       │   └── bootstrap.py    # Bootstrap for distributions
│       ├── assertions/
│       │   ├── column.py       # ColumnAssertion — col() expressions
│       │   └── dataset.py      # Dataset wrapper — dt.expect()
│       ├── comparison/
│       │   └── compare.py      # Dataset comparison context
│       ├── dsl/
│       │   ├── decorator.py    # @datatest decorator
│       │   └── yaml_loader.py  # YAML check parser
│       ├── renderers/
│       │   ├── terminal.py     # rich tables/confidence bars
│       │   ├── notebook.py     # plotly inline
│       │   ├── ci.py           # JSON + JUnit XML
│       │   └── agent.py        # structured dict
│       ├── detect/
│       │   └── time_column.py  # Time column auto-detection
│       ├── evidence.py         # Violating row collector
│       ├── setup.py            # Interactive Databricks Connect setup
│       ├── session.py          # Spark session builder (profile resolution)
│       └── cli.py              # CLI entry point
├── tests/
│   ├── unit/
│   └── integration/
├── docs/
├── LICENSE
└── README.md
```

### Dependencies

- **pyspark** — sampling, aggregations, DataFrame operations
- **databricks-connect** — remote cluster execution
- **scipy** / **statsmodels** — confidence intervals
- **rich** — terminal rendering
- **plotly** — notebook rendering
- **pyyaml** — YAML check parsing
- **click** — CLI framework

---

## Core Engine

Three-stage pipeline per test:

### Stage 1: Pre-scan (`engine/prescan.py`)

- Calls `DESCRIBE DETAIL` and `DESCRIBE TABLE EXTENDED` on the Delta table
- Extracts column-level stats from the Delta transaction log: null counts, min/max, row count
- For metrics resolvable from file stats alone (e.g., null_rate check when log shows 0 nulls), **short-circuit** with confidence=1.0, zero rows scanned
- Extracts partition columns, liquid clustering keys, and column types for sampler and time-column auto-detection

### Stage 2: Adaptive Sampling (`engine/sampler.py`)

- Seeds adaptive sample size calculation from pre-scan stats
- Computes minimum N for desired confidence level and margin of error (e.g., for proportions: `n = z^2 * p_hat * (1 - p_hat) / E^2`)
- Floor: 1,000 rows. Ceiling: 100,000 rows. Both configurable.
- For timeseries tables: auto-detects time column (see Time Column Auto-Detection below), applies stratified sampling via time-range predicate pushdown + random sampling within each stratum
- **Sampling fallback:** Primary strategy is `WHERE <time_col> BETWEEN ... AND ... ORDER BY rand() LIMIT N` with partition/clustering pushdown. `TABLESAMPLE BUCKET` is an optimization when supported but not relied upon — `rand()` filtering is the universal fallback.
- Returns a PySpark DataFrame for downstream computation

### Stage 3: Metric Computation (`engine/metrics.py`)

- Runs PySpark aggregations on the sampled DataFrame: null counts, distinct counts, mean/stddev, percentiles (`approx_percentile`), custom expressions
- Returns dict of computed metric values passed to the confidence layer

**Flow:** `Table ref -> prescan -> [short-circuit?] -> sampler -> metrics -> confidence -> result`

### Time Column Auto-Detection (`detect/time_column.py`)

Heuristic priority order:
1. **Partition columns** with timestamp/date type — highest signal, these are explicitly chosen for time-based access patterns
2. **Liquid clustering keys** with timestamp/date type — next best signal
3. **Any column** named `timestamp`, `created_at`, `event_time`, `date`, `event_date` (configurable name list) with a matching type
4. **Any column** with timestamp/date type if exactly one exists

If multiple candidates tie at the same priority level, log a warning and require explicit declaration via `time_column="col_name"` in the test or `delphi.toml`. If no candidate is found, skip time-based stratification and fall back to uniform random sampling.

---

## Confidence Layer

Three statistical methods, auto-selected based on assertion type:

| Method | Module | Use case | Approach |
|--------|--------|----------|----------|
| Wilson/Agresti-Coull | `proportions.py` | Rates (null_rate, uniqueness) | Wilson score interval |
| t-distribution | `means.py` | Means, sums | Classic t-interval |
| Bootstrap | `bootstrap.py` | Distributions, percentiles, custom | B=1000 resamples, percentile CI |

### ConfidenceResult

```python
@dataclass
class ConfidenceResult:
    observed: float
    ci_lower: float
    ci_upper: float
    confidence: float        # e.g. 0.95
    method: str              # "wilson", "t", "bootstrap"
    sample_size: int
    passed: bool
```

**Pass/fail logic:** A test passes when the entire confidence interval satisfies the threshold. For `null_rate < 0.01`: passes only if `ci_upper < 0.01`. Conservative — CI straddling the threshold means fail.

---

## DSL — PySpark-native

### Python DSL

```python
from delphi import datatest, col, compare
from delphi import functions as F

@datatest("catalog.schema.revenue_timeseries")
def test_revenue_quality(dt):
    dt.expect(col("revenue").null_rate < 0.01)
    dt.expect(col("revenue").mean.between(1000, 5000), confidence=0.99)
    dt.expect(col("customer_id").uniqueness > 0.99)
    dt.expect(F.row_count() > 1_000_000)
    dt.expect(F.approx_percentile("revenue", 0.95) < 10_000)
```

**Mechanics:**
- `col("name")` returns a `ColumnAssertion` (mirrors `pyspark.sql.functions.col`)
- `.null_rate`, `.mean`, `.uniqueness` return `MetricAssertion` — records what to compute, deferred execution
- Operators (`<`, `>`, `between()`) return `Expectation` — records threshold + direction
- `dt.expect()` collects expectations. After the function returns, the decorator batches compatible metrics into a single engine pass (one sample, multiple aggregations)
- `confidence` kwarg on `expect()`, defaults to 0.95

### YAML Layer

```yaml
table: catalog.schema.revenue_timeseries
checks:
  - column: revenue
    null_rate: "< 0.01"
  - column: revenue
    mean: "between 1000 and 5000"
  - column: customer_id
    uniqueness: "> 0.99"
```

Confidence defaults to 0.95, hidden unless explicitly set. YAML parser produces the same `Expectation` objects as the Python DSL — one execution code path.

### v1 Supported Metrics

`null_rate`, `uniqueness`, `mean`, `min`, `max`, `stddev`, `percentile(n)`, `row_count`

---

## Dataset Comparison

```python
@datatest("catalog.schema.output_table")
def test_matches_expected(dt):
    expected = compare("catalog.schema.expected_table")
    dt.expect(col("revenue").mean_diff(expected) < 0.05)
    dt.expect(col("revenue").distribution_shift(expected) < 0.1)
    dt.expect(F.row_count_ratio(expected).between(0.99, 1.01))
```

### Comparison Metrics (v1)

| Metric | What it measures | Confidence method |
|--------|-----------------|-------------------|
| `mean_diff` | Absolute difference in means as fraction | Welch's t-test |
| `distribution_shift` | KS statistic between distributions | Bootstrap on KS stat |
| `row_count_ratio` | Target / expected row count | Exact (no sampling) |
| `null_rate_diff` | Difference in null rates | Wilson on both, delta method |
| `schema_match` | Column names and types match | Exact (boolean) |

Both tables go through the same engine pipeline, sampled independently. Comparison metrics computed from paired results.

### YAML

```yaml
table: catalog.schema.output_table
compare_to: catalog.schema.expected_table
comparisons:
  - column: revenue
    mean_diff: "< 0.05"
  - column: revenue
    distribution_shift: "< 0.1"
  - row_count_ratio: "between 0.99 and 1.01"
```

---

## Error Handling

### Error Categories

| Category | Example | Severity |
|----------|---------|----------|
| Connection | Can't reach cluster, table not found, permission denied | Fatal — suite aborts (after retry) |
| Schema | Column doesn't exist in table | Fatal for that test, others continue |
| Statistical | Sample too small for requested confidence | Warning, test marked inconclusive |
| Threshold | CI straddles threshold | Soft fail with actionable guidance |

### Rendering

Every error includes a **suggestion** — never just "failed", always "failed because X, try Y". Fuzzy column name matching for typos, sample size recommendations for inconclusive results.

**Terminal:**
```
 x test_revenue_nulls    ERROR         Column "revnue" not found
                                       -> Did you mean "revenue"?

 ? test_distribution     INCONCLUSIVE  Sample size (847) too small for confidence=0.99
                                       -> Increase ceiling or lower confidence to 0.95
```

**CI/JSON:** `"status": "error"` or `"status": "inconclusive"` with `"error_type"`, `"message"`, `"suggestion"` fields. JUnit XML maps to `<error>` or `<failure>` elements.

### Connection Retry Policy

Databricks clusters may be starting up or auto-scaling. Before aborting on connection errors:
- Retry up to 3 times with exponential backoff (2s, 4s, 8s)
- Configurable via `delphi.toml`: `connection_retries = 3`, `connection_timeout = 300` (seconds to wait for cluster)
- Non-retryable errors (permission denied, table not found) fail immediately

---

## Evidence Rows

When a test fails, collect a sample of violating rows from the already-sampled DataFrame (no extra scan).

- Default: 10 rows (configurable via `--evidence-rows N` or `evidence_rows=N`)
- For comparison tests: show rows with largest divergence
- Privacy: `--no-evidence` flag to suppress all evidence rows
- `redact_columns=["ssn", "email"]` — redacted columns show `[REDACTED]` in evidence output (column remains visible but values are masked). Configured in `delphi.toml` or per-test.

**Terminal rendering:**
```
 x test_revenue_nulls    FAIL    null_rate=0.032  threshold=<0.01  CI=[0.028, 0.036]

   Evidence (5 of 1,612 violating rows):
   +------------+---------+-------------+--------+
   | date       | revenue | customer_id | region |
   +------------+---------+-------------+--------+
   | 2026-03-12 | NULL    | C-4821      | EMEA   |
   | 2026-03-12 | NULL    | C-9933      | NA     |
   | ...        |         |             |        |
   +------------+---------+-------------+--------+
```

**CI/JSON:** `"evidence"` array of row dicts. **Agent:** same structured field.

---

## Renderers

Auto-detected based on runtime:

1. `dbutils` available -> Notebook (plotly)
2. `CI` / `GITHUB_ACTIONS` / `JENKINS_URL` env vars -> CI (JSON + JUnit XML)
3. Called as library -> Agent (structured dict)
4. Default -> Terminal (rich)

Override with `--output terminal|notebook|ci|json`.

### Visual outputs

- **Summary table** — pass/fail per test with observed value, CI range, sample size, duration
- **Confidence bars** — observed value position within CI relative to threshold
- **Heatmap** — null rates across all columns (notebook)
- **Histograms** — value distributions with CI bands (notebook)

---

## Setup — Databricks Connect Configuration

`delphi setup` is an interactive command that configures Databricks Connect so all other commands (run, inspect) work out of the box.

### `delphi setup` flow

1. **Prompt for workspace URL** — no default; user provides their workspace URL
2. **Prompt for cluster ID** — the compute resource to connect to. Validate it exists and is compatible (DBR 13.3+).
3. **Prompt for auth method:**
   - **Databricks SDK unified auth (recommended)** — auto-discovers credentials from env vars, `~/.databrickscfg`, or cloud identity. The setup wizard writes a `~/.databrickscfg` profile.
   - **PAT (Personal Access Token)** — user pastes token, written to `~/.databrickscfg` profile
   - **OAuth (U2M)** — opens browser for Databricks OAuth flow
   - **Environment variables** — skip storage, expect `DATABRICKS_HOST`, `DATABRICKS_TOKEN` at runtime
4. **Prompt for default catalog and schema** — optional, used as defaults for unqualified table names in tests
5. **Verify connection** — run a lightweight query (`SELECT 1`) via Databricks Connect to confirm everything works
6. **Write config** — save to `delphi.toml` under `[delphi.connection]`

### Config storage (`delphi.toml`)

```toml
[delphi.connection]
host = "https://adb-1234567890.12.azuredatabricks.net"
cluster_id = "0123-456789-abcdef"
auth_type = "pat"                    # pat | oauth | service_principal | env
token = "dapi..."                    # only for PAT; omitted for other auth types
default_catalog = "main"
default_schema = "default"

[delphi.connection.profiles.staging]
host = "https://adb-staging.azuredatabricks.net"
cluster_id = "0123-456789-staging"
auth_type = "env"
```

### Named profiles

- Default profile is used when no `--profile` flag is passed
- Named profiles (`[delphi.connection.profiles.<name>]`) allow switching between environments: `delphi run tests/ --profile staging`
- CI/CD typically uses `auth_type = "env"` — reads `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID` from environment, no secrets in config files

### `delphi setup --verify`

Re-runs the connection check against the current (or specified) profile without re-prompting. Useful after rotating tokens or changing clusters.

### Security

- PAT tokens in `delphi.toml` are stored in plaintext — the setup wizard warns about this and recommends `auth_type = "env"` or OAuth for shared machines
- `.gitignore` template includes `delphi.toml` by default to prevent accidental credential commits
- `delphi setup` adds `delphi.toml` to `.gitignore` if not already present

### Spark session initialization

All Delphi commands that need Spark resolve the connection as:
1. CLI `--profile` flag → named profile in `delphi.toml`
2. `[delphi.connection]` default profile in `delphi.toml`
3. Databricks SDK unified auth — auto-discovers from environment variables, `~/.databrickscfg`, or cloud identity (Azure/GCP/AWS)

The session factory (`session.py`) uses `databricks.connect.DatabricksSession.builder` with the resolved config. The `databricks-sdk` unified auth handles credential discovery transparently — no auth logic in Delphi itself beyond reading the profile/host/cluster_id from config.

---

## CLI

```
delphi setup                               # interactive Databricks Connect setup
delphi setup --profile PROFILE             # configure a named profile
delphi setup --verify                      # test current connection

delphi run tests/                          # run all Python tests
delphi run tests/test_revenue.py           # run specific file
delphi run checks.yaml                     # run YAML checks
delphi run tests/ --output json            # override renderer
delphi run tests/ --evidence-rows 20       # more evidence rows
delphi run tests/ --no-evidence            # suppress evidence
delphi run tests/ --confidence 0.99        # override default confidence
delphi run tests/ --sample-ceiling 200000  # override max sample size
delphi run tests/ --profile PROFILE        # use named connection profile

delphi inspect catalog.schema.table        # table profile from prescan only

delphi version
```

### Config file (`delphi.toml`)

```toml
[delphi]
default_confidence = 0.95
sample_floor = 1000
sample_ceiling = 100000
evidence_rows = 10
redact_columns = ["ssn", "email"]
connection_retries = 3
connection_timeout = 300
time_column_names = ["timestamp", "created_at", "event_time", "date", "event_date"]
```

---

## Execution Runtimes

| Context | How it runs |
|---------|-------------|
| Local dev (TDD) | Databricks Connect — same code, remote cluster |
| CI/CD | Databricks Connect profile in env vars, JSON output |
| Scheduled job | Databricks Job with the library installed |
| Notebook | Import directly, inline visual output |
| Agentic | Call test suite programmatically, consume result dict |

---

## Structured Result Object

```json
{
  "test": "test_revenue_nulls",
  "table": "catalog.schema.revenue_timeseries",
  "status": "fail",
  "confidence": 0.95,
  "observed": 0.032,
  "threshold": "< 0.01",
  "sample_size": 50000,
  "sample_fraction": 0.002,
  "method": "wilson",
  "ci_lower": 0.028,
  "ci_upper": 0.036,
  "duration_ms": 1840,
  "error_type": null,
  "message": null,
  "suggestion": null,
  "evidence": [
    {"date": "2026-03-12", "revenue": null, "customer_id": "C-4821", "region": "EMEA"}
  ]
}
```

---

## Build Order (v1)

1. **Project scaffolding** — uv init, pyproject.toml, src layout, CI skeleton
2. **Setup & session** — `delphi setup` wizard, profile resolution, Spark session builder, connection verification
3. **Core engine** — prescan, adaptive sampler, metric runner
4. **Confidence layer** — Wilson, t-distribution, bootstrap
5. **Assertions & DSL** — col(), functions, expect(), expression tree, batching
6. **Dataset comparison** — compare(), comparison metrics
7. **Evidence collector** — violating row sampling, redaction
8. **CLI runner** — click-based, test discovery, config loading, `--profile` support
9. **Renderers** — terminal (rich), notebook (plotly), CI (JSON/JUnit XML), agent
10. **YAML layer** — parser, mapping to Expectation objects
11. **Packaging & docs** — README, PyPI metadata, GitHub Actions CI

## Deferred to v2

- Constraint auto-suggestion
- Run history / drift detection
- pytest plugin adapter

---

## Development Environment

During development, use the Databricks free workspace:
- **Host:** `dbc-71538a0d-9aed.cloud.databricks.com`
- **Tables:** `delphi.default.security`, `delphi.default.prices`

These are used for integration tests and smoke tests during development. They are NOT hardcoded into the production code — the `delphi setup` command remains generic.

**Test fixtures (`tests/conftest.py`):**
```python
@pytest.fixture(scope="session")
def spark():
    """Session-scoped Databricks Connect SparkSession."""
    return get_spark_session()

@pytest.fixture(scope="session")
def prices(spark):
    return spark.table("delphi.default.prices")

@pytest.fixture(scope="session")
def security(spark):
    return spark.table("delphi.default.security")
```

Tests skip gracefully if credentials are not configured.

---

## Verification

1. **Setup verification:** `delphi setup --verify` confirms Databricks Connect session, cluster compatibility, and catalog access
2. **Unit tests:** Each module tested in isolation with mock Spark sessions where appropriate
3. **Integration tests:** End-to-end against the dev workspace (`dbc-71538a0d-9aed.cloud.databricks.com`), using `delphi.default.prices` and `delphi.default.security`. Tests skip if no credentials configured.
4. **CLI smoke test:** `delphi run` on sample test files, verify exit codes and output formats
5. **YAML parity:** Same checks in Python and YAML produce identical results
6. **Evidence verification:** Failing tests produce correct violating rows
7. **Renderer verification:** Each output format validated (JSON schema, JUnit XML schema, rich terminal snapshot, plotly HTML)
