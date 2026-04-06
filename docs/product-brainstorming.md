# Brainstorm Summary: Probabilistic Data Test Framework for Databricks

## Problem Statement

Data engineers and analysts need to test terabyte-scale Delta Lake tables on Databricks with
response times in seconds, not minutes. Full row-level comparison is infeasible at this scale.
The framework must support TDD workflows, CI/CD pipelines, notebook exploration, and agentic
workflows — using probabilistic sampling and statistical confidence measures instead of
exhaustive scans.

---

## Key Insights

- **Alex (PM):** Two distinct user personas — engineers doing TDD and analysts doing ad-hoc
  quality checks — require a two-layer API: a rich Python DSL for engineers and a simple YAML
  layer for analysts. The analyst layer should default confidence levels silently; analysts write
  the *what*, the framework handles the *how certain*. Auto-suggestion of tests is a deliberate
  v1 non-goal.

- **Sam (Researcher):** No existing framework combines probabilistic confidence bounds +
  Spark-native execution + multi-runtime rendering + a clean DSL. PyDeequ (AWS) and DQX
  (Databricks Labs) are the closest alternatives but both treat pass/fail thresholds as
  first-class and confidence intervals as an afterthought. Soda Core has the best analyst-facing
  DSL inspiration but is SQL/warehouse-native, not Spark-native. The gap is real and the
  opportunity to build something genuinely new is confirmed.

- **Jordan (SA):** Databricks-first is the right architectural constraint. Delta's `DESCRIBE
  DETAIL` and file-level statistics (column min/max/null counts) are available before touching a
  single row — use them for pre-scan optimisation and to short-circuit trivially passing checks.
  Databricks Connect handles the local dev and CI runtime transparently. Wide timeseries tables
  with liquid clustering are well-suited to time-scoped stratified sampling via
  `TABLESAMPLE BUCKET` + partition predicate pushdown.

---

## Chosen Direction: Option C — Pure PySpark + Delta-native

Skip PyDeequ entirely. Build on:
- **PySpark** — `TABLESAMPLE`, `approx_percentile`, `describe`, aggregate queries
- **Delta Lake** — `DESCRIBE DETAIL`, file statistics, partition pruning, liquid clustering awareness
- **scipy / statsmodels** — Wilson/Agresti-Coull for proportions, bootstrap for distributions
- **Databricks Connect** — transparent local dev and CI execution against remote cluster
- **rich** — terminal rendering; matplotlib/plotly for notebooks; JSON/JUnit XML for CI

Zero JVM/Scala dependency. Fully pip-installable. Clean separation of concerns.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Test Authoring Layer                │
│   Python DSL (engineers)  │  YAML (analysts)        │
│                                                     │
│  @datatest("my_table")                              │
│  def test_revenue_nulls(dataset):                   │
│      assert dataset.column("revenue")               │
│             .null_rate < 0.01                       │
│             .with_confidence(0.95)                  │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│           Probabilistic Assertion Engine             │
│                                                     │
│  1. Pre-scan: Delta file stats (free, no scan)      │
│  2. Stratified sampling: TABLESAMPLE BUCKET         │
│     + time-range predicate pushdown                 │
│  3. Metric computation: PySpark aggregations        │
│  4. Confidence intervals: scipy                     │
│     - Proportions → Wilson / Agresti-Coull          │
│     - Distributions → bootstrap                    │
│     - Means → t-distribution                       │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│              Runtime-Aware Renderer                  │
│                                                     │
│  Terminal   → rich (ANSI tables, heatmaps,          │
│               histograms, confidence bars)          │
│  Notebook   → matplotlib / plotly (inline)          │
│  CI/CD      → JSON report + JUnit XML               │
│  Agent      → structured Python result dict         │
└─────────────────────────────────────────────────────┘
```

---

## Structured Result Object (for CI gates and agentic use)

Every test produces a machine-readable result:

```json
{
  "test": "test_revenue_nulls",
  "table": "catalog.schema.revenue_timeseries",
  "passed": true,
  "confidence": 0.95,
  "observed": 0.003,
  "threshold": 0.01,
  "sample_size": 50000,
  "sample_fraction": 0.002,
  "method": "wilson",
  "ci_lower": 0.001,
  "ci_upper": 0.005,
  "duration_ms": 1840
}
```

---

## Execution Runtimes

| Context         | How it runs                                           |
|-----------------|-------------------------------------------------------|
| Local dev (TDD) | Databricks Connect — same code, remote cluster        |
| CI/CD           | Databricks Connect profile in env vars, JSON output   |
| Scheduled job   | Databricks Job with the library installed             |
| Notebook        | Import directly, inline visual output                 |
| Agentic         | Call test suite programmatically, consume result dict |

---

## DSL Design Sketch

**Engineer layer (Python):**
```python
from dbx_delphi import datatest, dataset

@datatest("catalog.schema.revenue_timeseries")
def test_revenue_nulls(ds):
    ds.column("revenue").null_rate.is_below(0.01, confidence=0.95)

@datatest("catalog.schema.revenue_timeseries")
def test_revenue_distribution(ds):
    ds.column("revenue").mean.is_between(1000, 5000, confidence=0.99)
    ds.column("revenue").has_no_anomalous_spikes(window="7d")
```

**Analyst layer (YAML):**
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

Confidence defaults to 0.95 in the YAML layer — never exposed unless the analyst explicitly
sets it.

---

## Visual Output Goals

- **Heatmap** — null rates across all columns at a glance
- **Histograms** — value distributions per column with confidence bands overlaid
- **Confidence bar** — per-test visual showing observed value vs. threshold with CI range
- **Summary table** — pass/fail per test with observed value, confidence, and sample size
- **Drift indicator** — optional: compare current run against a baseline (v2)

---

## Recommended Build Order (v1 scope)

1. **Core engine** — Delta pre-scan, stratified sampler, PySpark metric runner
2. **Confidence layer** — Wilson, t-distribution, bootstrap implementations
3. **Python DSL** — `@datatest` decorator, fluent assertion API
4. **CLI runner** — `deltatest run tests/` with rich terminal output
5. **Renderer** — terminal (rich) + notebook (plotly) + CI (JSON/JUnit XML)
6. **YAML layer** — analyst-facing thin wrapper over the Python DSL

**Deferred to v2:**
- Constraint auto-suggestion
- Run history / drift detection
- pytest plugin adapter

---

## Open Questions

- What is the target sample size strategy? Fixed N (e.g. 50k rows), fixed fraction, or
  adaptive based on desired margin of error?
- Should the framework auto-detect the time column for timeseries sampling, or require
  explicit declaration in the test?
- Unity Catalog assumed throughout — any legacy Hive metastore tables to support?
- Distribution: internal library or open-source from day one?
