# CLAUDE.md

## Project Overview

Delphi (`dbx-delphi` on PyPI) is a probabilistic data test framework for Databricks. It tests terabyte-scale Delta Lake tables in seconds using statistical sampling and confidence intervals instead of exhaustive scans.

## Architecture

Three-stage engine pipeline: **prescan** (Delta file stats, free) -> **sample** (adaptive, random) -> **metrics** (PySpark aggregations) -> **confidence** (Wilson/t/bootstrap CI) -> **result** (pass/fail with evidence).

Supports Delta tables, views, and materialized views. Views skip DESCRIBE DETAIL and fall back to schema + count.

## Tech Stack

- **Python 3.11+**, **PySpark**, **databricks-connect**, **databricks-sdk**
- **scipy** (confidence intervals), **rich** (terminal), **plotly** (notebooks), **click** (CLI), **pyyaml**
- **uv** for dependency management, **hatchling** for builds
- **pytest** for testing

## Project Structure

```
src/delphi/
  __init__.py          # Public API: col, compare, datatest, functions
  config.py            # DelphiConfig, ConnectionConfig, load_config()
  session.py           # Spark session factory with retry
  setup.py             # Interactive delphi setup wizard
  cli.py               # Click CLI: setup, run, inspect
  runner.py            # Orchestrates full pipeline per test
  evidence.py          # Collects violating rows on failure
  engine/
    prescan.py         # Delta metadata + view detection
    sampler.py         # Adaptive sample size + rand() sampling
    metrics.py         # PySpark aggregation runner
  confidence/
    result.py          # ConfidenceResult dataclass
    proportions.py     # Wilson score interval (null_rate, uniqueness)
    means.py           # t-distribution (mean)
    bootstrap.py       # Bootstrap (percentiles, distributions)
  assertions/
    expectation.py     # Expectation dataclass
    column.py          # col() expression tree, MetricAssertion
    dataset.py         # Dataset wrapper, dt.expect()
    functions.py       # F.row_count(), F.approx_percentile()
  comparison/
    compare.py         # compare() reference
  detect/
    time_column.py     # Auto-detect time column for stratified sampling
  dsl/
    decorator.py       # @datatest decorator
    yaml_loader.py     # YAML check parser
  renderers/
    detect.py          # Auto-detect environment
    terminal.py        # Rich tables
    ci.py              # JSON + JUnit XML
    agent.py           # Structured dict
```

## Key Conventions

- **PySpark-native DSL**: `col("x").null_rate < 0.01`, operator overloading, `functions as F`
- **Conservative pass/fail**: entire CI must satisfy threshold, not just the point estimate
- **Adaptive sampling**: sample size computed from confidence level and margin of error, clamped by floor/ceiling
- **Views handled**: prescan detects table type via DESCRIBE TABLE EXTENDED, skips DESCRIBE DETAIL for views
- **numpy bool comparison**: use `== True` / `== False` in tests, not `is True` (scipy/numpy return `np.bool_`)

## Development

```bash
uv sync                              # Install deps
uv run pytest tests/unit/ -v         # Unit tests (no Databricks needed)
uv run delphi --version              # Verify CLI
uv build                             # Build wheel
```

## Testing

- **77 unit tests** in `tests/unit/` — run without Databricks, use mocked Spark
- **Integration tests** in `tests/integration/` — require Databricks credentials, marked with `@pytest.mark.integration`
- **Databricks notebooks** in `tests/integration_delphi_dsl.py` — uploaded and run via SQL warehouse for environments without cluster access
- Test data tables: `delphi.default.prices` (3.2M rows), `delphi.default.security`, `delphi.default.v_prices` (view)

## Dev Workspace

- **Host**: `dbc-71538a0d-9aed.cloud.databricks.com` (free tier, SQL warehouse only)
- **Warehouse ID**: `9b07ed70821c5ba2`
- **Tables**: `delphi.default.prices`, `delphi.default.security`, `delphi.default.v_prices`
- **No clusters available** — use SQL warehouse or upload notebooks for integration testing
- Databricks CLI profile: `delphi` (configured in `~/.databrickscfg`)

## Branching

- Create a feature branch for next version development (e.g., `git checkout -b v0.3.0-dev`)
- Keep `main` stable and releasable

## CI/CD

- `.github/workflows/ci.yml` — tests on Python 3.11-3.13 on push/PR
- `.github/workflows/publish.yml` — publishes to PyPI on GitHub release (trusted publisher)
- PyPI package name: `dbx-delphi`, import as `delphi`

## Current Version

v0.2.0 — adds view/materialized view support.

## Deferred to v0.3.0+

- Comparison metric execution (mean_diff, distribution_shift, KS statistic)
- Notebook renderer (plotly inline charts)
- Constraint auto-suggestion
- Run history / drift detection
- pytest plugin adapter
