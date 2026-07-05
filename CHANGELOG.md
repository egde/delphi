# Changelog

All notable changes to Delphi (`dbx-delphi`) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-07-05

Performance redesign of the sampling engine. Internal-only — the public API, DSL,
YAML checks, config, and output formats are unchanged.

### Changed
- **Fraction-based sampling.** Sampling now uses Bernoulli `df.sample(fraction)` instead
  of `orderBy(rand()).limit(n)`, eliminating a full-table sort. Sample size becomes
  approximate; confidence intervals read the actual observed row count, so estimates
  remain statistically valid. Results are no longer bit-for-bit reproducible run-to-run.
- **Single fused aggregation.** All regular metrics for a test batch are computed in one
  PySpark aggregation over a single cached sample — one job for the whole batch instead of
  a separate scan per expectation (previously N+1 full re-scans). Comparison metrics run in
  one aggregation pass per side.
- **Uniqueness via HyperLogLog.** `uniqueness` now uses `approx_count_distinct` so it stays
  within the single fused pass.

### Fixed
- **Evidence/measurement mismatch.** The sample is materialized once (cached) and reused for
  both the metric pass and evidence collection; previously the lazy sample re-randomized
  between passes, so failing evidence rows could differ from the rows that produced the verdict.
- **HLL over-count breaking uniqueness checks.** The HLL distinct count is clamped to the row
  total; an overestimate on near-unique columns pushed the proportion above 1 and produced a
  NaN Wilson interval, silently failing valid unique-key checks.
- **Percentile alias collision.** Two percentile checks on the same column no longer collide in
  the fused aggregation.
- **NULL aggregate guards.** Empty or all-null sample draws no longer propagate `None` into the
  confidence computation.

### Performance
- ~8.3× faster on a representative multi-metric batch against a 3.2M-row table (single fused
  sampled query ~1.2s vs. ~10s for the prior sort-per-metric pattern), measured on a Databricks
  SQL warehouse.

## [0.5.1]

### Fixed
- Use `F.try_divide` in reconciliation to avoid division-by-zero errors.

## [0.5.0]

### Added
- Data reconciliation metrics: `coverage`, `match_rate` (exact and tolerance-based), and
  `mean_deviation` for ETL validation, migration testing, and regression checks.

## [0.4.0]

### Added
- Comparison metric execution (`mean_diff`, `distribution_shift`, `row_count_ratio`,
  `null_rate_diff`, `schema_match`).
- Notebook renderer with inline plotly charts.
- Run history and drift detection.
- Total-time reporting.

## [0.3.0]

### Added
- Serverless compute support (`ConnectionConfig.serverless`).
- Actionable guidance for Databricks Connect / DBR version mismatches.
- Databricks Connect compatibility guide.

## [0.2.0]

### Added
- Support for views and materialized views (schema + count fallback, skipping
  `DESCRIBE DETAIL`).

[0.6.0]: https://github.com/egde/delphi/releases/tag/v0.6.0
[0.5.1]: https://github.com/egde/delphi/releases/tag/v0.5.1
[0.5.0]: https://github.com/egde/delphi/releases/tag/v0.5.0
[0.4.0]: https://github.com/egde/delphi/releases/tag/v0.4.0
[0.3.0]: https://github.com/egde/delphi/releases/tag/v0.3.0
[0.2.0]: https://github.com/egde/delphi/releases/tag/v0.2.0
