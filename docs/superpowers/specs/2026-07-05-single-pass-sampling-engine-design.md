# Single-Pass Sampling Engine — Performance Redesign

**Date:** 2026-07-05
**Status:** Approved (design), pending implementation plan
**Scope:** Internal engine only (`engine/sampler.py`, `engine/metrics.py`, `runner.py`). No public API/DSL changes.

## Problem

Delphi's promise is "test TB-scale tables in seconds via sampling," but the current
engine undercuts it:

1. **The sampled DataFrame is never materialized.** `sample_dataframe` returns a lazy
   `df.orderBy(rand()).limit(n)`. Every downstream action re-executes the entire sampling
   lineage from scratch.
2. **Each metric is a separate Spark action.** `compute_metrics` runs `df.count()` plus a
   separate `.count()` / `.agg().collect()` per expectation. With the un-materialized sample,
   N expectations trigger **N+1 full re-scans**, each including a global `orderBy(rand())`
   sort — a full shuffle of the whole table. This is the opposite of the design goal.
3. **`orderBy(rand()).limit(n)` is the most expensive sampling method available** — it forces
   a total sort of the entire table.
4. **Latent correctness bug:** because the sample re-randomizes on every action, the rows
   measured by `compute_metrics` differ from the rows returned by `collect_evidence`. Failing
   evidence may not correspond to the rows that produced the verdict.

Note: `prescan`'s `df.count()` is **not** a full scan on Delta — Databricks serves `COUNT(*)`
from transaction-log stats. It is left unchanged.

## Goals

- Reduce regular-metric Spark jobs from **N+1 → ~2** (materialize + one fused aggregation).
- Eliminate the full-table sort.
- Fix the evidence/measurement sample mismatch.

## Constraints (hard)

- **Preserve the public API/DSL.** `col().null_rate`, `F.row_count()`, `dt.expect()`, YAML
  checks, and all renderers keep working unchanged. The internal metric result dict keeps its
  `{col}:{metric}` shape so `runner._compute_confidence` needs no edits.
- **Validate on the free-tier SQL warehouse** (no clusters) via the existing
  `tests/integration_delphi_dsl.py` notebook path.
- **Keep the 77 unit tests green** (updated deliberately where the mock interaction changes,
  never broken silently).

Not a hard constraint: bit-for-bit reproducibility. Fraction sampling introduces run-to-run
sample-size variation; estimators remain statistically unbiased (confidence math reads the
actual observed `n`).

## Design

### Data flow (per test batch)

```
prescan  → row_count (Delta metadata, cheap — unchanged)
sample   → df.sample(fraction)          (no sort)
runner   → sampled_df.cache(); count()  (materialize once)
metrics  → ONE df.agg(*exprs).collect() (all regular metrics, one job)
confidence → unchanged (reads observed n)
evidence → reads the SAME cached sample (no re-randomization)
finally  → sampled_df.unpersist()
```

### 1. Sampling (`engine/sampler.py`)

- Replace `df.orderBy(rand()).limit(n)` with
  `df.sample(withReplacement=False, fraction=plan.fraction)`.
- Fraction includes headroom so Bernoulli undershoot rarely drops below the floor:
  `fraction = min(1.0, plan.n / row_count * 1.10)`.
- The **actual** observed row count (from the materialized sample) is used as `n` everywhere
  downstream — not the planned `n`.
- Full-table path (`use_full_table`) unchanged.
- `sample_dataframe` stays side-effect-free (no `.cache()` inside it) so it remains unit-testable;
  caching/unpersist is owned by the runner.

### 2. Fused metric aggregation (`engine/metrics.py`)

`compute_metrics` builds a list of aliased Column expressions from all regular expectations and
executes **one** `df.agg(*exprs).collect()[0]`, then demultiplexes results back into the existing
`{col}:{metric}` dict.

| Metric        | Fused expression(s)                                    |
|---------------|--------------------------------------------------------|
| `null_rate`   | `sum(col(c).isNull().cast('long'))` + shared `count('*')` |
| `uniqueness`  | `approx_count_distinct(c)` (HLL, ~2% error, fusable)   |
| `mean`        | `avg(c)`, `stddev(c)`                                   |
| `min`/`max`/`stddev` | `min(c)` / `max(c)` / `stddev(c)`               |
| `percentile`  | `percentile_approx(c, p)`                               |
| `row_count`   | from `prescan.row_count` (no aggregation)              |

Aliases are unique and reversible (e.g. `null__{col}`, `uniq__{col}`, `mean__{col}`,
`std__{col}`, `p__{col}`). A single shared `count('*')` alias supplies `total` for all
metrics that need it.

`uniqueness` uses HLL (`approx_count_distinct`) rather than exact `countDistinct`, keeping it
inside the single fused pass instead of spawning its own shuffle/job.

### 3. Comparison metrics (`engine/metrics.py`)

- `compute_comparison_metrics`: fuse `mean_diff`, `null_rate_diff`, and `row_count_ratio` per
  side; cache both target and expected samples.
- `distribution_shift` still `.collect()`s column values (scipy `ks_2samp` needs raw values),
  but against the cached sample.

### 4. Reconciliation

Unchanged. Reconciliation joins the full target table for accuracy and is out of scope for this
pass.

### 5. Error handling & edge cases

- If `fraction` rounds to an empty sample, the `use_full_table` path already triggers when
  `row_count ≤ floor`; keep divide-by-zero guards on all rate computations.
- `unpersist()` runs in a `finally` and must never mask the original exception.
- HLL is effectively exact on tiny samples — no special-casing needed.

### Resolved sub-decisions

- **Fraction headroom:** keep the `×1.10` heuristic (simple, avoids repeated re-sampling to hit
  the floor).
- **Materialize vs. fold-into-agg:** keep the explicit two-step (cache + count to materialize,
  then fused agg) for clarity. Collapsing the materialize-count into the fused agg is a possible
  later micro-optimization, not part of this design.

## Testing & validation

- **Unit (mocked Spark):** assert a single `.agg()` call carrying multiple columns; assert the
  alias→metric demux; assert the sample is cached and unpersisted (including on error).
- **Correctness:** on a fixed in-memory DataFrame, assert fused-agg results equal the previous
  per-metric results (within HLL tolerance for uniqueness).
- **Performance (free-tier SQL warehouse):** extend `tests/integration_delphi_dsl.py` to time a
  multi-expectation batch against `delphi.default.prices` (3.2M rows) before/after and record the
  job-count / wall-time reduction.

## Out of scope

- Reconciliation join optimization.
- Prescan changes (Delta `COUNT(*)` is already metadata-served).
- Any public API, DSL, config, or renderer changes.
