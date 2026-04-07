"""Integration tests for views — verify the full pipeline works without DESCRIBE DETAIL."""

import pytest
from delphi import col, datatest
from delphi import functions as F
from delphi.config import DelphiConfig
from delphi.runner import run_expectations
from delphi.engine.prescan import prescan_table
from delphi.assertions.expectation import Expectation

pytestmark = pytest.mark.integration

VIEW_TABLE = "delphi.default.v_prices"


def test_prescan_detects_view(spark):
    """Prescan identifies the target as a view and skips Delta stats."""
    result = prescan_table(spark, VIEW_TABLE)
    assert result.is_view is True
    assert result.table_type in ("VIEW", "MATERIALIZED_VIEW")
    assert result.row_count > 0
    assert result.num_files == 0
    assert result.partition_columns == []
    assert len(result.columns) > 0


def test_view_null_rate(spark):
    """Null rate check works on a view."""
    expectations = [
        Expectation(column="close", metric="null_rate", threshold=0.01, direction="below"),
    ]
    config = DelphiConfig(sample_ceiling=10000)
    results = run_expectations(spark, VIEW_TABLE, expectations, config, test_name="view_test")
    assert len(results) == 1
    assert results[0].status in ("pass", "fail")
    assert results[0].confidence_result is not None
    assert results[0].confidence_result.method == "wilson"


def test_view_mean(spark):
    """Mean check works on a view."""
    expectations = [
        Expectation(column="close", metric="mean", threshold_low=0, threshold_high=100000, direction="between"),
    ]
    config = DelphiConfig(sample_ceiling=5000)
    results = run_expectations(spark, VIEW_TABLE, expectations, config, test_name="view_mean")
    assert results[0].status == "pass"
    assert results[0].confidence_result.method == "t"


def test_view_full_dsl(spark):
    """Full DSL pipeline works on a view."""
    @datatest(VIEW_TABLE)
    def test_v_prices(dt):
        dt.expect(col("close").null_rate < 0.01)
        dt.expect(col("close").mean.between(0, 100000), confidence=0.99)
        dt.expect(F.row_count() > 1000)

    ds = test_v_prices()
    config = DelphiConfig(sample_ceiling=10000)
    results = run_expectations(spark, ds.table, ds.expectations, config, test_name="view_dsl")

    assert len(results) == 3
    for r in results:
        assert r.status == "pass", f"{r.test_name} failed: {r.error or r.confidence_result}"
