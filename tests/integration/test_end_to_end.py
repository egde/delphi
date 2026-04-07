"""End-to-end tests — full pipeline through DSL."""

import pytest
from delphi import col, datatest
from delphi.config import DelphiConfig
from delphi.runner import run_expectations
from delphi.assertions.expectation import Expectation

pytestmark = pytest.mark.integration


def test_null_rate_on_prices(spark):
    expectations = [
        Expectation(column="close", metric="null_rate", threshold=0.01, direction="below"),
    ]
    config = DelphiConfig(sample_ceiling=10000)
    results = run_expectations(spark, "delphi.default.prices", expectations, config, test_name="e2e")
    assert len(results) == 1
    assert results[0].status in ("pass", "fail")
    assert results[0].confidence_result is not None


def test_datatest_decorator_runs(spark):
    @datatest("delphi.default.prices")
    def test_prices(dt):
        dt.expect(col("close").null_rate < 0.01)

    ds = test_prices()
    assert len(ds.expectations) == 1

    config = DelphiConfig(sample_ceiling=5000)
    results = run_expectations(spark, ds.table, ds.expectations, config, test_name="decorator_test")
    assert len(results) == 1
    assert results[0].confidence_result is not None
