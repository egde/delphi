import pytest
from unittest.mock import MagicMock

from delphi.engine.sampler import compute_sample_size, sample_dataframe, SamplePlan
from delphi.engine.prescan import PrescanResult, ColumnInfo


def _prescan(row_count=10_000_000):
    return PrescanResult(
        table="t", row_count=row_count, partition_columns=["date"],
        clustering_columns=[], columns={
            "revenue": ColumnInfo("revenue", "double"),
            "date": ColumnInfo("date", "date"),
        },
    )


def test_compute_sample_size_respects_floor():
    plan = compute_sample_size(
        prescan=_prescan(row_count=500),
        confidence=0.95, sample_floor=1000, sample_ceiling=100000,
    )
    assert plan.n == 500


def test_compute_sample_size_respects_ceiling():
    plan = compute_sample_size(
        prescan=_prescan(row_count=100_000_000),
        confidence=0.99, sample_floor=1000, sample_ceiling=100000,
    )
    assert plan.n <= 100000


def test_compute_sample_size_adaptive():
    plan_95 = compute_sample_size(prescan=_prescan(), confidence=0.95, sample_floor=1000, sample_ceiling=100000)
    plan_99 = compute_sample_size(prescan=_prescan(), confidence=0.99, sample_floor=1000, sample_ceiling=100000)
    assert plan_99.n >= plan_95.n


def test_compute_sample_size_small_table():
    plan = compute_sample_size(
        prescan=_prescan(row_count=100),
        confidence=0.95, sample_floor=1000, sample_ceiling=100000,
    )
    assert plan.n == 100
    assert plan.use_full_table is True


def test_sample_dataframe_uses_fraction_not_sort():
    df = MagicMock()
    spark = MagicMock()
    spark.table.return_value = df
    plan = SamplePlan(n=1000, fraction=0.001, use_full_table=False)

    sample_dataframe(spark, "t", plan)

    df.sample.assert_called_once()
    _, kwargs = df.sample.call_args
    assert kwargs["withReplacement"] is False
    # 10% headroom applied over plan.fraction
    assert kwargs["fraction"] == pytest.approx(0.0011)
    df.orderBy.assert_not_called()


def test_sample_dataframe_full_table_returns_df_unchanged():
    df = MagicMock()
    spark = MagicMock()
    spark.table.return_value = df
    plan = SamplePlan(n=100, fraction=1.0, use_full_table=True)

    assert sample_dataframe(spark, "t", plan) is df
    df.sample.assert_not_called()
