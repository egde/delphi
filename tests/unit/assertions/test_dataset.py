from delphi.assertions.dataset import Dataset
from delphi.assertions.column import col
from delphi.assertions.expectation import Expectation
from delphi.dsl.decorator import datatest


def test_dataset_expect_collects():
    ds = Dataset("test.table")
    ds.expect(col("revenue").null_rate < 0.01)
    ds.expect(col("revenue").mean.between(1000, 5000), confidence=0.99)

    assert len(ds.expectations) == 2
    assert ds.expectations[0].column == "revenue"
    assert ds.expectations[0].metric == "null_rate"
    assert ds.expectations[1].confidence == 0.99


def test_dataset_table_name():
    ds = Dataset("catalog.schema.my_table")
    assert ds.table == "catalog.schema.my_table"


def test_datatest_decorator_registers():
    @datatest("catalog.schema.t")
    def test_fn(dt):
        dt.expect(col("x").null_rate < 0.01)

    assert test_fn._delphi_table == "catalog.schema.t"
    assert callable(test_fn)
