from delphi.assertions.column import col
from delphi.assertions.expectation import Expectation


def test_col_null_rate_below():
    exp = col("revenue").null_rate < 0.01
    assert isinstance(exp, Expectation)
    assert exp.column == "revenue"
    assert exp.metric == "null_rate"
    assert exp.threshold == 0.01
    assert exp.direction == "below"


def test_col_uniqueness_above():
    exp = col("customer_id").uniqueness > 0.99
    assert isinstance(exp, Expectation)
    assert exp.column == "customer_id"
    assert exp.metric == "uniqueness"
    assert exp.threshold == 0.99
    assert exp.direction == "above"


def test_col_mean_between():
    exp = col("revenue").mean.between(1000, 5000)
    assert isinstance(exp, Expectation)
    assert exp.column == "revenue"
    assert exp.metric == "mean"
    assert exp.threshold_low == 1000
    assert exp.threshold_high == 5000
    assert exp.direction == "between"


def test_col_min_above():
    exp = col("price").min > 0
    assert exp.metric == "min"
    assert exp.direction == "above"
    assert exp.threshold == 0


def test_col_stddev_below():
    exp = col("price").stddev < 100
    assert exp.metric == "stddev"
    assert exp.direction == "below"
