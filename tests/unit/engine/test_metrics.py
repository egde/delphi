from unittest.mock import MagicMock
from delphi.engine.metrics import compute_metrics
from delphi.assertions.expectation import Expectation


def _mock_df(agg_row: dict):
    """A MagicMock df whose single .agg(...).collect()[0] returns agg_row."""
    df = MagicMock()
    df.agg.return_value.collect.return_value = [agg_row]
    return df


def test_compute_null_rate():
    expectations = [
        Expectation(column="revenue", metric="null_rate", threshold=0.01, direction="below"),
    ]
    df = _mock_df({"nr__revenue": 30, "cnt": 1000})

    results = compute_metrics(df, expectations)

    assert results["revenue:null_rate"]["null_count"] == 30
    assert results["revenue:null_rate"]["total"] == 1000
    # single fused aggregation, not per-metric filter/count
    df.agg.assert_called_once()
    df.filter.assert_not_called()


def test_compute_row_count_uses_shared_count():
    expectations = [
        Expectation(column=None, metric="row_count", threshold=1000, direction="above"),
    ]
    df = _mock_df({"cnt": 5000})

    results = compute_metrics(df, expectations)
    assert results["row_count"]["count"] == 5000


def test_multiple_metrics_single_agg():
    expectations = [
        Expectation(column="revenue", metric="null_rate", threshold=0.01, direction="below"),
        Expectation(column="revenue", metric="mean", threshold=100, direction="below"),
        Expectation(column="user_id", metric="uniqueness", threshold=0.9, direction="above"),
        Expectation(column="price", metric="min", threshold=0, direction="above"),
        Expectation(column="price", metric="max", threshold=999, direction="below"),
    ]
    df = _mock_df({
        "nr__revenue": 5, "mean__revenue": 42.0, "std__revenue": 3.0,
        "uq__user_id": 950, "min__price": 1.0, "max__price": 998.0, "cnt": 1000,
    })

    results = compute_metrics(df, expectations)

    # exactly ONE aggregation for the whole batch
    df.agg.assert_called_once()
    assert results["revenue:null_rate"] == {"null_count": 5, "total": 1000}
    assert results["revenue:mean"] == {"mean": 42.0, "std": 3.0, "total": 1000}
    assert results["user_id:uniqueness"] == {"distinct_count": 950, "total": 1000}
    assert results["price:min"]["value"] == 1.0
    assert results["price:max"]["value"] == 998.0


def test_stddev_and_percentile_demux():
    expectations = [
        Expectation(column="latency", metric="stddev", threshold=50, direction="below"),
        Expectation(column="latency", metric="percentile", threshold=100, direction="below",
                    metric_args={"percentile": 0.95}),
    ]
    df = _mock_df({"sdev__latency": 12.5, "pct__latency__1": 88.0, "cnt": 1000})

    results = compute_metrics(df, expectations)
    assert results["latency:stddev"]["value"] == 12.5
    assert results["latency:percentile"]["value"] == 88.0


def test_null_aggregates_are_guarded_to_zero():
    # An all-null / empty sample draw yields NULL aggregates; they must not propagate None.
    expectations = [
        Expectation(column="revenue", metric="mean", threshold=100, direction="below"),
        Expectation(column="revenue", metric="null_rate", threshold=0.5, direction="below"),
    ]
    df = _mock_df({"mean__revenue": None, "std__revenue": None,
                   "nr__revenue": None, "cnt": 0})

    results = compute_metrics(df, expectations)
    assert results["revenue:mean"] == {"mean": 0, "std": 0, "total": 0}
    assert results["revenue:null_rate"] == {"null_count": 0, "total": 0}
