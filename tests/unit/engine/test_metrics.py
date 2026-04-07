from unittest.mock import MagicMock
from delphi.engine.metrics import compute_metrics
from delphi.assertions.expectation import Expectation


def test_compute_null_rate():
    expectations = [
        Expectation(column="revenue", metric="null_rate", threshold=0.01, direction="below"),
    ]
    df = MagicMock()
    df.count.return_value = 1000
    df.filter.return_value.count.return_value = 30

    results = compute_metrics(df, expectations)
    assert "revenue:null_rate" in results
    assert results["revenue:null_rate"]["null_count"] == 30
    assert results["revenue:null_rate"]["total"] == 1000


def test_compute_row_count():
    expectations = [
        Expectation(column=None, metric="row_count", threshold=1000, direction="above"),
    ]
    df = MagicMock()
    df.count.return_value = 5000

    results = compute_metrics(df, expectations)
    assert "row_count" in results
    assert results["row_count"]["count"] == 5000
