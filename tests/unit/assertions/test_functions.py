from delphi.assertions import functions as F
from delphi.assertions.expectation import Expectation


def test_row_count_above():
    exp = F.row_count() > 1_000_000
    assert isinstance(exp, Expectation)
    assert exp.column is None
    assert exp.metric == "row_count"
    assert exp.threshold == 1_000_000
    assert exp.direction == "above"


def test_approx_percentile():
    exp = F.approx_percentile("revenue", 0.95) < 10_000
    assert exp.column == "revenue"
    assert exp.metric == "percentile"
    assert exp.metric_args == {"percentile": 0.95}
    assert exp.threshold == 10_000
    assert exp.direction == "below"
