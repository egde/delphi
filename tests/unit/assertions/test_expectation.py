from delphi.assertions.expectation import Expectation


def test_expectation_stores_fields():
    exp = Expectation(
        column="revenue", metric="null_rate",
        threshold=0.01, direction="below", confidence=0.95,
    )
    assert exp.column == "revenue"
    assert exp.metric == "null_rate"
    assert exp.threshold == 0.01
    assert exp.direction == "below"
