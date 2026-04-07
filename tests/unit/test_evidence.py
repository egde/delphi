from unittest.mock import MagicMock
from delphi.evidence import collect_evidence
from delphi.assertions.expectation import Expectation


def test_collect_evidence_null_rate():
    df = MagicMock()
    violation_df = MagicMock()
    violation_df.limit.return_value.toPandas.return_value.to_dict.return_value = [
        {"revenue": None, "date": "2026-01-01"},
    ]
    df.filter.return_value = violation_df

    exp = Expectation(column="revenue", metric="null_rate", threshold=0.01, direction="below")
    rows = collect_evidence(df, exp, max_rows=10)
    assert len(rows) == 1


def test_collect_evidence_respects_max_rows():
    df = MagicMock()
    violation_df = MagicMock()
    violation_df.limit.return_value.toPandas.return_value.to_dict.return_value = [
        {"x": i} for i in range(5)
    ]
    df.filter.return_value = violation_df

    exp = Expectation(column="x", metric="null_rate", threshold=0.01, direction="below")
    rows = collect_evidence(df, exp, max_rows=5)
    assert len(rows) <= 5


def test_collect_evidence_redacts_columns():
    df = MagicMock()
    violation_df = MagicMock()
    violation_df.limit.return_value.toPandas.return_value.to_dict.return_value = [
        {"revenue": None, "ssn": "123-45-6789"},
    ]
    df.filter.return_value = violation_df

    exp = Expectation(column="revenue", metric="null_rate", threshold=0.01, direction="below")
    rows = collect_evidence(df, exp, max_rows=10, redact_columns=["ssn"])
    assert rows[0]["ssn"] == "[REDACTED]"
