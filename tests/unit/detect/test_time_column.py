from delphi.detect.time_column import detect_time_column
from delphi.engine.prescan import PrescanResult, ColumnInfo


def _prescan(columns, partition_cols=None, clustering_cols=None):
    return PrescanResult(
        table="t", row_count=1000,
        partition_columns=partition_cols or [],
        clustering_columns=clustering_cols or [],
        columns={c.name: c for c in columns},
    )


def test_partition_column_preferred():
    result = detect_time_column(
        _prescan(
            [ColumnInfo("id", "int"), ColumnInfo("date", "date"), ColumnInfo("ts", "timestamp")],
            partition_cols=["date"],
        )
    )
    assert result == "date"


def test_clustering_column_second():
    result = detect_time_column(
        _prescan(
            [ColumnInfo("id", "int"), ColumnInfo("event_time", "timestamp")],
            clustering_cols=["event_time"],
        )
    )
    assert result == "event_time"


def test_named_column_third():
    result = detect_time_column(
        _prescan([ColumnInfo("id", "int"), ColumnInfo("created_at", "timestamp")])
    )
    assert result == "created_at"


def test_sole_timestamp_column():
    result = detect_time_column(
        _prescan([ColumnInfo("id", "int"), ColumnInfo("my_ts", "timestamp")])
    )
    assert result == "my_ts"


def test_no_time_column():
    result = detect_time_column(
        _prescan([ColumnInfo("id", "int"), ColumnInfo("name", "string")])
    )
    assert result is None


def test_ambiguous_returns_none():
    result = detect_time_column(
        _prescan([ColumnInfo("ts1", "timestamp"), ColumnInfo("ts2", "timestamp")])
    )
    assert result is None
