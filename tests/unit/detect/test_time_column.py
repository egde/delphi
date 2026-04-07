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


def test_explicit_overrides_auto_detection():
    """Explicit time column bypasses heuristic."""
    prescan = _prescan(
        [ColumnInfo("id", "int"), ColumnInfo("ts1", "timestamp"), ColumnInfo("ts2", "timestamp")],
    )
    # Would be ambiguous without explicit
    assert detect_time_column(prescan) is None
    # Explicit picks ts2
    assert detect_time_column(prescan, explicit="ts2") == "ts2"


def test_explicit_overrides_partition_preference():
    """Explicit time column wins even over partition column."""
    prescan = _prescan(
        [ColumnInfo("date", "date"), ColumnInfo("updated_at", "timestamp")],
        partition_cols=["date"],
    )
    # Auto would pick date (partition)
    assert detect_time_column(prescan) == "date"
    # Explicit picks updated_at
    assert detect_time_column(prescan, explicit="updated_at") == "updated_at"


def test_explicit_nonexistent_returns_none():
    """Explicit column that doesn't exist returns None."""
    prescan = _prescan([ColumnInfo("id", "int"), ColumnInfo("date", "date")])
    result = detect_time_column(prescan, explicit="nonexistent")
    assert result is None
