from unittest.mock import MagicMock, patch
from delphi.engine.prescan import prescan_table, PrescanResult


def _make_field(name, dtype):
    field = MagicMock()
    field.name = name
    field.dataType.simpleString.return_value = dtype
    field.nullable = True
    return field


def _make_describe_extended_rows(table_type="TABLE"):
    """Mock rows from DESCRIBE TABLE EXTENDED with a Type row."""
    rows = []
    for col_name, data_type in [("col_name", "data_type"), ("Type", table_type)]:
        row = MagicMock()
        row.col_name = col_name
        row.data_type = data_type
        rows.append(row)
    return rows


def _mock_spark_table(row_count=1000000, table_type="TABLE"):
    spark = MagicMock()

    # DESCRIBE TABLE EXTENDED (for type detection)
    # DESCRIBE DETAIL (for Delta stats)
    detail_row = MagicMock()
    detail_row.partitionColumns = ["date"]
    detail_row.clusteringColumns = []
    detail_row.numFiles = 100
    detail_row.sizeInBytes = 1_000_000_000

    ext_rows = _make_describe_extended_rows(table_type)

    def sql_side_effect(query):
        result = MagicMock()
        if "DESCRIBE TABLE EXTENDED" in query:
            result.collect.return_value = ext_rows
        elif "DESCRIBE DETAIL" in query:
            if table_type in ("VIEW", "MATERIALIZED_VIEW"):
                result.collect.side_effect = Exception("EXPECT_TABLE_NOT_VIEW")
            else:
                result.collect.return_value = [detail_row]
        return result

    spark.sql.side_effect = sql_side_effect

    schema = MagicMock()
    schema.fields = [_make_field("revenue", "double"), _make_field("date", "date")]
    spark.table.return_value.schema = schema
    spark.table.return_value.count.return_value = row_count

    return spark


def test_prescan_returns_result():
    spark = _mock_spark_table()
    result = prescan_table(spark, "catalog.schema.t")
    assert isinstance(result, PrescanResult)
    assert result.row_count == 1000000
    assert result.partition_columns == ["date"]
    assert result.is_view is False
    assert result.table_type == "TABLE"


def test_prescan_extracts_column_info():
    spark = _mock_spark_table()
    result = prescan_table(spark, "catalog.schema.t")
    assert "revenue" in result.columns
    assert result.columns["revenue"].dtype == "double"


def test_prescan_detects_partition_columns():
    spark = _mock_spark_table()
    result = prescan_table(spark, "catalog.schema.t")
    assert "date" in result.partition_columns


def test_prescan_view():
    """Views skip DESCRIBE DETAIL and still return schema + count."""
    spark = _mock_spark_table(row_count=5000, table_type="VIEW")
    result = prescan_table(spark, "catalog.schema.v")
    assert result.is_view is True
    assert result.table_type == "VIEW"
    assert result.row_count == 5000
    assert "revenue" in result.columns
    assert result.partition_columns == []
    assert result.num_files == 0


def test_prescan_materialized_view():
    """Materialized views also skip DESCRIBE DETAIL."""
    spark = _mock_spark_table(row_count=10000, table_type="MATERIALIZED_VIEW")
    result = prescan_table(spark, "catalog.schema.mv")
    assert result.is_view is True
    assert result.table_type == "MATERIALIZED_VIEW"
    assert result.row_count == 10000
    assert result.partition_columns == []
