from unittest.mock import MagicMock
from delphi.engine.prescan import prescan_table, PrescanResult


def _mock_spark_with_stats(row_count=1000000):
    spark = MagicMock()
    detail_row = MagicMock()
    detail_row.partitionColumns = ["date"]
    detail_row.clusteringColumns = []
    detail_row.numFiles = 100
    detail_row.sizeInBytes = 1_000_000_000
    spark.sql.return_value.collect.return_value = [detail_row]

    field_revenue = MagicMock()
    field_revenue.name = "revenue"
    field_revenue.dataType.simpleString.return_value = "double"
    field_revenue.nullable = True

    field_date = MagicMock()
    field_date.name = "date"
    field_date.dataType.simpleString.return_value = "date"
    field_date.nullable = True

    schema = MagicMock()
    schema.fields = [field_revenue, field_date]
    spark.table.return_value.schema = schema
    spark.table.return_value.count.return_value = row_count

    return spark


def test_prescan_returns_result():
    spark = _mock_spark_with_stats()
    result = prescan_table(spark, "catalog.schema.t")
    assert isinstance(result, PrescanResult)
    assert result.row_count == 1000000
    assert result.partition_columns == ["date"]


def test_prescan_extracts_column_info():
    spark = _mock_spark_with_stats()
    result = prescan_table(spark, "catalog.schema.t")
    assert "revenue" in result.columns
    assert result.columns["revenue"].dtype == "double"


def test_prescan_detects_partition_columns():
    spark = _mock_spark_with_stats()
    result = prescan_table(spark, "catalog.schema.t")
    assert "date" in result.partition_columns
