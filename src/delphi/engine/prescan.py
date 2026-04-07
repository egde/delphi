"""Delta table pre-scan — extract stats without reading rows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    nullable: bool = True


@dataclass
class PrescanResult:
    table: str
    row_count: int
    partition_columns: list[str]
    clustering_columns: list[str]
    columns: dict[str, ColumnInfo]
    num_files: int = 0
    size_bytes: int = 0


def prescan_table(spark, table: str) -> PrescanResult:
    """Extract Delta table metadata without scanning rows."""
    detail_rows = spark.sql(f"DESCRIBE DETAIL {table}").collect()
    detail = detail_rows[0] if detail_rows else None

    partition_cols = []
    clustering_cols = []
    num_files = 0
    size_bytes = 0

    if detail:
        partition_cols = list(getattr(detail, "partitionColumns", []) or [])
        clustering_cols = list(getattr(detail, "clusteringColumns", []) or [])
        num_files = getattr(detail, "numFiles", 0) or 0
        size_bytes = getattr(detail, "sizeInBytes", 0) or 0

    df = spark.table(table)
    columns = {}
    for f in df.schema.fields:
        columns[f.name] = ColumnInfo(
            name=f.name,
            dtype=f.dataType.simpleString(),
            nullable=getattr(f, "nullable", True),
        )

    row_count = df.count()

    return PrescanResult(
        table=table,
        row_count=row_count,
        partition_columns=partition_cols,
        clustering_columns=clustering_cols,
        columns=columns,
        num_files=num_files,
        size_bytes=size_bytes,
    )
