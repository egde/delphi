"""Table pre-scan — extract stats without reading rows where possible."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


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
    is_view: bool = False
    table_type: str = "TABLE"


def _detect_table_type(spark, table: str) -> str:
    """Detect whether the target is a TABLE, VIEW, or MATERIALIZED_VIEW."""
    try:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        for row in rows:
            key = str(getattr(row, "col_name", "")).strip().lower()
            if key == "type":
                return str(getattr(row, "data_type", "TABLE")).strip().upper()
    except Exception:
        pass
    return "TABLE"


def _prescan_delta(spark, table: str) -> dict:
    """Read Delta file stats via DESCRIBE DETAIL. Returns empty dict on failure."""
    try:
        detail_rows = spark.sql(f"DESCRIBE DETAIL {table}").collect()
        if detail_rows:
            detail = detail_rows[0]
            return {
                "partition_columns": list(getattr(detail, "partitionColumns", []) or []),
                "clustering_columns": list(getattr(detail, "clusteringColumns", []) or []),
                "num_files": getattr(detail, "numFiles", 0) or 0,
                "size_bytes": getattr(detail, "sizeInBytes", 0) or 0,
            }
    except Exception as e:
        logger.debug("DESCRIBE DETAIL not available for %s: %s", table, e)
    return {}


def prescan_table(spark, table: str) -> PrescanResult:
    """Extract table metadata. Works for Delta tables, views, and materialized views.

    For Delta tables: reads file stats from the transaction log (free, no scan).
    For views: skips DESCRIBE DETAIL and falls back to schema + count only.
    """
    table_type = _detect_table_type(spark, table)
    is_view = table_type in ("VIEW", "MATERIALIZED_VIEW")

    # Only attempt DESCRIBE DETAIL on actual tables
    delta_stats = {}
    if not is_view:
        delta_stats = _prescan_delta(spark, table)

    if is_view:
        logger.info("%s is a %s — skipping Delta file stats", table, table_type)

    # Schema and count work for all table types
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
        partition_columns=delta_stats.get("partition_columns", []),
        clustering_columns=delta_stats.get("clustering_columns", []),
        columns=columns,
        num_files=delta_stats.get("num_files", 0),
        size_bytes=delta_stats.get("size_bytes", 0),
        is_view=is_view,
        table_type=table_type,
    )
