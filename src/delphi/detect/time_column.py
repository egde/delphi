"""Auto-detect time column for stratified sampling."""

from __future__ import annotations

import logging

from delphi.engine.prescan import PrescanResult

logger = logging.getLogger(__name__)

TIME_TYPES = {"date", "timestamp", "timestamp_ntz"}

DEFAULT_TIME_NAMES = [
    "timestamp", "created_at", "event_time", "date", "event_date",
]


def detect_time_column(
    prescan: PrescanResult,
    time_column_names: list[str] | None = None,
    explicit: str | None = None,
) -> str | None:
    """Detect the time column using a priority heuristic.

    If explicit is set, validates it exists in the table and returns it.
    Otherwise, auto-detects using the priority heuristic.
    """
    if explicit:
        if explicit in prescan.columns:
            return explicit
        logger.warning("Explicit time column '%s' not found in table %s", explicit, prescan.table)
        return None

    names = time_column_names or DEFAULT_TIME_NAMES

    def _is_time_type(col_name: str) -> bool:
        col = prescan.columns.get(col_name)
        return col is not None and col.dtype in TIME_TYPES

    # Priority 1: partition columns with time type
    candidates = [c for c in prescan.partition_columns if _is_time_type(c)]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        logger.warning("Ambiguous time column among partition columns: %s", candidates)
        return None

    # Priority 2: clustering columns with time type
    candidates = [c for c in prescan.clustering_columns if _is_time_type(c)]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        logger.warning("Ambiguous time column among clustering columns: %s", candidates)
        return None

    # Priority 3: well-known names with matching type
    candidates = [n for n in names if n in prescan.columns and _is_time_type(n)]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        logger.warning("Ambiguous time column among known names: %s", candidates)
        return None

    # Priority 4: sole timestamp/date column
    time_cols = [name for name, col in prescan.columns.items() if col.dtype in TIME_TYPES]
    if len(time_cols) == 1:
        return time_cols[0]
    if len(time_cols) > 1:
        logger.warning("Multiple time columns found, none matched by name: %s", time_cols)
        return None

    return None
