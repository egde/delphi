"""Dataset comparison context."""

from __future__ import annotations


class ComparisonRef:
    """Reference to a comparison table."""

    def __init__(self, table: str):
        self._table = table

    @property
    def table(self) -> str:
        return self._table


def compare(table: str) -> ComparisonRef:
    """Create a reference to a comparison table."""
    return ComparisonRef(table)
