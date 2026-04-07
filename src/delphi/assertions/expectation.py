"""Expectation — a recorded assertion to evaluate."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Expectation:
    column: str | None
    metric: str
    threshold: float | None = None
    threshold_low: float | None = None
    threshold_high: float | None = None
    direction: str | None = None  # "below", "above", "between"
    confidence: float = 0.95
    metric_args: dict = field(default_factory=dict)
    compare_table: str | None = None
