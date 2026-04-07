"""Confidence interval result dataclass."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConfidenceResult:
    observed: float
    ci_lower: float
    ci_upper: float
    confidence: float
    method: str
    sample_size: int
    passed: bool
