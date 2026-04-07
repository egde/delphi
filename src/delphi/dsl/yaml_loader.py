"""YAML check parser — maps YAML to Expectation objects."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import yaml

from delphi.assertions.expectation import Expectation


@dataclass
class YamlCheckSet:
    table: str
    expectations: list[Expectation]
    compare_to: str | None = None
    time_column: str | None = None


METRIC_KEYS = {
    "null_rate", "uniqueness", "mean", "min", "max", "stddev",
    "percentile", "row_count", "mean_diff", "distribution_shift",
    "null_rate_diff", "schema_match", "row_count_ratio",
}


def load_yaml_checks(yaml_str: str) -> YamlCheckSet:
    """Parse a YAML check definition into Expectation objects."""
    data = yaml.safe_load(yaml_str)

    table = data["table"]
    compare_to = data.get("compare_to")
    time_column = data.get("time_column")
    expectations = []

    for check in data.get("checks", []):
        column = check.get("column")
        confidence = check.get("confidence", 0.95)

        for key, value in check.items():
            if key in ("column", "confidence"):
                continue
            if key in METRIC_KEYS:
                exp = _parse_threshold(column, key, value, confidence)
                if compare_to:
                    exp.compare_table = compare_to
                expectations.append(exp)

    for check in data.get("comparisons", []):
        column = check.get("column")
        confidence = check.get("confidence", 0.95)

        for key, value in check.items():
            if key in ("column", "confidence"):
                continue
            if key in METRIC_KEYS:
                exp = _parse_threshold(column, key, value, confidence)
                exp.compare_table = compare_to
                expectations.append(exp)

    return YamlCheckSet(table=table, expectations=expectations, compare_to=compare_to, time_column=time_column)


def _parse_threshold(
    column: str | None, metric: str, value: str, confidence: float
) -> Expectation:
    """Parse a threshold string like '< 0.01', '> 0.99', 'between 1000 and 5000'."""
    value = value.strip()

    between_match = re.match(r"between\s+([\d.]+)\s+and\s+([\d.]+)", value)
    if between_match:
        return Expectation(
            column=column, metric=metric,
            threshold_low=float(between_match.group(1)),
            threshold_high=float(between_match.group(2)),
            direction="between", confidence=confidence,
        )

    below_match = re.match(r"<\s*([\d.]+)", value)
    if below_match:
        return Expectation(
            column=column, metric=metric,
            threshold=float(below_match.group(1)),
            direction="below", confidence=confidence,
        )

    above_match = re.match(r">\s*([\d.]+)", value)
    if above_match:
        return Expectation(
            column=column, metric=metric,
            threshold=float(above_match.group(1)),
            direction="above", confidence=confidence,
        )

    raise ValueError(f"Cannot parse threshold: '{value}' for metric '{metric}'")
