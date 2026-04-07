"""Dataset wrapper — collects expectations from dt.expect()."""

from __future__ import annotations

from delphi.assertions.expectation import Expectation


class Dataset:
    def __init__(self, table: str):
        self._table = table
        self._expectations: list[Expectation] = []

    @property
    def table(self) -> str:
        return self._table

    @property
    def expectations(self) -> list[Expectation]:
        return self._expectations

    def expect(self, expectation: Expectation, confidence: float = 0.95) -> None:
        expectation.confidence = confidence
        self._expectations.append(expectation)
