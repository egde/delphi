"""Smoke tests — verify connectivity and basic table access."""

import pytest

pytestmark = pytest.mark.integration


def test_prices_readable(prices):
    assert prices.count() > 0


def test_security_readable(security):
    assert security.count() > 0


def test_prices_has_expected_columns(prices):
    col_names = [f.name for f in prices.schema.fields]
    assert "ticker" in col_names
    assert "date" in col_names
    assert "close" in col_names


def test_security_has_expected_columns(security):
    col_names = [f.name for f in security.schema.fields]
    assert "ticker" in col_names
    assert "name" in col_names
    assert "sector" in col_names
