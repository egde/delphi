"""@datatest decorator — registers test functions for execution."""

from __future__ import annotations

import functools
from collections.abc import Callable

from delphi.assertions.dataset import Dataset


def datatest(table: str) -> Callable:
    """Decorator that marks a function as a Delphi data test."""
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            ds = Dataset(table)
            fn(ds, *args, **kwargs)
            return ds
        wrapper._delphi_table = table
        wrapper._delphi_fn = fn
        return wrapper
    return decorator
