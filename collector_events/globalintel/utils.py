"""Shared utility functions for globalintel extractors."""

from __future__ import annotations


def safe_float(val: object) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
