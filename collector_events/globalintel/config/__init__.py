"""Configuration loader for globalintel extractors.

All hardcoded constants (country lists, API URLs, channel lists, feed lists,
ticker symbols, etc.) are stored in JSON files under this directory and loaded
once at import time, immutable in memory.
"""

from __future__ import annotations

import json
from pathlib import Path

_CONFIG_DIR = Path(__file__).parent


def _load(name: str) -> dict | list:
    """Load a JSON config file from the config directory."""
    with open(_CONFIG_DIR / name, encoding="utf-8") as f:
        return json.load(f)
