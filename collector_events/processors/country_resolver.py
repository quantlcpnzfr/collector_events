# services/collector_events/collector_events/processors/country_resolver.py
"""
CountryResolver — resolves ISO 2-letter country codes from free text.

Uses ``unified-countries_main.json`` as the keyword dictionary:
~176 countries with name, currency, and keyword lists (capitals, leaders,
organizations, alternate names).

Design:
    - Singleton: dictionary loaded once, reused across all calls.
    - Multi-country: a single text can match multiple countries (e.g. a
      conflict event mentioning both "Ukraine" and "Russia").
    - Case-insensitive: all matching is done on lowercased text.
    - Word-boundary aware for short keywords to avoid false positives
      (e.g. "US" inside "bushfire" or "focus").
    - Priority: longer keywords are checked first to prefer specific
      matches over substrings.

Usage::

    resolver = CountryResolver()
    codes = resolver.resolve("Kremlin threatens Ukraine over NATO expansion")
    # → ["RU", "UA"]
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

#from numpy import info

from forex_shared.logging.get_logger import get_logger

logger = get_logger(__name__)

_DICT_PATH = Path(__file__).parent / "country_dictionary" / "unified-countries_main.json"

# Keywords shorter than this need word-boundary regex to avoid false positives
_SHORT_KEYWORD_THRESHOLD = 4


class CountryResolver:
    """Resolves country codes from text using a keyword dictionary.

    Singleton by default.  Pass ``dictionary=`` to bypass the singleton
    and use a custom dict (useful for testing).
    """

    _instance: CountryResolver | None = None
    _loaded: bool = False

    def __new__(cls, dictionary: dict[str, Any] | None = None) -> CountryResolver:
        if dictionary is not None:
            # Bypass singleton — return a fresh instance for testing
            return super().__new__(cls)
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, dictionary: dict[str, Any] | None = None) -> None:
        if dictionary is not None:
            # Fresh instance with injected data — always load
            self._lookup: list[tuple[str, str, re.Pattern | None]] = []
            self._state_affiliation_map: dict[str, list[str]] = {}
            self._currency_map: dict[str, str | None] = {}
            
            self._name_map: dict[str, str] = {}
            
            
            self._build(dictionary)
            return
        if self._loaded:
            return
        self._loaded = True
        self._lookup = []
        self._state_affiliation_map = {}
        self._currency_map = {}
        self._name_map: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        """Load the unified countries dictionary from disk and build lookup tables."""
        try:
            with open(_DICT_PATH, encoding="utf-8") as f:
                data: dict[str, Any] = json.load(f)
        except Exception as exc:
            logger.error("CountryResolver: failed to load %s: %s", _DICT_PATH, exc)
            return
        self._build(data)

    def _build(self, data: dict[str, Any]) -> None:
        """Build lookup tables from a countries dictionary."""

        entries: list[tuple[str, str, re.Pattern | None]] = []
        for iso_code, info in data.items():
            iso_code = iso_code.upper()
            
            # My girl deverloper asked for this to be added to the country ref output, 
            # so here we are
            for iso_code, info in data.items():
                self._name_map[iso_code.upper()] = info.get("name", "")
                
            name = info.get("name", "")
            keywords = info.get("keywords", [])

            # Add the country name itself as a keyword
            all_keywords = list(set([name.lower()] + [kw.lower() for kw in keywords]))

            for kw in all_keywords:
                if not kw:
                    continue
                # Short keywords need word-boundary matching
                if len(kw) < _SHORT_KEYWORD_THRESHOLD:
                    pattern = re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
                    entries.append((iso_code, kw, pattern))
                else:
                    entries.append((iso_code, kw, None))

        # Sort: longer keywords first (more specific matches first)
        entries.sort(key=lambda e: len(e[1]), reverse=True)
        self._lookup = entries

        # Build state_affiliation → country code map (for RSS feeds)
        for iso_code, info in data.items():
            name = info.get("name", "")
            if name:
                self._state_affiliation_map[name.lower()] = [iso_code.upper()]

        # Cache currency codes so get_currency() never re-reads the file
        for iso_code, info in data.items():
            self._currency_map[iso_code.upper()] = info.get("currency")

        logger.info(
            "CountryResolver: loaded %d countries, %d keyword entries",
            len(data), len(self._lookup),
        )

    def resolve(self, text: str) -> list[str]:
        """Resolve country codes from free text.

        Args:
            text: Any text (title, body, tags concatenated). Case-insensitive.

        Returns:
            List of unique ISO 2-letter country codes found, ordered by
            first occurrence in the lookup. Empty list if none found.
        """
        if not text:
            return []

        text_lower = text.lower()
        found: dict[str, None] = {}  # ordered set via dict

        for iso_code, kw, pattern in self._lookup:
            if iso_code in found:
                continue
            if pattern is not None:
                # Short keyword — use regex word boundary
                if pattern.search(text_lower):
                    found[iso_code] = None
            else:
                # Long keyword — simple substring match is safe
                if kw in text_lower:
                    found[iso_code] = None

        return list(found.keys())

    def resolve_state_affiliation(self, affiliation: str) -> list[str]:
        """Map a state_affiliation string (e.g. "Russia") to country codes.

        RSS feeds carry ``state_affiliation`` in their FeedDef (e.g. "Russia"
        for TASS/RT, "China" for Xinhua, "Qatar" for Al Jazeera).

        Returns:
            List of ISO 2-letter codes, or empty list if not mapped.
        """
        if not affiliation:
            return []
        return self._state_affiliation_map.get(affiliation.lower(), [])

    def get_currency(self, iso_code: str) -> str | None:
        """Get the ISO 4217 currency code for a country.

        Convenience method for downstream services that need country → currency.
        Uses the cached dictionary (loaded once at init).
        """
        return self._currency_map.get(iso_code.upper())
    
    # Developer girl asked for this to be added to the country ref output, so here we are
    def get_country_ref(self, iso_code: str) -> dict[str, str | None]:
        code = iso_code.upper()
        return {
            "code": code,
            "name": self._name_map.get(code),
            "currency": self._currency_map.get(code),
        }

    # Developer girl asked for this to be added to the country ref output, so here we are
    def resolve_refs(self, text: str) -> list[dict[str, str | None]]:
        return [self.get_country_ref(code) for code in self.resolve(text)]
