# globalintel/tagging/asset_resolver.py
"""
AssetResolver — determines which forex pairs are affected by an IntelItem.

Resolution priority:
    1. ``item.extra["affected_assets"]`` — explicit list from extractor
    2. ``item.country`` — country → currency → pairs
    3. ``item.tags`` — text search for country/currency names

Extracted from GlobalTagManager to isolate country→currency→pair logic.
"""

from __future__ import annotations

from forex_shared.domain.intel import IntelItem
from forex_shared.logging.loggable import Loggable

from .reference_data import COUNTRY_CURRENCY_MAP


class AssetResolver(Loggable):
    """Resolve IntelItem → affected forex pairs."""

    def resolve(self, item: IntelItem) -> list[str]:
        """Determine which forex pairs are affected by this item.

        Returns list of forex pair strings (e.g., ["EURUSD"]).
        """
        # 1. Explicit assets from extractor
        explicit = item.extra.get("affected_assets")
        if explicit and isinstance(explicit, list):
            return [str(a) for a in explicit if a]

        # 2. Country field → currency → pairs
        currencies: set[str] = set()
        if item.country:
            for cc in item.country:
                currency = self._country_to_currency(cc)
                if currency:
                    currencies.add(currency)

        # 3. Search item.tags for country/currency mentions
        if not currencies:
            lowered_tags = " ".join(item.tags).lower()
            for country_key, currency in COUNTRY_CURRENCY_MAP.items():
                if country_key in lowered_tags:
                    currencies.add(currency)

        if not currencies:
            return []

        return self._currencies_to_pairs(currencies, item.domain)

    @staticmethod
    def _country_to_currency(country: str) -> str | None:
        """Map country name/code to ISO 4217 currency code."""
        return COUNTRY_CURRENCY_MAP.get(country.lower().strip())

    @staticmethod
    def _currencies_to_pairs(currencies: set[str], domain: str) -> list[str]:
        """Convert currency codes to forex pair strings.

        Rules:
        - EUR/GBP/AUD/NZD/CAD → {CCY}USD
        - JPY/CHF → USD{CCY}
        - USD affected → skip (too broad)
        - Safe-haven fallback for conflict/environment domains
        """
        base_currencies = {"EUR", "GBP", "AUD", "NZD", "CAD"}
        quote_currencies = {"JPY", "CHF"}

        pairs: list[str] = []

        for ccy in currencies:
            if ccy == "USD":
                continue  # USD affected → impact on all majors — too broad
            elif ccy in base_currencies:
                pairs.append(f"{ccy}USD")
            elif ccy in quote_currencies:
                pairs.append(f"USD{ccy}")
            elif ccy in {"CNY", "KRW", "SGD", "INR"}:
                pairs.append(f"USD{ccy}" if ccy == "CNY" else f"{ccy}USD")

        # Geopolitical/conflict with no clear currency: safe havens benefit
        if not pairs and domain in {"conflict", "environment", "sanctions", "cyber"}:
            pairs = ["USDJPY", "USDCHF"]

        return pairs[:3]  # Limit to top 3 to avoid spam
