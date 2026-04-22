# globalintel/tagging/reference_data.py
"""
Static reference data for the GlobalTag pipeline.

Contains domain TTLs, severity scores, bias defaults, country→currency mappings,
major forex pairs, and safe-haven currency sets.

Extracted from GlobalTagManager to keep configuration data separate from logic.
"""

from forex_shared.domain.intel import IntelBias, IntelSeverity

# ── TTL per domain (seconds) ──────────────────────────────────────────
DOMAIN_TTL: dict[str, int] = {
    "conflict":     6 * 3600,    # 6h  — conflicts evolve quickly
    "cyber":        4 * 3600,    # 4h  — cyber threats have a short window
    "economic":     12 * 3600,   # 12h — macro data lasts longer
    "environment":  6 * 3600,    # 6h  — natural disasters
    "sanctions":    48 * 3600,   # 48h — sanctions have prolonged impact
    "social":       2 * 3600,    # 2h  — sentiment changes fast
    "trade":        24 * 3600,   # 24h — trade restrictions last longer
    "supply_chain": 12 * 3600,   # 12h
    "advisories":   12 * 3600,   # 12h
    "feeds":        1 * 3600,    # 1h  — generic feeds
    "market":       4 * 3600,    # 4h  — market events
    "reference":    24 * 3600,   # 24h
}
DEFAULT_TTL = 6 * 3600

# ── Risk score per severity ───────────────────────────────────────────
SEVERITY_SCORE: dict[str, float] = {
    IntelSeverity.HIGH:   0.85,
    IntelSeverity.MEDIUM: 0.55,
    IntelSeverity.LOW:    0.25,
    "":                   0.30,
}

# ── Default bias per domain ───────────────────────────────────────────
# Conflict, cyber, sanctions events tend to be bearish for affected currencies;
# economic and market events are neutral until further analysis.
DOMAIN_BIAS: dict[str, str] = {
    "conflict":     IntelBias.BEARISH,
    "cyber":        IntelBias.BEARISH,
    "sanctions":    IntelBias.BEARISH,
    "supply_chain": IntelBias.BEARISH,
    "environment":  IntelBias.BEARISH,
    "advisories":   IntelBias.BEARISH,
    "trade":        IntelBias.BEARISH,
    "economic":     IntelBias.NEUTRAL,
    "market":       IntelBias.NEUTRAL,
    "social":       IntelBias.NEUTRAL,
    "feeds":        IntelBias.NEUTRAL,
    "reference":    IntelBias.NEUTRAL,
}

# ── Country/region → currency mapping ─────────────────────────────────
# Key: lowercase search string. Value: ISO 4217 currency code.
COUNTRY_CURRENCY_MAP: dict[str, str] = {
    # Americas
    "united states": "USD", "us": "USD", "usa": "USD",
    "canada": "CAD",
    "mexico": "MXN",
    "brazil": "BRL",
    # Europe
    "eurozone": "EUR", "euro area": "EUR",
    "germany": "EUR", "france": "EUR", "italy": "EUR", "spain": "EUR",
    "netherlands": "EUR", "belgium": "EUR", "portugal": "EUR",
    "united kingdom": "GBP", "uk": "GBP", "britain": "GBP", "england": "GBP",
    "switzerland": "CHF",
    "norway": "NOK",
    "sweden": "SEK",
    "denmark": "DKK",
    "poland": "PLN",
    # Asia-Pacific
    "japan": "JPY",
    "china": "CNY", "prc": "CNY",
    "australia": "AUD",
    "new zealand": "NZD",
    "singapore": "SGD",
    "south korea": "KRW",
    "india": "INR",
    # Middle East / Africa
    "russia": "RUB",
    "ukraine": "UAH",
    "iran": "IRR",
    "turkey": "TRY",
    "saudi arabia": "SAR",
}

# Major forex pairs that can receive tags
MAJOR_PAIRS: list[str] = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
    "USDCHF", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
]

# Safe-haven currencies — benefit when geopolitical risk spikes
SAFE_HAVEN_CURRENCIES: frozenset[str] = frozenset({"USD", "JPY", "CHF"})
