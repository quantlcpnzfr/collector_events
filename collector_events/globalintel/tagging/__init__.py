# globalintel/tagging/__init__.py
"""
Tagging sub-package — converts IntelItems into forex-correlated GlobalTags.

Components:
    GlobalTagManager  — orchestrator (severity filter → resolve → tag → Redis + MQ)
    AssetResolver     — country/currency → forex pair resolution
    LLMEnricher       — optional AI Trend Oracle refinement (DeepSeek / Claude)
    reference_data    — static dicts (TTLs, bias defaults, country→currency map)
"""

from .asset_resolver import AssetResolver
from .llm_enricher import LLMEnricher
from .reference_data import (
    COUNTRY_CURRENCY_MAP,
    DEFAULT_TTL,
    DOMAIN_BIAS,
    DOMAIN_TTL,
    MAJOR_PAIRS,
    SAFE_HAVEN_CURRENCIES,
    SEVERITY_SCORE,
)
from .tag_manager import GlobalTagManager

__all__ = [
    "AssetResolver",
    "COUNTRY_CURRENCY_MAP",
    "DEFAULT_TTL",
    "DOMAIN_BIAS",
    "DOMAIN_TTL",
    "GlobalTagManager",
    "LLMEnricher",
    "MAJOR_PAIRS",
    "SAFE_HAVEN_CURRENCIES",
    "SEVERITY_SCORE",
]
