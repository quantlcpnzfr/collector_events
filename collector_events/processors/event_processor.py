# services/collector_events/collector_events/processors/event_processor.py
"""
EventProcessor — Stage 2 of the global intelligence pipeline (NLP).

Responsibility:
    Receive ``IntelItem`` (produced by ``globalintel`` extractors) and enrich
    them with:

    1. **Impact classification** — categories: RATE_DECISION, ECONOMIC_DATA,
       GEOPOLITICAL, CONFLICT, CYBER, SANCTION, SUPPLY_CHAIN, SOCIAL, GENERIC.
    2. **danger_score** (0.0 – 1.0) — risk score derived from severity base,
       domain weight, keyword matching, and critical keyword bonuses.
    3. Fields ``extra["impact_category"]`` and ``extra["danger_score"]``
       written to the IntelItem.

Keyword rules and scoring constants are loaded from JSON config files
(``processors/config/keywords.json`` and ``processors/config/scoring.json``),
making it easy to tune without code changes.

Usage::

    processor = EventProcessor()
    enriched: list[IntelItem] = processor.process_items(items)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from forex_shared.domain.intel import CountryRef, IntelItem
from forex_shared.logging.loggable import Loggable

from collector_events.processors.country_resolver import CountryResolver

_CONFIG_DIR = Path(__file__).resolve().parent / "config"


# ── Config loader ───────────────────────────────────────────────────────────

def _load_json(filename: str) -> dict:
    path = _CONFIG_DIR / filename
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_keywords() -> tuple[
    frozenset[str],
    tuple[tuple[frozenset[str], str, float], ...],
    frozenset[str],
]:
    """Load keywords.json and return (exclude, rules, critical)."""
    data = _load_json("keywords.json")
    exclude = frozenset(data["exclude_keywords"])
    rules = tuple(
        (frozenset(r["keywords"]), r["category"], r["score_boost"])
        for r in data["keyword_rules"]
    )
    critical = frozenset(data["critical_keywords"])
    return exclude, rules, critical


def _load_scoring() -> dict:
    """Load scoring.json and return the raw dict."""
    return _load_json("scoring.json")


# ── Module-level data (loaded once at import) ──────────────────────────────

_EXCLUDE_KEYWORDS, _KEYWORD_RULES, _CRITICAL_KEYWORDS = _load_keywords()
_SCORING = _load_scoring()

_SEVERITY_BASE: dict[str, float] = _SCORING["severity_base"]
_DOMAIN_WEIGHT: dict[str, float] = _SCORING["domain_weight"]
_CRITICAL_BONUS_VALUE: float = _SCORING["critical_bonus"]
_MAX_KEYWORD_BOOST: float = _SCORING["max_keyword_boost"]
_KW_DIM_MAX_HITS: int = _SCORING["keyword_diminishing_max_hits"]
_KW_DIM_FACTOR: float = _SCORING["keyword_diminishing_factor"]
_NUMERIC_THRESHOLDS: list[dict] = _SCORING["numeric_thresholds"]

_DIRECTIONAL_PCT = re.compile(_SCORING["directional_pct_regex"], re.IGNORECASE)


# ── Impact categories ───────────────────────────────────────────────────────

class ImpactCategory:
    RATE_DECISION  = "RATE_DECISION"
    ECONOMIC_DATA  = "ECONOMIC_DATA"
    GEOPOLITICAL   = "GEOPOLITICAL"
    CONFLICT       = "CONFLICT"
    CYBER          = "CYBER"
    SANCTION       = "SANCTION"
    SUPPLY_CHAIN   = "SUPPLY_CHAIN"
    SOCIAL         = "SOCIAL"
    NATURAL_EVENT  = "NATURAL_EVENT"
    PIRACY         = "PIRACY"
    ELECTION       = "ELECTION"
    TECH_CRYPTO    = "TECH_CRYPTO"
    GENERIC        = "GENERIC"
    IGNORED        = "IGNORED"


# ── Result dataclass ────────────────────────────────────────────────────────

@dataclass
class ProcessedEvent:
    """Result of NLP processing for a single IntelItem."""
    item: IntelItem
    impact_category: str
    danger_score: float          # 0.0 – 1.0, clamped
    matched_keywords: list[str]  # matched keywords (for audit)


# ── EventProcessor ──────────────────────────────────────────────────────────

class EventProcessor(Loggable):
    """NLP Stage 2 pipeline for IntelItem processing.

    Applies keyword matching to determine impact category and calculate a
    ``danger_score`` for each item.  No external calls — purely local text
    analysis.

    Scoring algorithm::

        base  = severity_base[item.severity]
        score = (base + keyword_boost) * domain_weight + critical_bonus
        score += max(numeric_bonus, text_pct_bonus)
        score = clamp(score, 0.0, 1.0)

    Fields ``extra["impact_category"]`` and ``extra["danger_score"]`` are
    written directly to the item (in-place mutation).
    """

    def __init__(self, min_danger_score: float = 0.0) -> None:
        self._min_danger_score = min_danger_score
        self._country_resolver = CountryResolver()

    # ── Public API ───────────────────────────────────────────────────────

    def process_item(self, item: IntelItem) -> ProcessedEvent:
        """Process a single IntelItem and return an enriched ProcessedEvent.

        Side-effect: writes ``item.extra["impact_category"]`` and
        ``item.extra["danger_score"]`` on the original item.
        """
        text = self._extract_text(item)

        # Anti-noise: skip sports / entertainment / celebrity content
        if self._is_noise(text):
            item.extra["impact_category"] = ImpactCategory.IGNORED
            item.extra["danger_score"] = 0.0
            return ProcessedEvent(
                item=item,
                impact_category=ImpactCategory.IGNORED,
                danger_score=0.0,
                matched_keywords=[],
            )

        # Country enrichment: 
        # New feature by my girl developer, she asked for this to be added, so here we are
        if not item.mentioned_countries:
            item.mentioned_countries = self._country_resolver.resolve_refs(text)
            item.country = item.mentioned_country_codes
            
        # TODO: DELETE THIS: legacy single country code field (deprecated in favor of mentioned_countries list) 
        #if not item.country:
            #item.country = self._country_resolver.resolve(text)

        category, keyword_boost, matched = self._classify(text)
        base_score = _SEVERITY_BASE.get(item.severity, 0.30)
        domain_weight = _DOMAIN_WEIGHT.get(item.domain, 1.0)
        critical_bonus = self._critical_bonus(text)

        raw_score = (base_score + keyword_boost) * domain_weight + critical_bonus
        numeric_bonus = self._numeric_signal_bonus(item)
        text_bonus    = self._text_pct_bonus(text)
        danger_score = round(min(max(raw_score + max(numeric_bonus, text_bonus), 0.0), 1.0), 3)
        item.extra["impact_category"] = category
        item.extra["danger_score"] = danger_score

        return ProcessedEvent(
            item=item,
            impact_category=category,
            danger_score=danger_score,
            matched_keywords=matched,
        )

    def process_items(self, items: Sequence[IntelItem]) -> list[ProcessedEvent]:
        """Process a list of IntelItems and return enriched ProcessedEvents.

        Returns list sorted by danger_score descending.
        """
        results: list[ProcessedEvent] = []
        for item in items:
            try:
                result = self.process_item(item)
                results.append(result)
            except Exception as exc:
                self.log.warning(
                    "EventProcessor: error processing item %s — %s",
                    getattr(item, "id", "?"), exc,
                )
        results.sort(key=lambda r: r.danger_score, reverse=True)
        self.log.debug(
            "EventProcessor: %d items processed, top danger_score=%.3f",
            len(results),
            results[0].danger_score if results else 0.0,
        )
        return results

    # ── Internal helpers ─────────────────────────────────────────────────

    @staticmethod
    def _extract_text(item: IntelItem) -> str:
        """Concatenate text fields for analysis."""
        parts = [item.title, item.body]
        if item.tags:
            parts.append(" ".join(item.tags))
        
        # Country enrichment: 
        # New feature by my girl developer, she asked for this to be added, so here we are    
        for ref in item.mentioned_countries + item.actor_countries + item.target_countries:
            if isinstance(ref, CountryRef):
                parts.append(ref.name)
                parts.append(ref.code)
                if ref.currency:
                    parts.append(ref.currency)
                    
        # TODO: DELETE THIS: legacy single country code field (deprecated in favor of mentioned_countries list) 
        for v in item.extra.values():
            if isinstance(v, str):
                parts.append(v)
                
        return " ".join(parts).lower()

    @staticmethod
    def _is_noise(text: str) -> bool:
        """Return True if text matches sports/entertainment/celebrity noise."""
        for kw in _EXCLUDE_KEYWORDS:
            if kw in text:
                return True
        return False

    @staticmethod
    def _classify(text: str) -> tuple[str, float, list[str]]:
        """Determine the best-matching category and accumulated keyword boost.

        Returns (category, total_boost, matched_keywords).
        """
        best_category = ImpactCategory.GENERIC
        best_boost: float = 0.0
        total_boost: float = 0.0
        all_matched: list[str] = []

        for keywords, category, boost in _KEYWORD_RULES:
            found = [kw for kw in keywords if kw in text]
            if found:
                all_matched.extend(found)
                total_boost += boost * min(len(found), _KW_DIM_MAX_HITS) * _KW_DIM_FACTOR
                if boost > best_boost:
                    best_boost = boost
                    best_category = category

        return best_category, round(min(total_boost, _MAX_KEYWORD_BOOST), 4), all_matched

    @staticmethod
    def _critical_bonus(text: str) -> float:
        """Return bonus if critical keywords are found."""
        for kw in _CRITICAL_KEYWORDS:
            if kw in text:
                return _CRITICAL_BONUS_VALUE
        return 0.0

    @staticmethod
    def _numeric_signal_bonus(item: IntelItem) -> float:
        """Bonus based on numeric fields in item.extra (market-domain items)."""
        extra = item.extra
        if not extra:
            return 0.0

        change_pct: float | None = None
        for key in ("change_pct", "pct_change"):
            if key in extra and isinstance(extra[key], (int, float)):
                change_pct = float(extra[key])
                break
        if change_pct is None and "change" in extra:
            val = extra["change"]
            if isinstance(val, (int, float)) and abs(val) <= 100:
                change_pct = float(val)

        if change_pct is None:
            return 0.0

        abs_change = abs(change_pct)
        for threshold in _NUMERIC_THRESHOLDS:
            if abs_change >= threshold["min_pct"]:
                return threshold["bonus"]
        return 0.0

    @staticmethod
    def _text_pct_bonus(text: str) -> float:
        """Bonus from prose percentage mentions (e.g. 'EUR/USD drops 5%')."""
        best = max(
            (float(m.group(1)) for m in _DIRECTIONAL_PCT.finditer(text)),
            default=0.0,
        )
        for threshold in _NUMERIC_THRESHOLDS:
            if best >= threshold["min_pct"]:
                return threshold["bonus"]
        return 0.0
