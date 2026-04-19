# services/collector_events/collector_events/processors/event_processor.py
"""
EventProcessor — Estágio 2 do pipeline de inteligência global (NLP).

Refatorado com foco em:
- Alta Performance (uso de frozenset para alocação otimizada).
- Redução de Ruído (Anti-Noise) aplicando excludeKeywords do prediction-tags.json.
- Early Return (Curto-circuito) para economizar ciclos de CPU.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

from forex_shared.domain.intel import IntelDomain, IntelItem, IntelSeverity
from forex_shared.logging.loggable import Loggable


# ── Categorias de impacto ────────────────────────────────────────────────────

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
    TECH_CRYPTO    = "TECH_CRYPTO" # Nova categoria extraída do prediction-tags
    GENERIC        = "GENERIC"
    IGNORED        = "IGNORED"     # Nova categoria para ruídos esportivos/entretenimento


# ── Lógica Anti-Ruído (Baseada no prediction-tags.json) ──────────────────────
# Se qualquer destas palavras aparecer, o evento é severamente penalizado ou ignorado.
_EXCLUDE_KEYWORDS: frozenset[str] = frozenset({
    "nba", "nfl", "mlb", "nhl", "fifa", "world cup", "super bowl", "championship",
    "playoffs", "oscar", "grammy", "emmy", "box office", "movie", "album", "song",
    "streamer", "influencer", "celebrity", "kardashian",
    "bachelor", "reality tv", "mvp", "touchdown", "home run", "goal scorer",
    "academy award", "bafta", "golden globe", "cannes", "sundance",
    "documentary", "feature film", "tv series", "season finale"
})

# ── Tabelas de keyword matching (Otimizadas para frozenset/tuplas) ───────────

# keywords → (category, score_boost)
_KEYWORD_RULES: tuple[tuple[frozenset[str], str, float], ...] = (
    # Rate Decisions
    (frozenset({"interest rate", "rate hike", "rate cut", "fed rate", "ecb rate", "boe rate"}),
     ImpactCategory.RATE_DECISION, 0.25),
     
    # Economic Data & Finance (Adicionado "debt-ceiling", "tariffs" do prediction-tags)
    (frozenset({"gdp", "inflation", "cpi", "ppi", "nfp", "recession", "trade war", "tariffs", "debt-ceiling"}),
     ImpactCategory.ECONOMIC_DATA, 0.15),
     
    # Tech & Crypto (Nova categoria vinda do prediction-tags)
    (frozenset({"ai", "tech", "crypto", "elon-musk", "bitcoin", "ethereum", "defi"}),
     ImpactCategory.TECH_CRYPTO, 0.12),

    # Conflict / War
    (frozenset({"war", "airstrikes", "invasion", "missile", "troops", "drone attack"}),
     ImpactCategory.CONFLICT, 0.30),
     
    # Geopolitical (Adicionado "ukraine", "china", "middle-east" do prediction-tags)
    (frozenset({"geopolitics", "elections", "ukraine", "china", "middle-east", "diplomatic"}),
     ImpactCategory.GEOPOLITICAL, 0.15),
     
    # Natural Events (Tempestades, Ciclones - Referência ao JSON do GDACS)
    (frozenset({"cyclone", "hurricane", "earthquake", "tsunami", "wildfire", "flood", "storm"}),
     ImpactCategory.NATURAL_EVENT, 0.15),
     
    # Supply Chain (Referência ao JSON de Critical Minerals)
    (frozenset({"supply chain", "critical mineral", "rare earth", "shipping", "port congestion"}),
     ImpactCategory.SUPPLY_CHAIN, 0.15),
)

_CRITICAL_KEYWORDS: frozenset[str] = frozenset({
    "nuclear", "collapse", "default", "emergency", "crash", "crisis",
    "assassination", "coup", "systemic risk", "contagion", "bailout"
})

_SEVERITY_BASE: dict[str, float] = {
    IntelSeverity.HIGH:   0.70,
    IntelSeverity.MEDIUM: 0.45,
    IntelSeverity.LOW:    0.20,
    "":                   0.30,
}

_DOMAIN_WEIGHT: dict[str, float] = {
    IntelDomain.CONFLICT:     1.30,
    IntelDomain.ECONOMIC:     1.10,
    IntelDomain.SUPPLY_CHAIN: 1.10,
    IntelDomain.ENVIRONMENT:  1.05,
    IntelDomain.MARKET:       1.00,
}

_DIRECTIONAL_PCT = re.compile(
    r'(?:drop|fall|rise|gain|surge|plunge|jump|climb|crash|rally|slide|tumble|soar|spike|dip|down|up)'
    r'\w{0,4}\s+(\d+(?:\.\d+)?)\s*%',
    re.IGNORECASE,
)

@dataclass
class ProcessedEvent:
    item: IntelItem
    impact_category: str
    danger_score: float
    matched_keywords: list[str]

class EventProcessor(Loggable):
    def __init__(self, min_danger_score: float = 0.0) -> None:
        self._min_danger_score = min_danger_score

    def process_item(self, item: IntelItem) -> ProcessedEvent:
        text = self._extract_text(item)
        
        # 1. EARLY RETURN (Curto-Circuito Anti-Ruído)
        # Extremamente rápido em CPU. Checa a lista de exclusão primeiro.
        if self._is_noise(text):
            item.extra["impact_category"] = ImpactCategory.IGNORED
            item.extra["danger_score"] = 0.0
            return ProcessedEvent(item, ImpactCategory.IGNORED, 0.0, ["noise_filtered"])

        # 2. Continua com o processamento normal
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
        results = []
        for item in items:
            try:
                results.append(self.process_item(item))
            except Exception as exc:
                self.log.warning("EventProcessor erro em %s: %s", getattr(item, "id", "?"), exc)
        results.sort(key=lambda r: r.danger_score, reverse=True)
        return results

    @staticmethod
    def _is_noise(text: str) -> bool:
        """Verifica se o texto contém palavras de entretenimento/esportes. Operação O(K)."""
        # Usando set intersection para performance máxima
        words_in_text = set(text.split())
        return bool(_EXCLUDE_KEYWORDS.intersection(words_in_text))

    @staticmethod
    def _extract_text(item: IntelItem) -> str:
        parts = [item.title, item.body]
        if item.tags:
            parts.extend(item.tags)
        for v in item.extra.values():
            if isinstance(v, str):
                parts.append(v)
        return " ".join(parts).lower()

    @staticmethod
    def _classify(text: str) -> tuple[str, float, list[str]]:
        best_category = ImpactCategory.GENERIC
        best_boost: float = 0.0
        total_boost: float = 0.0
        all_matched: list[str] = []

        for keywords, category, boost in _KEYWORD_RULES:
            # Comprehension mais eficiente usando intersecção
            found = [kw for kw in keywords if kw in text]
            if found:
                all_matched.extend(found)
                total_boost += boost * min(len(found), 3) * 0.4
                if boost > best_boost:
                    best_boost = boost
                    best_category = category

        return best_category, round(min(total_boost, 0.40), 4), all_matched

    @staticmethod
    def _critical_bonus(text: str) -> float:
        for kw in _CRITICAL_KEYWORDS:
            if kw in text: return 0.15
        return 0.0

    @staticmethod
    def _numeric_signal_bonus(item: IntelItem) -> float:
        extra = item.extra
        if not extra: return 0.0

        change_pct = extra.get("change_pct") or extra.get("pct_change")
        if change_pct is None and "change" in extra:
            val = extra["change"]
            if isinstance(val, (int, float)) and abs(val) <= 100:
                change_pct = float(val)

        if change_pct is None: return 0.0
        
        abs_change = abs(float(change_pct))
        if abs_change >= 5.0: return 0.15
        if abs_change >= 3.0: return 0.10
        if abs_change >= 1.0: return 0.05
        return 0.0

    @staticmethod
    def _text_pct_bonus(text: str) -> float:
        best = max((float(m.group(1)) for m in _DIRECTIONAL_PCT.finditer(text)), default=0.0)
        if best >= 5.0: return 0.15
        if best >= 3.0: return 0.10
        if best >= 1.0: return 0.05
        return 0.0