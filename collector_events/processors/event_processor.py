# services/collector_events/collector_events/processors/event_processor.py
"""
EventProcessor — Estágio 2 do pipeline de inteligência global (NLP).

Responsabilidade:
    Receber ``IntelItem`` (produzidos pelos extractors do ``globalintel``)
    e enriquecê-los com:

    1. **Classificação de impacto** — categorias: RATE_DECISION, ECONOMIC_DATA,
       GEOPOLITICAL, CONFLICT, CYBER, SANCTION, SUPPLY_CHAIN, SOCIAL, GENERIC.
    2. **danger_score** (0.0 – 1.0) — pontuação de risco derivada de:
       - Severidade base (HIGH=0.7, MEDIUM=0.45, LOW=0.2)
       - Pesos por domínio (conflict/sanctions = urgentes)
       - Detecção de palavras-chave de alto impacto (war, sanction, default, …)
       - Boost por keywords críticos (nuclear, embargo, collapse, …)
    3. **campo ``extra["impact_category"]``** e
       **``extra["danger_score"]``** gravados no IntelItem.

Uso::

    processor = EventProcessor()
    enriched: list[IntelItem] = processor.process_items(items)

O ``IntelOrchestrator`` pode chamar ``EventProcessor`` após cada extração.
Nenhuma dependência externa — usa apenas keyword matching (sem modelo ML
nem chamada de API externa neste estágio).
"""

from __future__ import annotations

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
    GENERIC        = "GENERIC"


# ── Tabelas de keyword matching ──────────────────────────────────────────────

# keywords → (category, score_boost)
_KEYWORD_RULES: list[tuple[set[str], str, float]] = [
    # Monetary / Rate decisions — alto impacto imediato
    (
        {"interest rate", "rate hike", "rate cut", "fed rate", "ecb rate",
         "boe rate", "boj rate", "fomc", "monetary policy", "basis points",
         "hawkish", "dovish", "quantitative easing", "qe", "tapering"},
        ImpactCategory.RATE_DECISION,
        0.25,
    ),
    # Economic data releases
    (
        {"gdp", "inflation", "cpi", "ppi", "nfp", "non-farm payroll",
         "unemployment", "trade balance", "current account", "retail sales",
         "manufacturing pmi", "services pmi", "ism", "housing starts",
         "consumer confidence", "oecd", "imf forecast"},
        ImpactCategory.ECONOMIC_DATA,
        0.15,
    ),
    # Armed conflict and wars
    (
        {"war", "armed conflict", "military", "airstrikes", "invasion",
         "missile", "troops", "ceasefire", "offensive", "nato", "drone attack",
         "artillery", "escalation", "troops movement"},
        ImpactCategory.CONFLICT,
        0.30,
    ),
    # Geopolitical tension (non-kinetic)
    (
        {"geopolitical", "diplomatic crisis", "summit", "trade war",
         "tariff", "retaliation", "expulsion", "ambassador", "g7", "g20",
         "un resolution", "veto", "bilateral", "multilateral"},
        ImpactCategory.GEOPOLITICAL,
        0.15,
    ),
    # Sanctions
    (
        {"sanction", "sanctions", "embargo", "asset freeze", "blacklist",
         "ofac", "blocked", "export restriction", "import ban"},
        ImpactCategory.SANCTION,
        0.25,
    ),
    # Cyber threats
    (
        {"cyberattack", "ransomware", "ddos", "data breach", "malware",
         "exploit", "vulnerability", "critical infrastructure", "hacked",
         "zero-day", "apt group"},
        ImpactCategory.CYBER,
        0.20,
    ),
    # Supply chain
    (
        {"supply chain", "port congestion", "shipping lanes", "suez",
         "panama canal", "strait of hormuz", "bdi", "freight rates",
         "container shortage", "blank sailing", "logistics"},
        ImpactCategory.SUPPLY_CHAIN,
        0.15,
    ),
    # Social unrest
    (
        {"protest", "riot", "strike", "unrest", "demonstration", "coup",
         "revolution", "election fraud", "civil war", "insurrection"},
        ImpactCategory.SOCIAL,
        0.10,
    ),
]

# Critical keywords that amplify the danger score further
_CRITICAL_KEYWORDS: frozenset[str] = frozenset({
    "nuclear", "collapse", "default", "emergency", "crash", "crisis",
    "catastrophe", "imminent", "immediate threat", "financial meltdown",
    "bank run", "sovereign default", "hyperinflation", "currency crisis",
    "market crash", "circuit breaker", "halt trading",
})

# Base danger score by severity level
_SEVERITY_BASE: dict[str, float] = {
    IntelSeverity.HIGH:   0.70,
    IntelSeverity.MEDIUM: 0.45,
    IntelSeverity.LOW:    0.20,
    "":                   0.30,
}

# Domain weight multipliers
_DOMAIN_WEIGHT: dict[str, float] = {
    IntelDomain.CONFLICT:     1.30,
    IntelDomain.SANCTIONS:    1.25,
    IntelDomain.CYBER:        1.15,
    IntelDomain.ECONOMIC:     1.10,
    IntelDomain.SUPPLY_CHAIN: 1.10,
    IntelDomain.ENVIRONMENT:  1.05,
    IntelDomain.TRADE:        1.10,
    IntelDomain.SOCIAL:       0.90,
    IntelDomain.FEEDS:        0.80,
    IntelDomain.MARKET:       1.00,
    IntelDomain.REFERENCE:    0.70,
    IntelDomain.ADVISORIES:   1.05,
}


# ── Result dataclass ─────────────────────────────────────────────────────────

@dataclass
class ProcessedEvent:
    """Resultado do processamento NLP de um único IntelItem."""
    item: IntelItem
    impact_category: str
    danger_score: float          # 0.0 – 1.0, clampado
    matched_keywords: list[str]  # keywords encontrados (para auditoria)


# ── EventProcessor ───────────────────────────────────────────────────────────

class EventProcessor(Loggable):
    """Pipeline NLP Estágio 2 para processamento de IntelItems.

    Aplica keyword matching para determinar categoria de impacto e
    calcular um ``danger_score`` para cada item. Não realiza chamadas
    externas — processamento puramente local via análise de texto.

    Algoritmo de scoring:
        base  = _SEVERITY_BASE[item.severity]
        score = base + keyword_boost(title+body)
        score = score * _DOMAIN_WEIGHT[item.domain]
        score += critical_keyword_bonus
        score = clamp(score, 0.0, 1.0)

    Os campos ``extra["impact_category"]`` e ``extra["danger_score"]``
    são escritos diretamente no item (mutação in-place).
    """

    def __init__(self, min_danger_score: float = 0.0) -> None:
        """
        Args:
            min_danger_score: Items com danger_score abaixo deste limiar
                são retornados mas sinalizados como GENERIC de baixo risco.
        """
        self._min_danger_score = min_danger_score

    # ── Public API ───────────────────────────────────────────────────────

    def process_item(self, item: IntelItem) -> ProcessedEvent:
        """Processa um único IntelItem e retorna um ProcessedEvent enriquecido.

        Side-effect: escreve ``item.extra["impact_category"]`` e
        ``item.extra["danger_score"]`` no item original.
        """
        text = self._extract_text(item)
        category, keyword_boost, matched = self._classify(text)
        base_score = _SEVERITY_BASE.get(item.severity, 0.30)
        domain_weight = _DOMAIN_WEIGHT.get(item.domain, 1.0)
        critical_bonus = self._critical_bonus(text)

        raw_score = (base_score + keyword_boost) * domain_weight + critical_bonus
        danger_score = round(min(max(raw_score, 0.0), 1.0), 3)

        # Enrich item in-place
        item.extra["impact_category"] = category
        item.extra["danger_score"] = danger_score

        return ProcessedEvent(
            item=item,
            impact_category=category,
            danger_score=danger_score,
            matched_keywords=matched,
        )

    def process_items(self, items: Sequence[IntelItem]) -> list[ProcessedEvent]:
        """Processa uma lista de IntelItems e retorna ProcessedEvents enriquecidos.

        Args:
            items: Itens produzidos por qualquer extractor do globalintel.

        Returns:
            Lista de ProcessedEvent, ordenada por danger_score descrescente.
        """
        results: list[ProcessedEvent] = []
        for item in items:
            try:
                result = self.process_item(item)
                results.append(result)
            except Exception as exc:
                self.log.warning(
                    "EventProcessor: erro ao processar item %s — %s",
                    getattr(item, "id", "?"), exc,
                )
        results.sort(key=lambda r: r.danger_score, reverse=True)
        self.log.debug(
            "EventProcessor: %d items processados, top danger_score=%.3f",
            len(results),
            results[0].danger_score if results else 0.0,
        )
        return results

    # ── Internal helpers ─────────────────────────────────────────────────

    @staticmethod
    def _extract_text(item: IntelItem) -> str:
        """Concatena campos textuais para análise."""
        parts = [item.title, item.body]
        if item.tags:
            parts.append(" ".join(item.tags))
        # Also scan keys in extra for textual context
        for v in item.extra.values():
            if isinstance(v, str):
                parts.append(v)
        return " ".join(parts).lower()

    @staticmethod
    def _classify(text: str) -> tuple[str, float, list[str]]:
        """Determina a categoria de maior score e o boost acumulado.

        Retorna (category, total_boost, matched_keywords).
        Regras com maior boost ganham quando há empate.
        """
        best_category = ImpactCategory.GENERIC
        best_boost: float = 0.0
        total_boost: float = 0.0
        all_matched: list[str] = []

        for keywords, category, boost in _KEYWORD_RULES:
            found = [kw for kw in keywords if kw in text]
            if found:
                all_matched.extend(found)
                total_boost += boost * min(len(found), 3) * 0.4  # diminishing returns
                if boost > best_boost:
                    best_boost = boost
                    best_category = category

        return best_category, round(min(total_boost, 0.40), 4), all_matched

    @staticmethod
    def _critical_bonus(text: str) -> float:
        """Retorna bônus de 0.15 se keywords críticos forem encontrados."""
        for kw in _CRITICAL_KEYWORDS:
            if kw in text:
                return 0.15
        return 0.0