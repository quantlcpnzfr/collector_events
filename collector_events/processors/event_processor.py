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
    GENERIC        = "GENERIC"


# ── Tabelas de keyword matching ──────────────────────────────────────────────

# keywords → (category, score_boost)
_KEYWORD_RULES: list[tuple[set[str], str, float]] = [
    # Monetary / Rate decisions — alto impacto imediato
    (
        {"interest rate", "rate hike", "rate cut", "fed rate", "ecb rate",
         "boe rate", "boj rate", "fomc", "monetary policy", "basis points",
         "hawkish", "dovish", "quantitative easing", "qe", "tapering",
         "rate decision", "central bank", "balance sheet", "forward guidance",
         "yield curve control", "negative rates", "rate hold", "pause hike",
         "inflation target", "policy rate", "overnight rate", "repo rate",
         "reserve requirement", "open market operations"},
        ImpactCategory.RATE_DECISION,
        0.25,
    ),
    # Economic data releases
    (
        {"gdp", "inflation", "cpi", "ppi", "nfp", "non-farm payroll",
         "unemployment", "trade balance", "current account", "retail sales",
         "manufacturing pmi", "services pmi", "ism", "housing starts",
         "consumer confidence", "oecd", "imf forecast",
         "jobless claims", "payrolls", "wage growth", "labor market",
         "industrial production", "factory orders", "durable goods",
         "business confidence", "economic outlook", "recession",
         "stagflation", "gdp contraction", "gdp growth", "economic slowdown",
         "debt ceiling", "fiscal deficit", "budget deficit", "austerity",
         "stimulus package", "fiscal policy", "government spending",
         "trade deficit", "balance of payments", "foreign reserves",
         "capital flows", "credit rating", "sovereign debt", "bond yields",
         "yield spread", "inverted yield curve", "treasury yields"},
        ImpactCategory.ECONOMIC_DATA,
        0.15,
    ),
    # Armed conflict and wars
    (
        {"war", "armed conflict", "military", "airstrikes", "invasion",
         "missile", "troops", "ceasefire", "offensive", "nato", "drone attack",
         "artillery", "escalation", "troops movement",
         "bombing", "casualties", "drone strike", "airstrike", "assassination",
         "coup", "martial law", "troops deployed", "peace treaty", "hostage",
         "evacuation order", "insurgency", "rebel", "militia", "shelling",
         "ground offensive", "air campaign", "naval blockade", "siege",
         "military operation", "special operation", "combat", "frontline",
         "occupied territory", "annexation", "separatist", "paramilitary",
         "war crimes", "civilian casualties", "refugee crisis", "displaced",
         "active conflict", "ceasefire violation", "peace negotiations",
         "demilitarized zone", "no-fly zone", "arms shipment", "weapons supply",
         "military aid", "defense pact", "mutual defense", "article 5",
         "chemical weapons", "biological weapons", "cluster munitions",
         "ballistic missile", "hypersonic", "intercontinental", "warhead",
         "nuclear submarine", "aircraft carrier", "military mobilization"},
        ImpactCategory.CONFLICT,
        0.30,
    ),
    # Geopolitical tension (non-kinetic)
    (
        {"geopolitical", "diplomatic crisis", "summit", "trade war",
         "tariff", "retaliation", "expulsion", "ambassador", "g7", "g20",
         "un resolution", "veto", "bilateral", "multilateral",
         "coup d'état", "diplomatic incident", "expel diplomat",
         "economic pressure", "foreign policy", "regime change",
         "political crisis", "state department", "foreign minister",
         "foreign affairs", "international relations", "geopolitical risk",
         "power struggle", "political instability", "failed state",
         "governance crisis", "authoritarian", "democratic backsliding",
         "national security", "strategic competition", "great power",
         "sphere of influence", "proxy war", "regime stability",
         "state collapse", "political transition", "junta",
         "international tribunal", "war crimes tribunal", "icj", "icc"},
        ImpactCategory.GEOPOLITICAL,
        0.15,
    ),
    # Sanctions
    (
        {"sanction", "sanctions", "embargo", "asset freeze", "blacklist",
         "ofac", "blocked", "export restriction", "import ban",
         "trade restriction", "economic sanction", "financial sanction",
         "targeted sanction", "sectoral sanction", "secondary sanction",
         "travel ban", "visa restriction", "frozen assets", "seized assets",
         "swift ban", "correspondent banking", "de-risking",
         "export control", "technology ban", "chip ban", "semiconductor ban",
         "entity list", "denied party", "debarment", "delisting",
         "oil embargo", "arms embargo", "financial blockade"},
        ImpactCategory.SANCTION,
        0.25,
    ),
    # Cyber threats
    (
        {"cyberattack", "ransomware", "ddos", "data breach", "malware",
         "exploit", "vulnerability", "critical infrastructure", "hacked",
         "zero-day", "apt group",
         "cyber attack", "internet disruption", "gps jamming",
         "phishing", "spyware", "backdoor", "rootkit", "botnet",
         "network intrusion", "state-sponsored hacking", "cyber espionage",
         "supply chain attack", "software vulnerability", "patch",
         "power grid attack", "water system attack", "hospital attack",
         "election interference", "disinformation campaign",
         "information warfare", "hybrid warfare", "deepfake",
         "social engineering", "credential theft", "identity theft"},
        ImpactCategory.CYBER,
        0.20,
    ),
    # Supply chain
    (
        {"supply chain", "port congestion", "shipping lanes", "suez",
         "panama canal", "strait of hormuz", "bdi", "freight rates",
         "container shortage", "blank sailing", "logistics",
         "chokepoint", "bab el-mandeb", "strait of malacca",
         "pipeline disruption", "energy supply", "gas supply",
         "oil supply disruption", "refinery shutdown", "terminal closure",
         "port strike", "dock workers", "longshoremen",
         "shipping disruption", "cargo delay", "supply shortage",
         "chip shortage", "semiconductor supply", "critical minerals",
         "rare earth", "lithium supply", "cobalt supply",
         "food supply", "grain export", "wheat export", "corn export",
         "fertilizer shortage", "agriculture disruption",
         "energy crisis", "gas crisis", "power outage", "blackout",
         "infrastructure damage", "pipeline explosion", "cable cut"},
        ImpactCategory.SUPPLY_CHAIN,
        0.15,
    ),
    # Social unrest
    (
        {"protest", "riot", "strike", "unrest", "demonstration",
         "revolution", "civil war", "insurrection",
         "general strike", "mass protest", "political violence",
         "social unrest", "civil unrest", "popular uprising",
         "anti-government", "crackdown", "police brutality",
         "human rights", "political prisoner", "opposition leader",
         "dissident", "censorship", "press freedom", "internet shutdown",
         "surveillance", "authoritarian crackdown", "jailed opposition",
         "political repression", "forced disappearance"},
        ImpactCategory.SOCIAL,
        0.10,
    ),
    # Natural events and disasters
    (
        {"earthquake", "wildfire", "storm", "flood", "tsunami",
         "hurricane", "typhoon", "volcano", "drought", "tropical system",
         "climate anomaly", "extreme weather",
         "volcanic eruption", "seismic activity", "aftershock",
         "landslide", "mudslide", "avalanche", "blizzard", "ice storm",
         "heatwave", "cold snap", "flash flood", "river flooding",
         "storm surge", "cyclone", "tornado", "superstorm",
         "forest fire", "bushfire", "drought emergency", "famine",
         "crop failure", "water shortage", "el niño", "la niña",
         "climate disaster", "natural disaster", "state of emergency",
         "disaster declaration", "evacuation", "displaced population",
         "humanitarian crisis", "aid effort", "rescue operation"},
        ImpactCategory.NATURAL_EVENT,
        0.15,
    ),
    # Maritime piracy and vessel threats
    (
        {"piracy", "hijack", "vessel seized", "maritime threat", "naval blockade",
         "ship attacked", "tanker seized", "cargo ship hijacked",
         "pirate attack", "armed boarding", "crew held hostage",
         "vessel detained", "maritime incident", "sea lanes threat",
         "red sea attack", "houthi attack", "gulf of aden",
         "somali pirates", "strait attack", "naval incident"},
        ImpactCategory.PIRACY,
        0.20,
    ),
    # Elections and political transitions
    (
        {"election", "referendum", "ballot", "polling", "president-elect",
         "electoral fraud", "recount", "snap election", "early election",
         "election results", "vote count", "exit poll", "election day",
         "voter turnout", "contested election", "disputed election",
         "election interference", "electoral college", "runoff",
         "primary election", "midterm election", "general election",
         "parliamentary election", "presidential election",
         "political transition", "power transfer", "inauguration",
         "election monitoring", "international observers"},
        ImpactCategory.ELECTION,
        0.10,
    ),
    # Forex pairs and FX market moves
    (
        {"eur/usd", "eurusd", "gbp/usd", "gbpusd", "usd/jpy", "usdjpy",
         "usd/chf", "usdchf", "aud/usd", "audusd", "nzd/usd", "nzdusd",
         "usd/cad", "usdcad", "eur/gbp", "eurgbp", "eur/jpy", "eurjpy",
         "gbp/jpy", "gbpjpy", "dollar index", "dxy", "usdx",
         "currency pair", "exchange rate", "spot rate",
         "dollar strength", "dollar weakness", "dollar rally", "dollar selloff",
         "forex market", "fx market", "currency move", "fx volatility",
         "currency crisis", "devaluation", "peg broken", "managed float"},
        ImpactCategory.ECONOMIC_DATA,
        0.12,
    ),
    # Crypto / digital assets — market sentiment and risk appetite signal
    (
        {"bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
         "digital asset", "stablecoin", "defi", "altcoin", "token",
         "etf", "spot etf", "btc etf", "crypto etf", "grayscale", "ibit",
         "bitb", "fbtc", "arkb", "btco", "brrr", "hodl",
         "on-chain", "hash rate", "mining", "halving", "blockchain",
         "crypto market", "crypto sell-off", "crypto rally",
         "exchange outflow", "exchange inflow", "whale", "liquidation",
         "funding rate", "open interest", "perpetual", "futures basis",
         "tether", "usdc", "usdt", "stablecoin depeg", "binance", "coinbase",
         "kraken", "okx", "bybit", "deribit", "cme bitcoin"},
        ImpactCategory.ECONOMIC_DATA,
        0.08,
    ),
]

# Critical keywords that amplify the danger score further
_CRITICAL_KEYWORDS: frozenset[str] = frozenset({
    "nuclear", "collapse", "default", "emergency", "crash", "crisis",
    "catastrophe", "imminent", "immediate threat", "financial meltdown",
    "bank run", "sovereign default", "hyperinflation", "currency crisis",
    "market crash", "circuit breaker", "halt trading",
    # Conflict escalation
    "assassination", "coup", "martial law", "troops deployed",
    "evacuation order", "terror attack", "terrorist", "hostage",
    "drone strike", "airstrike", "active conflict", "nuclear threat",
    "chemical attack", "biological attack", "nuclear launch",
    "nuclear detonation", "dirty bomb", "radiological",
    # Financial systemic risk
    "systemic risk", "contagion", "bailout", "nationalization",
    "capital controls", "currency peg broken", "devaluation",
    "debt default", "payment default", "credit freeze",
    # Infrastructure catastrophe
    "grid collapse", "infrastructure collapse", "dam failure",
    "nuclear meltdown", "reactor leak", "radiation leak",
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
    IntelDomain.ADVISORIES:   1.10,
}


# Regex: directional word followed by a percentage value (e.g. "drops 5%", "surges 2.3%")
_DIRECTIONAL_PCT = re.compile(
    r'(?:drop|fall|rise|gain|surge|plunge|jump|climb|crash|rally|slide|tumble|soar|spike|dip|down|up)'
    r'\w{0,4}\s+(\d+(?:\.\d+)?)\s*%',
    re.IGNORECASE,
)


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

    @staticmethod
    def _numeric_signal_bonus(item: IntelItem) -> float:
        """Bonus based on numeric fields in item.extra (market-domain items).

        Applied on top of keyword score. Designed for price/flow extractors
        (btc_etf_flows, commodity_quotes, yahoo_fx, etc.) that embed numeric
        signals in ``extra`` rather than prose text.

        Scale:
            |change_pct| >= 5%  → +0.15
            |change_pct| >= 3%  → +0.10
            |change_pct| >= 1%  → +0.05
            flow_usd / volume signals may be added per extractor convention.
        """
        extra = item.extra
        if not extra:
            return 0.0

        # Normalise change: "change_pct", "change", "pct_change" (float, may be 0)
        change_pct: float | None = None
        for key in ("change_pct", "pct_change"):
            if key in extra and isinstance(extra[key], (int, float)):
                change_pct = float(extra[key])
                break
        # Some extractors store raw price change (not %) under "change"
        # Only use it if it looks like a percentage (abs <= 100)
        if change_pct is None and "change" in extra:
            val = extra["change"]
            if isinstance(val, (int, float)) and abs(val) <= 100:
                change_pct = float(val)

        if change_pct is None:
            return 0.0

        abs_change = abs(change_pct)
        if abs_change >= 5.0:
            return 0.15
        if abs_change >= 3.0:
            return 0.10
        if abs_change >= 1.0:
            return 0.05
        return 0.0

    @staticmethod
    def _text_pct_bonus(text: str) -> float:
        """Bonus extracted from prose percentage mentions (e.g. 'EUR/USD drops 5%').

        Complements _numeric_signal_bonus for news items where the move is
        expressed in text rather than structured extra fields.
        Takes the largest single match to avoid double-counting.

        Scale:
            >= 5%  → +0.15
            >= 3%  → +0.10
            >= 1%  → +0.05
        """
        best = max(
            (float(m.group(1)) for m in _DIRECTIONAL_PCT.finditer(text)),
            default=0.0,
        )
        if best >= 5.0:
            return 0.15
        if best >= 3.0:
            return 0.10
        if best >= 1.0:
            return 0.05
        return 0.0