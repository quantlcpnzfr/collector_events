# services/collector_events/collector_events/processors/event_processor.py
"""
EventProcessor — Stage 2 da pipeline de inteligência global.

Versão balanced hard-quant:
- Usa LocalNLPEngine para NLP/NER/GLiNER.
- Usa scoring.json como matriz real de calibração.
- Corrige categorias óbvias por contexto antes do score.
- Deduplica entidades e grafo GLiNER antes do cálculo.
- Aplica filtros de incidente local / referência histórica / micro battlefield.
- Evita inflação por clamp seco usando soft-cap compression.
- Injeta score_breakdown completo em item.extra.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from forex_shared.domain.intel import IntelItem
from forex_shared.logging.loggable import Loggable

from collector_events.processors.country_resolver import CountryResolver
from collector_events.nlp.nlp_engine import LocalNLPEngine

_CONFIG_DIR = Path(__file__).parent / "config"
_SCORING_FILE = _CONFIG_DIR / "scoring.json"


@dataclass
class ProcessedEvent:
    impact_category: str
    danger_score: float
    matched_keywords: List[str]
    domain_weight: float = 1.0
    features: Dict[str, Any] = field(default_factory=dict)
    score_breakdown: Dict[str, Any] = field(default_factory=dict)
    raw_danger_score: float = 0.0
    risk_bucket: str = "low"
    saturation: bool = False


class EventProcessor(Loggable):
    """Processa IntelItem e calcula um score de risco calibrado e auditável."""

    DEFAULT_KEYWORD_WEIGHTS: dict[str, float] = {
        # Geopolítica / militar
        "attack": 0.055,
        "attacks": 0.055,
        "strike": 0.070,
        "strikes": 0.070,
        "missile": 0.090,
        "missiles": 0.090,
        "drone": 0.060,
        "drones": 0.060,
        "explosion": 0.060,
        "explosions": 0.060,
        "invasion": 0.110,
        "war": 0.090,
        "troops": 0.055,
        "mobilization": 0.075,
        "blockade": 0.100,
        "airspace closure": 0.080,
        "naval blockade": 0.110,
        "terrorist": 0.100,
        "mass casualty": 0.110,
        "nuclear": 0.150,
        "radioactive": 0.135,
        "radiation": 0.115,
        # Sanctions / supply chain
        "sanction": 0.070,
        "sanctions": 0.070,
        "embargo": 0.080,
        "trade embargo": 0.090,
        "export ban": 0.070,
        "supply chain": 0.060,
        "port strike": 0.070,
        "shipping route": 0.065,
        "chokepoint": 0.080,
        "strait of hormuz": 0.120,
        "red sea": 0.070,
        "suez": 0.070,
        "pipeline": 0.070,
        "refinery": 0.080,
        "refineries": 0.080,
        "power grid": 0.100,
        # Macro / mercado
        "fed": 0.065,
        "federal reserve": 0.080,
        "ecb": 0.065,
        "central bank": 0.070,
        "interest rate": 0.070,
        "rate hike": 0.080,
        "rate cut": 0.080,
        "inflation": 0.075,
        "cpi": 0.075,
        "nfp": 0.075,
        "nonfarm payroll": 0.075,
        "recession": 0.095,
        "oil": 0.065,
        "oil price": 0.085,
        "oil prices": 0.085,
        "brent": 0.065,
        "wti": 0.065,
        "gold": 0.060,
        "xau": 0.060,
        "sell-off": 0.085,
        "crash": 0.100,
        "flash crash": 0.120,
        "plunge": 0.085,
        "surge": 0.065,
        # Cyber
        "cyber attack": 0.090,
        "ransomware": 0.085,
        "data breach": 0.065,
        "infrastructure hack": 0.090,
    }

    HIGH_RISK_CATEGORIES = {
        "military drone or missile strike",
        "military attack or action",
        "declaration of war or armed conflict",
        "nuclear threat or radioactive incident",
        "state-sponsored cyber attack or infrastructure hack",
        "stock market crash or massive sell-off",
        "terrorist attack or mass casualty event",
        "unprecedented global crisis or major disruptive anomaly",
        "sudden market shock or black swan event",
    }

    NOISE_CATEGORIES = {
        "generic news or daily politics",
        "sports, entertainment or celebrity gossip",
    }

    STRATEGIC_INFRA_TERMS = [
        "oil refinery", "refinery", "refineries", "pipeline", "power grid", "grid",
        "port", "shipping route", "strait", "hormuz", "suez", "red sea",
        "airport", "airbase", "naval base", "semiconductor", "data center", "telecom",
        "railway", "bridge", "canal", "chokepoint", "oil terminal", "gas terminal",
    ]

    LOCAL_NOISE_TERMS = [
        "falling tree", "falling trees", "trees on cars", "tree fell", "strong winds",
        "local storm", "storm related", "car accident", "traffic accident", "captured on",
        "several trees", "farmer's blocks", "cars in", "local accident", "personal tragedy",
    ]

    HISTORICAL_REFERENCE_TERMS = [
        "years since", "anniversary", "commemorat", "memorial", "remembering",
        "on this day", "since the", "40 years since", "30 years since", "chernobyl nuclear accident",
        "years ago", "decades since", "decades ago", "in memory of",
        "looking back", "historic event", "marks the anniversary",
        "this day in history", "nuclear disaster of",
    ]

    SELF_PROMOTION_TERMS = [
        "subscriber", "subscribers", "youtube", "countdown",
        "channel milestone", "thank you subscribers", "follow us",
        "like and subscribe", "join our channel", "100k", "1m followers",
    ]

    DEESCALATION_TERMS = [
        "ceasefire", "peace", "agreement", "treaty", "friendly",
        "support", "negotiate", "negotiation", "diplomatic",
        "anniversary", "memorial", "commemorat", "years since", "years ago",
    ]

    GLINER_BLACKLIST: dict[str, set[str]] = {
        "central bank": {"monobank", "monobank.ua", "mono bank"},
        "fiat currency": {"rodensky", "sinkovka", "beroazov"},
        "strategic infrastructure": {"farmer's blocks", "farmers blocks"},
        "institutional actor": {"deepstateua", "deepstate"},
    }

    BATTLEFIELD_MICRO_TERMS = [
        "map updated", "enemy has advanced", "enemy advanced", "near sinkovka",
        "frontline", "front line", "brigade", "destroys the enemy", "enemy force",
        "settlement", "village", "positions near", "advanced near",
    ]

    def __init__(self):
        super().__init__()
        self.country_resolver = CountryResolver()

        self.severity_base: dict[str, float] = {}
        self.category_weight: dict[str, float] = {}
        self.domain_weights: dict[str, float] = {}
        self.domain_aliases: dict[str, str] = {}

        self.critical_bonus: float = 0.08
        self.directional_pct_regex: str | None = None
        self.max_keyword_boost: float = 0.22
        self.keyword_diminishing_max_hits: int = 3
        self.keyword_diminishing_factor: float = 0.35
        self.keyword_weights: dict[str, float] = dict(self.DEFAULT_KEYWORD_WEIGHTS)
        self.localized_noise_multiplier: float = 0.25
        self.numeric_thresholds: list[dict[str, float]] = []

        self.engagement_max_bonus: float = 0.07
        self.engagement_views_threshold: int = 15_000
        self.engagement_forwards_threshold: int = 75

        self.domain_weight_softening: float = 0.45
        self.soft_cap_start: float = 0.80
        self.soft_cap_max: float = 0.98
        self.soft_cap_scale: float = 0.55
        self.local_incident_score_cap: float = 0.55
        self.historical_reference_score_cap: float = 0.45
        self.battlefield_micro_score_cap: float = 0.78

        self._load_scoring_config()
        self.nlp = LocalNLPEngine.get_instance()

    def _load_scoring_config(self) -> None:
        try:
            with open(_SCORING_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)

            self.severity_base = config.get(
                "severity_base",
                {"HIGH": 0.62, "MEDIUM": 0.40, "LOW": 0.18, "": 0.24},
            )
            self.category_weight = config.get("category_weight", {})
            self.domain_weights = config.get("domain_weight", {})
            self.domain_aliases = config.get("domain_aliases", {})

            self.critical_bonus = float(config.get("critical_bonus", self.critical_bonus))
            self.directional_pct_regex = config.get("directional_pct_regex")
            self.max_keyword_boost = float(config.get("max_keyword_boost", self.max_keyword_boost))
            self.keyword_diminishing_max_hits = int(config.get("keyword_diminishing_max_hits", self.keyword_diminishing_max_hits))
            self.keyword_diminishing_factor = float(config.get("keyword_diminishing_factor", self.keyword_diminishing_factor))
            self.localized_noise_multiplier = float(config.get("localized_noise_multiplier", self.localized_noise_multiplier))

            json_keyword_weights = config.get("keyword_weights", {})
            self.keyword_weights = {**self.DEFAULT_KEYWORD_WEIGHTS, **json_keyword_weights}

            self.numeric_thresholds = config.get(
                "numeric_thresholds",
                [
                    {"min_pct": 20.0, "bonus": 0.12},
                    {"min_pct": 10.0, "bonus": 0.09},
                    {"min_pct": 5.0, "bonus": 0.06},
                    {"min_pct": 1.0, "bonus": 0.03},
                ],
            )
            self.numeric_thresholds.sort(key=lambda x: float(x.get("min_pct", 0.0)), reverse=True)

            engagement = config.get("engagement_bonus", {})
            self.engagement_max_bonus = float(engagement.get("max_bonus", self.engagement_max_bonus))
            self.engagement_views_threshold = int(engagement.get("views_threshold", self.engagement_views_threshold))
            self.engagement_forwards_threshold = int(engagement.get("forwards_threshold", self.engagement_forwards_threshold))

            compression = config.get("score_compression", {})
            self.domain_weight_softening = float(compression.get("domain_weight_softening", self.domain_weight_softening))
            self.soft_cap_start = float(compression.get("soft_cap_start", self.soft_cap_start))
            self.soft_cap_max = float(compression.get("soft_cap_max", self.soft_cap_max))
            self.soft_cap_scale = float(compression.get("soft_cap_scale", self.soft_cap_scale))

            filters = config.get("context_filters", {})
            self.local_incident_score_cap = float(filters.get("local_incident_score_cap", self.local_incident_score_cap))
            self.historical_reference_score_cap = float(filters.get("historical_reference_score_cap", self.historical_reference_score_cap))
            self.battlefield_micro_score_cap = float(filters.get("battlefield_micro_score_cap", self.battlefield_micro_score_cap))

            self.log.info(
                "Scoring config carregada | domains=%s | categories=%s | keywords=%s",
                len(self.domain_weights), len(self.category_weight), len(self.keyword_weights),
            )
        except Exception as e:
            self.log.warning("Aviso: Não foi possível carregar %s. Usando defaults. Erro: %s", _SCORING_FILE, e)

    def process_items(self, items: Sequence[IntelItem]) -> list[IntelItem]:
        processed_list = []
        for item in items:
            try:
                self.process_item(item)
                processed_list.append(item)
            except Exception as e:
                self.log.error("Erro ao processar item %s: %s", item.id, e)
        return processed_list

    def process_item(self, item: IntelItem) -> ProcessedEvent:
        text = self._build_text(item)
        nlp_features = self._sanitize_nlp_features(self.nlp.extract_features(text))

        if not getattr(item, "country", None):
            item.country = self.country_resolver.resolve(text)

        domain_weight = self._resolve_domain_weight(getattr(item, "domain", ""))
        danger_score, score_breakdown = self._compute_semantic_danger_score(item, nlp_features, domain_weight)
        impact_category = nlp_features.get("inferred_category", "generic")
        matched_keywords = self._extract_matched_keywords(text, nlp_features)

        if item.extra is None:
            item.extra = {}

        item.extra["danger_score"] = danger_score
        item.extra["raw_danger_score"] = score_breakdown["scores"]["raw_score"]
        item.extra["risk_bucket"] = score_breakdown["scores"]["risk_bucket"]
        item.extra["saturation"] = score_breakdown["scores"]["saturation"]
        item.extra["impact_category"] = impact_category
        item.extra["nlp_features"] = nlp_features
        item.extra["domain_weight"] = domain_weight
        item.extra["score_breakdown"] = score_breakdown
        item.extra["matched_keywords"] = matched_keywords

        return ProcessedEvent(
            impact_category=impact_category,
            danger_score=danger_score,
            matched_keywords=matched_keywords,
            domain_weight=domain_weight,
            features=nlp_features,
            score_breakdown=score_breakdown,
            raw_danger_score=score_breakdown["scores"]["raw_score"],
            risk_bucket=score_breakdown["scores"]["risk_bucket"],
            saturation=score_breakdown["scores"]["saturation"],
        )

    def _compute_semantic_danger_score(self, item: IntelItem, nlp_features: Dict[str, Any], domain_weight: float) -> Tuple[float, Dict[str, Any]]:
        text = self._build_text(item)
        text_lower = text.lower()

        original_category = str(nlp_features.get("inferred_category", "generic"))
        original_confidence = self._safe_float(nlp_features.get("category_confidence", 0.0))

        context = self._detect_context_filters(text_lower, nlp_features)
        corrected_category, corrected_confidence, corrections = self._correct_category(
            original_category, original_confidence, text_lower, context
        )
        nlp_features["original_inferred_category"] = original_category
        nlp_features["original_category_confidence"] = original_confidence
        nlp_features["inferred_category"] = corrected_category
        nlp_features["category_confidence"] = corrected_confidence
        nlp_features["category_corrections"] = corrections

        severity_key = self._normalize_severity(getattr(item, "severity", ""))
        severity_base = self._safe_float(self.severity_base.get(severity_key, self.severity_base.get("", 0.24)))
        category_weight = self._safe_float(self.category_weight.get(corrected_category, self._default_category_weight(corrected_category)))

        category_component = 0.42 * corrected_confidence * category_weight
        severity_component = severity_base * min(max(category_weight, 0.05), 1.55)
        base_score = max(category_component, severity_component)

        sentiment_bonus = self._sentiment_modifier(nlp_features=nlp_features, category_weight=category_weight, confidence=corrected_confidence)
        entity_bonus = self._entity_bonus(nlp_features.get("entities", []))
        gliner_bonus, gliner_multiplier, gliner_details = self._gliner_adjustment(
            nlp_features.get("gliner_graph", {}), impact_category=corrected_category, text_lower=text_lower
        )
        numeric_bonus = self._numeric_signal_bonus(item)
        keyword_bonus, keyword_hits = self._keyword_boost(text)
        engagement_bonus = self._engagement_bonus(item, corrected_category)
        critical_bonus = self._critical_bonus(
            impact_category=corrected_category,
            confidence=corrected_confidence,
            gliner_details=gliner_details,
            keyword_hits=keyword_hits,
            context=context,
        )

        additive_score = (
            base_score + sentiment_bonus + entity_bonus + gliner_bonus + numeric_bonus
            + keyword_bonus + engagement_bonus + critical_bonus
        )

        effective_domain_weight = self._soften_domain_weight(domain_weight)
        raw_score = additive_score * effective_domain_weight * gliner_multiplier

        context_multiplier, context_caps = self._context_score_adjustment(context, corrected_category)
        raw_after_context = raw_score * context_multiplier
        compressed_score = self._compress_score(raw_after_context)

        applied_caps = []
        for cap_name, cap_value in context_caps:
            if compressed_score > cap_value:
                compressed_score = cap_value
                applied_caps.append({"name": cap_name, "value": cap_value})

        final_score = self._clamp(compressed_score, 0.0, 0.99)
        risk_bucket = self._risk_bucket(final_score)
        saturation = raw_after_context > 1.0 or final_score >= self.soft_cap_max

        breakdown: Dict[str, Any] = {
            "version": "balanced_hard_quant_v2",
            "inputs": {
                "category": corrected_category,
                "original_category": original_category,
                "confidence": corrected_confidence,
                "original_confidence": original_confidence,
                "sentiment": nlp_features.get("sentiment", "neutral"),
                "severity": getattr(item, "severity", ""),
                "severity_key": severity_key,
                "domain": getattr(item, "domain", ""),
            },
            "category_corrections": corrections,
            "context_filters": context,
            "base": {
                "severity_base": severity_base,
                "category_weight": category_weight,
                "category_component": category_component,
                "severity_component": severity_component,
                "base_score": base_score,
            },
            "additive_modifiers": {
                "sentiment_bonus": sentiment_bonus,
                "entity_bonus": entity_bonus,
                "gliner_bonus": gliner_bonus,
                "numeric_bonus": numeric_bonus,
                "keyword_bonus": keyword_bonus,
                "engagement_bonus": engagement_bonus,
                "critical_bonus": critical_bonus,
            },
            "keyword_hits": keyword_hits,
            "gliner_details": gliner_details,
            "multipliers": {
                "domain_weight": domain_weight,
                "effective_domain_weight": effective_domain_weight,
                "gliner_multiplier": gliner_multiplier,
                "context_multiplier": context_multiplier,
                "total_multiplier": effective_domain_weight * gliner_multiplier * context_multiplier,
            },
            "caps": applied_caps,
            "scores": {
                "additive_score": additive_score,
                "raw_score": raw_score,
                "raw_after_context": raw_after_context,
                "compressed_score": compressed_score,
                "final_score": final_score,
                "risk_bucket": risk_bucket,
                "saturation": saturation,
            },
        }

        return final_score, breakdown

    def _sanitize_nlp_features(self, features: Dict[str, Any]) -> Dict[str, Any]:
        features = dict(features or {})
        features["entities"] = self._dedupe_entities(features.get("entities", []))
        features["gliner_graph"] = self._dedupe_gliner_graph(features.get("gliner_graph", {}))
        return features

    def _dedupe_entities(self, entities: Sequence[Dict[str, Any]]) -> List[Dict[str, str]]:
        seen = set()
        out: List[Dict[str, str]] = []
        for ent in entities or []:
            text = str(ent.get("text", "")).strip()
            label = str(ent.get("label", "")).strip()
            if not text or not label:
                continue
            key = (text.lower(), label.upper())
            if key in seen:
                continue
            seen.add(key)
            out.append({"text": text, "label": label})
        return out

    def _dedupe_gliner_graph(self, graph: Dict[str, list]) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for label, values in (graph or {}).items():
            blacklist = self.GLINER_BLACKLIST.get(label.lower(), set())
            seen = set()
            cleaned = []
            for value in values or []:
                text = str(value).strip()
                key = text.lower()
                if text and key not in seen and key not in blacklist:
                    seen.add(key)
                    cleaned.append(text)
            out[label] = cleaned
        return out

    def _detect_context_filters(self, text_lower: str, nlp_features: Dict[str, Any]) -> Dict[str, Any]:
        historical = self._contains_any(text_lower, self.HISTORICAL_REFERENCE_TERMS)
        local_noise = self._contains_any(text_lower, self.LOCAL_NOISE_TERMS)
        battlefield_micro = self._contains_any(text_lower, self.BATTLEFIELD_MICRO_TERMS)
        self_promotion = self._contains_any(text_lower, self.SELF_PROMOTION_TERMS)
        active_escalation = self._contains_any(text_lower, [
            "today", "breaking", "launches", "launched", "attack", "attacks", "strike", "strikes",
            "missile", "drone", "invasion", "blockade", "threatened", "warned", "declared",
        ])
        
        strong_historical = historical and self._contains_any(text_lower, [
            "years since", "years ago", "decades since", "anniversary",
            "commemorat", "memorial", "in memory",
        ])
        import re
        if not strong_historical and historical:
            if re.search(r'\b(19|20)\d{2}\b', text_lower):
                strong_historical = True

        macro_signal = self._contains_any(text_lower, [
            "central bank", "inflation", "currency", "dollar", "oil", "refinery", "pipeline", "hormuz",
            "suez", "red sea", "sanction", "market", "stock", "recession", "rate hike", "rate cut",
            "free-market price", "lowest value", "highest recorded", "devalu", "depreciat",
        ])
        strategic_signal = self._contains_any(text_lower, self.STRATEGIC_INFRA_TERMS)

        if strong_historical:
            historical_final = True
        else:
            historical_final = historical and not active_escalation

        return {
            "historical_reference": historical_final,
            "local_incident": local_noise and not macro_signal,
            "battlefield_micro": battlefield_micro and not macro_signal,
            "self_promotion": self_promotion and not macro_signal and not active_escalation,
            "active_escalation": active_escalation,
            "macro_signal": macro_signal,
            "strategic_signal": strategic_signal,
        }

    def _correct_category(self, category: str, confidence: float, text_lower: str, context: Dict[str, Any]) -> Tuple[str, float, List[Dict[str, Any]]]:
        corrections: List[Dict[str, Any]] = []
        new_category = category
        new_confidence = confidence

        if context.get("historical_reference"):
            new_category = "generic news or daily politics"
            new_confidence = min(confidence, 0.55)
            corrections.append({"rule": "historical_reference", "from": category, "to": new_category})

        elif context.get("self_promotion"):
            new_category = "sports, entertainment or celebrity gossip"
            new_confidence = min(confidence, 0.15)
            corrections.append({"rule": "self_promotion", "from": category, "to": new_category})

        elif context.get("local_incident"):
            if self._contains_any(text_lower, ["storm", "winds", "trees", "weather"]):
                new_category = "severe extreme weather or hurricane"
            else:
                new_category = "generic news or daily politics"
            new_confidence = min(confidence, 0.55)
            corrections.append({"rule": "local_incident", "from": category, "to": new_category})

        elif context.get("battlefield_micro") and category in {"macroeconomic data release or inflation report", "sudden market shock or black swan event"}:
            new_category = "troop mobilization or border skirmish"
            new_confidence = max(min(confidence, 0.72), 0.55)
            corrections.append({"rule": "battlefield_micro", "from": category, "to": new_category})

        if self._contains_any(text_lower, ["dollar", "national currency", "free-market price", "lowest value", "declined rapidly", "devalu", "depreciat", "more than doubled"]):
            if category in {"macroeconomic data release or inflation report", "generic news or daily politics"} or confidence < 0.75:
                prev = new_category
                new_category = "currency intervention or severe devaluation"
                new_confidence = max(new_confidence, 0.72)
                corrections.append({"rule": "currency_devaluation_signal", "from": prev, "to": new_category})

        return new_category, new_confidence, corrections

    def _sentiment_modifier(self, *, nlp_features: Dict[str, Any], category_weight: float, confidence: float) -> float:
        sentiment = str(nlp_features.get("sentiment", "neutral")).lower()
        category = str(nlp_features.get("inferred_category", ""))
        if sentiment == "negative":
            return min(0.075 * confidence * min(category_weight, 1.45), 0.11)
        if sentiment == "positive" and category not in self.HIGH_RISK_CATEGORIES:
            return -0.035 * confidence
        return 0.0

    def _entity_bonus(self, entities: Sequence[Dict[str, Any]]) -> float:
        labels = {str(e.get("label", "")) for e in entities}
        bonus = 0.0
        if "CENTRAL_BANK" in labels:
            bonus += 0.12
        if "G10_COUNTRY" in labels:
            bonus += 0.05
        if "FOREX_PAIR" in labels:
            bonus += 0.08
        if "COMMODITY" in labels:
            bonus += 0.06
        return min(bonus, 0.20)

    def _gliner_adjustment(self, gliner_graph: Dict[str, list], *, impact_category: str, text_lower: str) -> Tuple[float, float, Dict[str, Any]]:
        if not gliner_graph:
            return 0.0, 1.0, {"strong_tactical_signal": False}

        def count(label: str) -> int:
            return len(gliner_graph.get(label, []) or [])

        macro_target = count("macroeconomic infrastructure or strategic target") > 0
        strategic_raw = count("strategic infrastructure") > 0
        strategic_infra = strategic_raw and self._contains_any(
            text_lower + " " + " ".join(gliner_graph.get("strategic infrastructure", [])).lower(),
            self.STRATEGIC_INFRA_TERMS,
        )
        kinetic = count("kinetic or military action") > 0
        weapon = count("weapon or military vehicle") > 0
        military_actor = count("military actor") > 0
        geopolitical = count("geopolitical event") > 0
        central_bank = count("central bank") > 0
        economic_indicator = count("economic indicator") > 0
        market_sentiment = count("market sentiment") > 0
        commodity_action = count("commodity price action") > 0
        institutional_actor = count("institutional actor") > 0
        political_leader = count("political leader") > 0
        localized_noise = count("civilian vehicle or local infrastructure") > 0 or count("localized accident or personal tragedy") > 0

        strong_tactical_signal = any([macro_target, strategic_infra, kinetic, weapon, military_actor, geopolitical, central_bank, economic_indicator, market_sentiment, commodity_action])

        bonus = 0.0
        if macro_target:
            bonus += 0.14
        if strategic_infra:
            bonus += 0.12
        if kinetic:
            bonus += 0.10
        if weapon:
            bonus += 0.045
        if military_actor:
            bonus += 0.055
        if geopolitical:
            bonus += 0.050
        if central_bank:
            bonus += 0.085
        if economic_indicator:
            bonus += 0.075
        if commodity_action:
            bonus += 0.080
        if market_sentiment:
            bonus += 0.050
        if institutional_actor:
            bonus += 0.030
        if political_leader and strong_tactical_signal:
            bonus += 0.015
        bonus = min(bonus, 0.24)

        multiplier = 1.0
        if macro_target or strategic_infra:
            multiplier *= 1.08
        if localized_noise and not strong_tactical_signal and impact_category not in self.HIGH_RISK_CATEGORIES:
            multiplier *= self.localized_noise_multiplier

        details = {
            "macro_target": macro_target,
            "strategic_infra": strategic_infra,
            "strategic_raw": strategic_raw,
            "kinetic": kinetic,
            "weapon": weapon,
            "military_actor": military_actor,
            "geopolitical": geopolitical,
            "central_bank": central_bank,
            "economic_indicator": economic_indicator,
            "market_sentiment": market_sentiment,
            "commodity_action": commodity_action,
            "institutional_actor": institutional_actor,
            "political_leader": political_leader,
            "localized_noise": localized_noise,
            "strong_tactical_signal": strong_tactical_signal,
            "bonus": bonus,
            "multiplier": multiplier,
        }
        return bonus, multiplier, details

    def _numeric_signal_bonus(self, item: IntelItem) -> float:
        text = self._build_text(item).lower()
        extra = item.extra or {}
        pct_values: list[float] = []

        for key in ("change_pct", "pct_change"):
            if extra.get(key) is not None:
                try:
                    pct_values.append(abs(float(str(extra[key]).replace(",", "."))))
                except Exception:
                    pass

        patterns = []
        if self.directional_pct_regex:
            patterns.append(self.directional_pct_regex)
        patterns.extend([
            r"(?:drop|fall|rise|gain|surge|plunge|jump|climb|crash|rally|slide|tumble|soar|spike|dip|down|up|grow|grew|grown|declin\w*|devalu\w*|depreciat\w*)\w{0,6}\s+(\d+(?:\.\d+)?)\s*(?:%|percent)",
            r"(\d+(?:\.\d+)?)\s*(?:%|percent)\s+(?:drop|fall|rise|gain|surge|plunge|jump|climb|crash|rally|slide|tumble|soar|spike|dip|down|up|growth|decline)",
        ])
        for pattern in patterns:
            try:
                for match in re.findall(pattern, text, flags=re.IGNORECASE):
                    raw = match[0] if isinstance(match, tuple) else match
                    pct_values.append(abs(float(str(raw).replace(",", "."))))
            except Exception as exc:
                self.log.warning("numeric regex falhou: %s", exc)

        phrase_bonus = 0.0
        if self._contains_any(text, ["more than doubled", "more than double", "doubled", "lowest value ever", "highest recorded", "record high", "record low", "declined rapidly", "currency collapsed"]):
            phrase_bonus = 0.10

        pct_bonus = 0.0
        if pct_values:
            val = max(pct_values)
            for threshold in self.numeric_thresholds:
                if val >= float(threshold.get("min_pct", 0.0)):
                    pct_bonus = float(threshold.get("bonus", 0.0))
                    break

        return min(max(pct_bonus, phrase_bonus), 0.14)

    def _keyword_boost(self, text: str) -> Tuple[float, List[Dict[str, Any]]]:
        normalized = text.lower()
        hits: list[tuple[str, float, str]] = []
        seen_roots = set()
        for keyword, weight in self.keyword_weights.items():
            kw = str(keyword).lower().strip()
            if not kw:
                continue
            if self._keyword_matches(normalized, kw):
                root = self._keyword_root(kw)
                if root in seen_roots:
                    continue
                seen_roots.add(root)
                hits.append((kw, float(weight), root))

        if not hits:
            return 0.0, []
            
        deescalation = self._contains_any(normalized, self.DEESCALATION_TERMS)
        if deescalation:
            for i, (kw, weight, root) in enumerate(hits):
                if kw in {"nuclear", "radioactive", "radiation"}:
                    hits[i] = (kw, weight * 0.25, root)

        hits.sort(key=lambda x: x[1], reverse=True)
        hits = hits[: max(1, self.keyword_diminishing_max_hits)]
        total = 0.0
        details = []
        for idx, (kw, weight, root) in enumerate(hits):
            contribution = weight * (self.keyword_diminishing_factor ** idx)
            total += contribution
            details.append({"keyword": kw, "root": root, "weight": weight, "rank": idx + 1, "contribution": contribution})
        return min(total, self.max_keyword_boost), details

    def _engagement_bonus(self, item: IntelItem, category: str) -> float:
        if category in self.NOISE_CATEGORIES:
            return 0.0
        extra = item.extra or {}
        views = self._safe_float(extra.get("views", 0.0))
        forwards = self._safe_float(extra.get("forwards", 0.0))
        bonus = 0.0
        if views >= self.engagement_views_threshold:
            bonus += 0.020 * math.log10(max(views / max(self.engagement_views_threshold, 1), 1.0))
        if forwards >= self.engagement_forwards_threshold:
            bonus += 0.030 * math.log10(max(forwards / max(self.engagement_forwards_threshold, 1), 1.0))
        return min(max(bonus, 0.0), self.engagement_max_bonus)

    def _critical_bonus(self, *, impact_category: str, confidence: float, gliner_details: Dict[str, Any], keyword_hits: Sequence[Dict[str, Any]], context: Dict[str, Any]) -> float:
        if context.get("local_incident") or context.get("historical_reference"):
            return 0.0
        high_risk_category = impact_category in self.HIGH_RISK_CATEGORIES
        tactical = bool(gliner_details.get("strong_tactical_signal"))
        macro = bool(gliner_details.get("macro_target") or gliner_details.get("strategic_infra"))
        strong_keyword = any(hit.get("weight", 0.0) >= 0.11 for hit in keyword_hits)
        if confidence >= 0.80 and high_risk_category and (tactical or strong_keyword or macro):
            return self.critical_bonus
        if confidence >= 0.70 and high_risk_category and (tactical and strong_keyword):
            return self.critical_bonus * 0.75
        return 0.0

    def _context_score_adjustment(self, context: Dict[str, Any], category: str) -> Tuple[float, List[Tuple[str, float]]]:
        multiplier = 1.0
        caps: List[Tuple[str, float]] = []
        if context.get("self_promotion"):
            multiplier *= 0.15
            caps.append(("self_promotion_score_cap", 0.10))
        if context.get("local_incident"):
            multiplier *= 0.35
            caps.append(("local_incident_score_cap", self.local_incident_score_cap))
        if context.get("historical_reference"):
            multiplier *= 0.45
            caps.append(("historical_reference_score_cap", self.historical_reference_score_cap))
        if context.get("battlefield_micro") and not context.get("macro_signal"):
            multiplier *= 0.80
            caps.append(("battlefield_micro_score_cap", self.battlefield_micro_score_cap))
        return multiplier, caps

    def _compress_score(self, raw_score: float) -> float:
        raw_score = max(raw_score, 0.0)
        if raw_score <= self.soft_cap_start:
            return raw_score
        headroom = self.soft_cap_max - self.soft_cap_start
        compressed_tail = headroom * (1.0 - math.exp(-(raw_score - self.soft_cap_start) / max(self.soft_cap_scale, 0.01)))
        return self.soft_cap_start + compressed_tail

    def _soften_domain_weight(self, domain_weight: float) -> float:
        return 1.0 + (domain_weight - 1.0) * self.domain_weight_softening

    def _risk_bucket(self, score: float) -> str:
        if score >= 0.85:
            return "critical"
        if score >= 0.70:
            return "high"
        if score >= 0.55:
            return "elevated"
        if score >= 0.35:
            return "watch"
        return "low"

    def _build_text(self, item: IntelItem) -> str:
        return f"{getattr(item, 'title', '')} {getattr(item, 'body', '')}".strip()

    def _resolve_domain_weight(self, domain: str) -> float:
        normalized = (domain or "").strip().lower()
        normalized = self.domain_aliases.get(normalized, normalized)
        return self._safe_float(self.domain_weights.get(normalized, 1.0))

    def _normalize_severity(self, severity: Any) -> str:
        value = str(severity or "").strip().upper()
        if value in {"HIGH", "CRITICAL", "SEVERE"}:
            return "HIGH"
        if value in {"MEDIUM", "MODERATE"}:
            return "MEDIUM"
        if value in {"LOW", "MINOR"}:
            return "LOW"
        return ""

    def _default_category_weight(self, category: str) -> float:
        if category in self.NOISE_CATEGORIES:
            return 0.20
        if category in self.HIGH_RISK_CATEGORIES:
            return 1.10
        return 0.95

    def _extract_matched_keywords(self, text: str, nlp_features: Dict[str, Any]) -> List[str]:
        matched_keywords = [ent["text"] for ent in nlp_features.get("entities", []) if ent.get("text")]
        gliner_graph = nlp_features.get("gliner_graph", {})
        for words in gliner_graph.values():
            matched_keywords.extend(words)
        normalized = text.lower()
        for keyword in self.keyword_weights:
            kw = str(keyword).strip()
            if kw and self._keyword_matches(normalized, kw.lower()):
                matched_keywords.append(kw)
        return self._unique_strings(matched_keywords)

    def _keyword_matches(self, text_lower: str, keyword_lower: str) -> bool:
        if not keyword_lower:
            return False
        if len(keyword_lower) <= 3 or keyword_lower.isalpha():
            return re.search(rf"\b{re.escape(keyword_lower)}\b", text_lower, flags=re.IGNORECASE) is not None
        return keyword_lower in text_lower

    def _keyword_root(self, keyword: str) -> str:
        if keyword.endswith("ies"):
            return keyword[:-3] + "y"
        if keyword.endswith("s") and len(keyword) > 4:
            return keyword[:-1]
        return keyword

    def _contains_any(self, text_lower: str, terms: Sequence[str]) -> bool:
        return any(term.lower() in text_lower for term in terms)

    def _unique_strings(self, values: Iterable[Any]) -> List[str]:
        out = []
        seen = set()
        for value in values:
            text = str(value).strip()
            key = text.lower()
            if text and key not in seen:
                seen.add(key)
                out.append(text)
        return out

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(min(value, high), low)
