# services/collector_events/collector_events/processors/event_processor.py
"""
EventProcessor — Stage 2 da pipeline de inteligência global.

Versão hard-quant / auditável:
- Usa LocalNLPEngine para NLP/NER/GLiNER.
- Usa scoring.json como matriz de calibração real.
- Separa base score, bônus, penalidades, multiplicadores e score final.
- Injeta score_breakdown em item.extra para auditoria e calibração.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from forex_shared.domain.intel import IntelItem
from forex_shared.logging.loggable import Loggable

from collector_events.processors.country_resolver import CountryResolver
from collector_events.nlp.nlp_engine import LocalNLPEngine

_CONFIG_DIR = Path(__file__).parent / "config"
_SCORING_FILE = _CONFIG_DIR / "scoring.json"


@dataclass
class ProcessedEvent:
    """Contrato esperado pelo IntelMongoStore para gravação no DB."""
    impact_category: str
    danger_score: float
    matched_keywords: List[str]
    domain_weight: float = 1.0
    features: Dict[str, Any] = field(default_factory=dict)
    score_breakdown: Dict[str, Any] = field(default_factory=dict)


class EventProcessor(Loggable):
    """
    Processa um IntelItem, extrai features semânticas e calcula danger_score.

    Ideia central:
    - NLP classifica o tipo de evento.
    - scoring.json calibra o impacto operacional.
    - score_breakdown explica exatamente por que o score ficou alto/baixo.
    """

    DEFAULT_KEYWORD_WEIGHTS: dict[str, float] = {
        # Geopolítica / militar
        "attack": 0.08,
        "attacks": 0.08,
        "strike": 0.08,
        "strikes": 0.08,
        "missile": 0.10,
        "missiles": 0.10,
        "drone": 0.08,
        "drones": 0.08,
        "explosion": 0.07,
        "explosions": 0.07,
        "invasion": 0.12,
        "war": 0.10,
        "troops": 0.07,
        "mobilization": 0.09,
        "blockade": 0.11,
        "airspace closure": 0.09,
        "naval blockade": 0.12,
        "terrorist": 0.10,
        "mass casualty": 0.12,
        "nuclear": 0.18,
        "radioactive": 0.16,
        "radiation": 0.14,
        # Sanctions / supply chain
        "sanction": 0.08,
        "sanctions": 0.08,
        "embargo": 0.09,
        "trade embargo": 0.10,
        "export ban": 0.08,
        "supply chain": 0.07,
        "port strike": 0.08,
        "shipping route": 0.07,
        "chokepoint": 0.09,
        "strait of hormuz": 0.14,
        "red sea": 0.08,
        "suez": 0.08,
        "pipeline": 0.08,
        "refinery": 0.09,
        "power grid": 0.11,
        # Macro / mercado
        "fed": 0.08,
        "federal reserve": 0.09,
        "ecb": 0.08,
        "central bank": 0.08,
        "interest rate": 0.08,
        "rate hike": 0.09,
        "rate cut": 0.09,
        "inflation": 0.08,
        "cpi": 0.08,
        "nfp": 0.08,
        "nonfarm payroll": 0.08,
        "recession": 0.10,
        "oil": 0.07,
        "brent": 0.07,
        "wti": 0.07,
        "gold": 0.07,
        "xau": 0.07,
        "sell-off": 0.09,
        "crash": 0.11,
        "flash crash": 0.13,
        "plunge": 0.09,
        "surge": 0.07,
        # Cyber
        "cyber attack": 0.10,
        "ransomware": 0.09,
        "data breach": 0.07,
        "infrastructure hack": 0.10,
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

    def __init__(self):
        super().__init__()
        self.country_resolver = CountryResolver()

        self.severity_base: dict[str, float] = {}
        self.category_weight: dict[str, float] = {}
        self.domain_weights: dict[str, float] = {}
        self.domain_aliases: dict[str, str] = {}

        self.critical_bonus: float = 0.15
        self.directional_pct_regex: str | None = None
        self.max_keyword_boost: float = 0.40
        self.keyword_diminishing_max_hits: int = 3
        self.keyword_diminishing_factor: float = 0.4
        self.keyword_weights: dict[str, float] = dict(self.DEFAULT_KEYWORD_WEIGHTS)
        self.localized_noise_multiplier: float = 0.35
        self.numeric_thresholds: list[dict[str, float]] = []

        self.engagement_max_bonus: float = 0.12
        self.engagement_views_threshold: int = 10_000
        self.engagement_forwards_threshold: int = 50

        self._load_scoring_config()

        # Motor NLP Singleton (Carrega na RAM apenas 1x)
        self.nlp = LocalNLPEngine.get_instance()

    # ─────────────────────────────────────────────────────────────
    # Config
    # ─────────────────────────────────────────────────────────────

    def _load_scoring_config(self) -> None:
        """Permite alterar pesos e limites sem tocar no código fonte."""
        try:
            with open(_SCORING_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)

            self.severity_base = config.get(
                "severity_base",
                {"HIGH": 0.70, "MEDIUM": 0.45, "LOW": 0.20, "": 0.30},
            )
            self.category_weight = config.get("category_weight", {})
            self.domain_weights = config.get("domain_weight", {})
            self.domain_aliases = config.get("domain_aliases", {})

            self.critical_bonus = float(config.get("critical_bonus", 0.15))
            self.directional_pct_regex = config.get("directional_pct_regex")
            self.max_keyword_boost = float(config.get("max_keyword_boost", 0.40))
            self.keyword_diminishing_max_hits = int(
                config.get("keyword_diminishing_max_hits", 3)
            )
            self.keyword_diminishing_factor = float(
                config.get("keyword_diminishing_factor", 0.4)
            )
            self.localized_noise_multiplier = float(
                config.get("localized_noise_multiplier", 0.35)
            )

            # Defaults + overrides do JSON.
            json_keyword_weights = config.get("keyword_weights", {})
            self.keyword_weights = {
                **self.DEFAULT_KEYWORD_WEIGHTS,
                **json_keyword_weights,
            }

            self.numeric_thresholds = config.get(
                "numeric_thresholds",
                [
                    {"min_pct": 5.0, "bonus": 0.15},
                    {"min_pct": 3.0, "bonus": 0.10},
                    {"min_pct": 1.0, "bonus": 0.05},
                ],
            )
            self.numeric_thresholds.sort(
                key=lambda x: float(x.get("min_pct", 0.0)),
                reverse=True,
            )

            engagement = config.get("engagement_bonus", {})
            self.engagement_max_bonus = float(
                engagement.get("max_bonus", self.engagement_max_bonus)
            )
            self.engagement_views_threshold = int(
                engagement.get("views_threshold", self.engagement_views_threshold)
            )
            self.engagement_forwards_threshold = int(
                engagement.get("forwards_threshold", self.engagement_forwards_threshold)
            )

            self.log.info(
                "Scoring config carregada | domains=%s | categories=%s | keywords=%s",
                len(self.domain_weights),
                len(self.category_weight),
                len(self.keyword_weights),
            )

        except Exception as e:
            self.log.warning(
                "Aviso: Não foi possível carregar %s. Usando defaults. Erro: %s",
                _SCORING_FILE,
                e,
            )

    # ─────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────

    def process_items(self, items: Sequence[IntelItem]) -> list[IntelItem]:
        """Permite processamento em lote pelo Orquestrador."""
        processed_list = []
        for item in items:
            try:
                self.process_item(item)
                processed_list.append(item)
            except Exception as e:
                self.log.error("Erro ao processar item %s: %s", item.id, e)
        return processed_list

    def process_item(self, item: IntelItem) -> ProcessedEvent:
        """Executa extração semântica e calcula score."""
        text = self._build_text(item)

        # 1. Inferência NLP
        nlp_features = self.nlp.extract_features(text)

        # 2. Resolução de país
        if not getattr(item, "country", None) or len(item.country) == 0:
            item.country = self.country_resolver.resolve(text)

        # 3. Score
        domain_weight = self._resolve_domain_weight(getattr(item, "domain", ""))
        danger_score, score_breakdown = self._compute_semantic_danger_score(
            item,
            nlp_features,
            domain_weight,
        )
        impact_category = nlp_features.get("inferred_category", "generic")

        # 4. Matched keywords / entidades para indexação e auditoria
        matched_keywords = self._extract_matched_keywords(text, nlp_features)

        # 5. Injeção in-place no IntelItem
        if item.extra is None:
            item.extra = {}

        item.extra["danger_score"] = danger_score
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
        )

    # ─────────────────────────────────────────────────────────────
    # Scoring
    # ─────────────────────────────────────────────────────────────

    def _compute_semantic_danger_score(
        self,
        item: IntelItem,
        nlp_features: Dict[str, Any],
        domain_weight: float,
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Matemática de risco auditável.

        Filosofia:
        - confidence mede encaixe semântico, não severidade bruta.
        - category_weight transforma categoria em impacto esperado.
        - severity_base atua como piso calibrado pelo tipo de evento.
        - GLiNER adiciona sinais táticos.
        - domain_weight escala pelo domínio operacional.
        """
        text = self._build_text(item)
        confidence = self._safe_float(nlp_features.get("category_confidence", 0.0))
        impact_category = str(nlp_features.get("inferred_category", "generic"))

        severity_key = self._normalize_severity(getattr(item, "severity", ""))
        severity_base = self._safe_float(
            self.severity_base.get(severity_key, self.severity_base.get("", 0.30))
        )
        category_weight = self._safe_float(
            self.category_weight.get(impact_category, self._default_category_weight(impact_category))
        )

        # Score base:
        # - category_component: confiança semântica ponderada pela categoria.
        # - severity_component: severidade de feed ponderada pela categoria.
        #   Isso evita "unknown/default severity" inflar lixo genérico ou fofoca.
        category_component = 0.55 * confidence * category_weight
        severity_component = severity_base * min(max(category_weight, 0.05), 1.75)
        base_score = max(category_component, severity_component)

        sentiment_bonus = self._sentiment_modifier(
            nlp_features=nlp_features,
            category_weight=category_weight,
            confidence=confidence,
        )
        entity_bonus = self._entity_bonus(nlp_features.get("entities", []))
        gliner_bonus, gliner_multiplier, gliner_details = self._gliner_adjustment(
            nlp_features.get("gliner_graph", {}),
            impact_category=impact_category,
        )
        numeric_bonus = self._numeric_signal_bonus(item)
        keyword_bonus, keyword_hits = self._keyword_boost(text)
        engagement_bonus = self._engagement_bonus(item)

        critical_bonus = self._critical_bonus(
            impact_category=impact_category,
            confidence=confidence,
            gliner_details=gliner_details,
            keyword_hits=keyword_hits,
        )

        additive_score = (
            base_score
            + sentiment_bonus
            + entity_bonus
            + gliner_bonus
            + numeric_bonus
            + keyword_bonus
            + engagement_bonus
            + critical_bonus
        )

        # Multiplicadores finais.
        multiplier = domain_weight * gliner_multiplier
        final_score = self._clamp(additive_score * multiplier, 0.0, 1.0)

        breakdown: Dict[str, Any] = {
            "version": "hard_quant_v1",
            "inputs": {
                "category": impact_category,
                "confidence": confidence,
                "sentiment": nlp_features.get("sentiment", "neutral"),
                "severity": getattr(item, "severity", ""),
                "severity_key": severity_key,
                "domain": getattr(item, "domain", ""),
            },
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
                "gliner_multiplier": gliner_multiplier,
                "total_multiplier": multiplier,
            },
            "scores": {
                "additive_score": additive_score,
                "final_score": final_score,
            },
        }

        return final_score, breakdown

    def _sentiment_modifier(
        self,
        *,
        nlp_features: Dict[str, Any],
        category_weight: float,
        confidence: float,
    ) -> float:
        sentiment = str(nlp_features.get("sentiment", "neutral")).lower()

        if sentiment == "negative":
            return 0.12 * confidence * min(category_weight, 1.5)

        if sentiment == "positive":
            # Para evento macro/mercado, "positive" nem sempre reduz risco.
            # Reduz pouco e só quando categoria não é crítica.
            category = str(nlp_features.get("inferred_category", ""))
            if category in self.HIGH_RISK_CATEGORIES:
                return 0.0
            return -0.06 * confidence

        return 0.0

    def _entity_bonus(self, entities: Sequence[Dict[str, Any]]) -> float:
        labels = {str(e.get("label", "")) for e in entities}

        bonus = 0.0
        if "CENTRAL_BANK" in labels:
            bonus += 0.18
        if "G10_COUNTRY" in labels:
            bonus += 0.08
        if "FOREX_PAIR" in labels:
            bonus += 0.10
        if "COMMODITY" in labels:
            bonus += 0.08

        return min(bonus, 0.28)

    def _gliner_adjustment(
        self,
        gliner_graph: Dict[str, list],
        *,
        impact_category: str,
    ) -> Tuple[float, float, Dict[str, Any]]:
        if not gliner_graph:
            return 0.0, 1.0, {
                "macro_target": False,
                "localized_noise": False,
                "strong_tactical_signal": False,
            }

        def count(label: str) -> int:
            values = gliner_graph.get(label, []) or []
            return len(values)

        macro_target = count("macroeconomic infrastructure or strategic target") > 0
        strategic_infra = count("strategic infrastructure") > 0
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

        localized_noise = (
            count("civilian vehicle or local infrastructure") > 0
            or count("localized accident or personal tragedy") > 0
        )

        strong_tactical_signal = any(
            [
                macro_target,
                strategic_infra,
                kinetic,
                weapon,
                military_actor,
                geopolitical,
                central_bank,
                economic_indicator,
                market_sentiment,
                commodity_action,
            ]
        )

        bonus = 0.0
        if macro_target:
            bonus += 0.20
        if strategic_infra:
            bonus += 0.18
        if kinetic:
            bonus += 0.15
        if weapon:
            bonus += 0.07
        if military_actor:
            bonus += 0.08
        if geopolitical:
            bonus += 0.08
        if central_bank:
            bonus += 0.12
        if economic_indicator:
            bonus += 0.10
        if commodity_action:
            bonus += 0.10
        if market_sentiment:
            bonus += 0.08
        if institutional_actor:
            bonus += 0.05
        if political_leader:
            bonus += 0.03

        bonus = min(bonus, 0.35)

        multiplier = 1.0
        if macro_target or strategic_infra:
            multiplier *= 1.25

        # Penaliza ruído local só se não houver sinal forte.
        # Antes isso podia esmagar evento relevante com multiplicador 0.1/0.35.
        if localized_noise and not strong_tactical_signal and impact_category not in self.HIGH_RISK_CATEGORIES:
            multiplier *= self.localized_noise_multiplier

        details = {
            "macro_target": macro_target,
            "strategic_infra": strategic_infra,
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
        """
        Captura movimentos percentuais relevantes.

        Ordem:
        1. Usa change_pct/pct_change estruturado, se existir.
        2. Usa directional_pct_regex do scoring.json.
        3. Não usa regex genérica solta para evitar falso positivo tipo:
           "80% chance of rain", "30% of voters", etc.
        """
        text = self._build_text(item).lower()
        extra = item.extra or {}

        change_pct = extra.get("change_pct") or extra.get("pct_change")
        pct_values: list[float] = []

        if change_pct is not None:
            try:
                pct_values.append(abs(float(str(change_pct).replace(",", "."))))
            except Exception:
                pass

        if self.directional_pct_regex:
            try:
                matches = re.findall(self.directional_pct_regex, text, flags=re.IGNORECASE)
                for match in matches:
                    # findall pode retornar str ou tuple, dependendo dos grupos.
                    raw = match[0] if isinstance(match, tuple) else match
                    pct_values.append(abs(float(str(raw).replace(",", "."))))
            except Exception as exc:
                self.log.warning("directional_pct_regex falhou: %s", exc)

        if not pct_values:
            return 0.0

        val = max(pct_values)
        for threshold in self.numeric_thresholds:
            min_pct = float(threshold.get("min_pct", 0.0))
            bonus = float(threshold.get("bonus", 0.0))
            if val >= min_pct:
                return bonus

        return 0.0

    def _keyword_boost(self, text: str) -> Tuple[float, List[Dict[str, Any]]]:
        """
        Keyword boost com diminishing returns.

        Não substitui o NLP. Serve como sinal quant leve para termos críticos
        que historicamente mexem com risco/mercado.
        """
        normalized = text.lower()
        hits: list[tuple[str, float]] = []

        for keyword, weight in self.keyword_weights.items():
            kw = str(keyword).lower().strip()
            if not kw:
                continue

            if kw in normalized:
                hits.append((kw, float(weight)))

        if not hits:
            return 0.0, []

        # Mais fortes primeiro.
        hits.sort(key=lambda x: x[1], reverse=True)
        hits = hits[: max(1, self.keyword_diminishing_max_hits)]

        total = 0.0
        hit_details: list[dict[str, Any]] = []

        for idx, (keyword, weight) in enumerate(hits):
            contribution = weight * (self.keyword_diminishing_factor ** idx)
            total += contribution
            hit_details.append(
                {
                    "keyword": keyword,
                    "weight": weight,
                    "rank": idx + 1,
                    "contribution": contribution,
                }
            )

        return min(total, self.max_keyword_boost), hit_details

    def _engagement_bonus(self, item: IntelItem) -> float:
        """
        Bônus leve para propagação em fontes sociais/Telegram.

        Usa escala logarítmica para não deixar views/forwards dominarem o score.
        """
        extra = item.extra or {}

        views = self._safe_float(extra.get("views", 0.0))
        forwards = self._safe_float(extra.get("forwards", 0.0))

        bonus = 0.0

        if views >= self.engagement_views_threshold:
            ratio = max(views / max(self.engagement_views_threshold, 1), 1.0)
            bonus += 0.03 * math.log10(ratio)

        if forwards >= self.engagement_forwards_threshold:
            ratio = max(forwards / max(self.engagement_forwards_threshold, 1), 1.0)
            bonus += 0.04 * math.log10(ratio)

        return min(max(bonus, 0.0), self.engagement_max_bonus)

    def _critical_bonus(
        self,
        *,
        impact_category: str,
        confidence: float,
        gliner_details: Dict[str, Any],
        keyword_hits: Sequence[Dict[str, Any]],
    ) -> float:
        high_risk_category = impact_category in self.HIGH_RISK_CATEGORIES
        tactical = bool(gliner_details.get("strong_tactical_signal"))
        macro = bool(gliner_details.get("macro_target") or gliner_details.get("strategic_infra"))

        strong_keyword = any(
            hit.get("weight", 0.0) >= 0.12 for hit in keyword_hits
        )

        if confidence >= 0.70 and high_risk_category:
            return self.critical_bonus

        if confidence >= 0.55 and high_risk_category and (tactical or strong_keyword):
            return self.critical_bonus

        if confidence >= 0.50 and macro:
            return self.critical_bonus * 0.75

        return 0.0

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────

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

        # "unknown", "none", vazio etc caem no fallback "" do JSON.
        return ""

    def _default_category_weight(self, category: str) -> float:
        if category in self.NOISE_CATEGORIES:
            return 0.25
        if category in self.HIGH_RISK_CATEGORIES:
            return 1.20
        return 1.0

    def _extract_matched_keywords(
        self,
        text: str,
        nlp_features: Dict[str, Any],
    ) -> List[str]:
        matched_keywords = [
            ent["text"]
            for ent in nlp_features.get("entities", [])
            if ent.get("text")
        ]

        gliner_graph = nlp_features.get("gliner_graph", {})
        for words in gliner_graph.values():
            matched_keywords.extend(words)

        normalized = text.lower()
        for keyword in self.keyword_weights:
            kw = str(keyword).strip()
            if kw and kw.lower() in normalized:
                matched_keywords.append(kw)

        # Dedup preservando ordem.
        seen = set()
        result = []
        for value in matched_keywords:
            clean = str(value).strip()
            key = clean.lower()
            if clean and key not in seen:
                seen.add(key)
                result.append(clean)

        return result

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(min(value, high), low)
