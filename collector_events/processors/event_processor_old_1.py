# services/collector_events/collector_events/processors/event_processor.py
"""
EventProcessor — Stage 2 da pipeline de inteligência global.

Unificação: 
Traz o poder semântico da IA Local (DeBERTa, FinBERT, GLiNER) 
mas REINTEGRA o processamento em lote, pesos por domínio (domain_weights)
e limiares configuráveis da versão clássica para calibração dinâmica.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Sequence

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


class EventProcessor(Loggable):
    def __init__(self):
        super().__init__()
        self.country_resolver = CountryResolver()
        
        # Estruturas de calibração recuperadas da versão antiga
        self.domain_weights: dict[str, float] = {}
        self.numeric_thresholds: list[dict] = []
        self._load_scoring_config()
        
        # Motor NLP Singleton (Carrega na RAM apenas 1x)
        self.nlp = LocalNLPEngine.get_instance()

    def _load_scoring_config(self):
        """Reintegrado: Permite alterar pesos e limites sem tocar no código fonte."""
        try:
            with open(_SCORING_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)
                
                # Scoring config (Ex: Pesos por domínio, bónus para palavras-chave, limiares numéricos)
                self.severity_base = config.get("severity_base", {})
                self.category_weight = config.get("category_weight", {})
                self.domain_weights = config.get("domain_weight", {})
                self.critical_bonus = config.get("critical_bonus", 0.15)
                self.directional_pct_regex = config.get("directional_pct_regex")
                self.max_keyword_boost = config.get("max_keyword_boost", 0.25)
                self.keyword_diminishing_max_hits = config.get("keyword_diminishing_max_hits", 3)
                self.keyword_diminishing_factor = config.get("keyword_diminishing_factor", 0.4)
                self.keyword_weights = config.get("keyword_weights", {})
                self.localized_noise_multiplier = config.get("localized_noise_multiplier", 0.35)
                self.numeric_thresholds = config.get("numeric_thresholds", [
                    {"min_pct": 5.0, "bonus": 0.25},
                    {"min_pct": 2.0, "bonus": 0.15},
                    {"min_pct": 0.5, "bonus": 0.05}
                ])
                # Ordena os limiares do maior para o menor para a lógica de bónus
                self.numeric_thresholds.sort(key=lambda x: x["min_pct"], reverse=True)
                
                self.log.info("Scoring config carregada com sucesso.")
                
        except Exception as e:
            self.log.warning(f"Aviso: Não foi possível carregar {_SCORING_FILE}. Usando defaults. Erro: {e}")
            self.domain_weights = {}

    def process_items(self, items: Sequence[IntelItem]) -> list[IntelItem]:
        """Reintegrado: Permite processamento em lote pelo Orquestrador."""
        processed_list = []
        for item in items:
            try:
                self.process_item(item)
                processed_list.append(item)
            except Exception as e:
                self.log.error(f"Erro ao processar item {item.id}: {e}")
        return processed_list

    def process_item(self, item: IntelItem) -> ProcessedEvent:
        """Executa a extração semântica e calcula o score com base no domínio."""
        text = (item.title + " " + item.body).strip()
        
        # 1. Inferência NLP
        nlp_features = self.nlp.extract_features(text)
        
        # 2. Resolução de País (Heurística)
        if not getattr(item, "country", None) or len(item.country) == 0:
            item.country = self.country_resolver.resolve(text)
            
        # 3. Pesos e Score
        domain_weight = self.domain_weights.get(item.domain, 1.0)
        danger_score = self._compute_semantic_danger_score(item, nlp_features, domain_weight)
        impact_category = nlp_features.get("inferred_category", "generic")
        
        # 4. Compatibilidade 'matched_keywords' para o MongoDB
        matched_keywords = [ent["text"] for ent in nlp_features.get("entities", [])]
        gliner_graph = nlp_features.get("gliner_graph", {})
        for words in gliner_graph.values():
            matched_keywords.extend(words)
        matched_keywords = list(set(matched_keywords))
        
        # 5. Injeção in-place no IntelItem
        if item.extra is None:
            item.extra = {}
        item.extra["danger_score"] = danger_score
        item.extra["impact_category"] = impact_category
        item.extra["nlp_features"] = nlp_features
        item.extra["domain_weight"] = domain_weight
        
        return ProcessedEvent(
            impact_category=impact_category,
            danger_score=danger_score,
            matched_keywords=matched_keywords,
            domain_weight=domain_weight,
            features=nlp_features
        )

    def _compute_semantic_danger_score(self, item: IntelItem, nlp_features: Dict[str, Any], domain_weight: float) -> float:
        """Matemática de risco: Combina a base da IA com os multiplicadores de negócio."""
        confidence = nlp_features.get("category_confidence", 0.0)
        score = 0.5 * confidence 
        
        # Modificadores Lineares
        sentiment = nlp_features.get("sentiment", "neutral")
        if sentiment == "negative": score += (0.2 * confidence)
        elif sentiment == "positive": score -= (0.15 * confidence)

        entities = nlp_features.get("entities", [])
        if any(e["label"] == "CENTRAL_BANK" for e in entities): score += 0.25
        if any(e["label"] == "G10_COUNTRY" for e in entities): score += 0.15

        score += self._numeric_signal_bonus(item)
        
        # Multiplicadores de Escala
        score *= domain_weight # Reintegrado: Aplica peso do domínio (ex: Geopolítica tem mais impacto que Clima)
        score *= self._reverse_crisis_filter(nlp_features.get("gliner_graph", {}))

        return max(min(score, 1.0), 0.0)

    def _numeric_signal_bonus(self, item: IntelItem) -> float:
        """Reintegrado: Captura quedas usando limiares configuráveis no JSON."""
        text = (item.title + " " + item.body).lower()
        change_pct = (item.extra or {}).get("change_pct") or (item.extra or {}).get("pct_change")
        
        if change_pct is None:
            matches = re.findall(r'([-+]?\d+(?:[\.,]\d+)?)\s*%', text)
            if matches:
                change_pct = max([abs(float(m.replace(',', '.'))) for m in matches])

        if change_pct:
            val = abs(float(change_pct))
            for threshold in self.numeric_thresholds:
                if val >= threshold["min_pct"]:
                    return threshold["bonus"]
                    
        return 0.0

    def _reverse_crisis_filter(self, gliner_graph: dict) -> float:
        """Avalia se o evento é um Cisne Branco ou Choque Macro."""
        if not gliner_graph: return 1.0

        if len(gliner_graph.get("macroeconomic infrastructure or strategic target", [])) > 0:
            return 1.5
        if len(gliner_graph.get("civilian vehicle or local infrastructure", [])) > 0 or \
           len(gliner_graph.get("localized accident or personal tragedy", [])) > 0:
            return self.localized_noise_multiplier

        return 1.0