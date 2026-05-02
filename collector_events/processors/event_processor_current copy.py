# services/collector_events/collector_events/processors/event_processor.py
"""
EventProcessor — Stage 2 da pipeline de inteligência global.

Refatorado: Substitui a análise puramente lexical (heurística) por uma camada 
semântica offline (LocalNLPEngine) que extrai Sentimento, Categoria e Entidades
antes de calcular o `danger_score`.
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
from collector_events.nlp.nlp_engine import LocalNLPEngine

_CONFIG_DIR = Path(__file__).parent / "config"
_SCORING_FILE = _CONFIG_DIR / "scoring.json"

class EventProcessor(Loggable):
    def __init__(self):
        super().__init__()
        self.country_resolver = CountryResolver()
        
        # Carrega as configurações base de pontuação (mantendo retrocompatibilidade)
        self._load_scoring_config()
        
        # Inicia o Motor NLP (como é Singleton, é partilhado se instanciado várias vezes)
        self.nlp = LocalNLPEngine.get_instance()

    def _load_scoring_config(self):
        try:
            with open(_SCORING_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            self.severity_base = cfg.get("severity_base", {})
            self.domain_weights = cfg.get("domain_weights", {})
            self.numeric_thresholds = cfg.get("numeric_thresholds", [])
        except Exception as e:
            self.log.warning(f"Falha ao carregar scoring.json: {e}. A usar defaults.")
            self.severity_base = {"generic": 0.1, "conflict": 0.6, "macroeconomic data": 0.4}
            self.domain_weights = {"telegram": 1.2, "news": 1.0}
            self.numeric_thresholds = [{"min_pct": 5, "bonus": 0.2}]

    def process_items(self, items: Sequence[IntelItem]) -> list[IntelItem]:
        processed = []
        for it in items:
            processed.append(self.process_item(it))
        return processed

    def process_item(self, item: IntelItem) -> IntelItem:
        # 1. Resolução de Países (mantém o seu código existente)
        text_to_analyze = item.title
        if item.content:
            text_to_analyze += f". {item.content[:500]}"
            
        found_countries = self.country_resolver.resolve_countries(text_to_analyze)
        if found_countries:
            item.extra["countries"] = [c.iso2 for c in found_countries]

        # 2. ENRIQUECIMENTO SEMÂNTICO (O Novo Motor NLP)
        nlp_features = self.nlp.extract_features(text_to_analyze)
        
        # Atualizar metadados do item com os insights da IA
        item.extra["nlp_sentiment"] = nlp_features.get("sentiment", "neutral")
        item.extra["nlp_sentiment_score"] = round(nlp_features.get("sentiment_score", 0.0), 3)
        item.extra["impact_category"] = nlp_features.get("inferred_category", "generic")
        item.extra["nlp_entities"] = [e["text"] for e in nlp_features.get("entities", [])]
        item.extra["gliner_graph"] = nlp_features.get("gliner_graph", {})
        # 3. CÁLCULO DO SCORE SEMÂNTICO
        score = self._compute_semantic_danger_score(item, nlp_features, found_countries)
        item.extra["danger_score"] = round(min(score, 1.0), 3)

        return item
    
    def _reverse_crisis_filter(self, gliner_graph: dict) -> float:
        """
        Avalia se o evento é um Cisne Branco (ruído) ou um choque macroeconómico, 
        baseado na extração tática do GLiNER. Devolve um multiplicador para o score base.
        """
        if not gliner_graph:
            return 1.0

        has_macro_target = len(gliner_graph.get("macroeconomic infrastructure or strategic target", [])) > 0
        has_white_swan = len(gliner_graph.get("civilian vehicle or local infrastructure", [])) > 0
        has_local_tragedy = len(gliner_graph.get("localized accident or personal tragedy", [])) > 0

        # Ouro para Forex: Infraestrutura Macroeconómica afetada
        if has_macro_target:
            return 1.5  

        # Cisne Branco: Tragédia local sem impacto sistémico
        if has_white_swan or has_local_tragedy:
            return 0.1  

        # Neutro
        return 1.0

    def _compute_semantic_danger_score(
        self, item: IntelItem, nlp_features: dict, found_countries: list[CountryRef]
    ) -> float:
        """
        Nova fórmula: Base Severity + (Sentimento) + (Relevância de Entidades) + (Bónus Numérico)
        """
        category = item.extra.get("impact_category", "generic")
        domain = item.domain
        
        # Base da categoria inferida pelo DeBERTa
        base = self.severity_base.get(category, 0.2)
        weight = self.domain_weights.get(domain, 1.0)
        
        score = base * weight

        # 1. Modificador de Sentimento (FinBERT)
        sentiment = nlp_features.get("sentiment")
        confidence = nlp_features.get("sentiment_score", 0.0)
        
        if sentiment == "negative":
            # Notícias negativas aumentam o perigo/risco
            score += (0.3 * confidence)
        elif sentiment == "positive":
            # Notícias positivas geralmente reduzem o risco sistémico
            score -= (0.15 * confidence)

        # 2. Modificador de Entidades (spaCy NER)
        entities = nlp_features.get("entities", [])
        has_central_bank = any(e["label"] == "CENTRAL_BANK" for e in entities)
        has_g10_country = any(e["label"] == "G10_COUNTRY" for e in entities)
        
        if has_central_bank:
            score += 0.25 # Impacto fortíssimo no Forex
        if has_g10_country:
            score += 0.15

        # 3. Modificador Numérico Clássico (Regex para percentagens - Mantido!)
        score += self._numeric_signal_bonus(item)
        
        _reverse_crisis_filter = self._reverse_crisis_filter(nlp_features.get("gliner_graph", {}))
        score *= _reverse_crisis_filter

        # Prevenir scores negativos
        return max(score, 0.0)

    def _numeric_signal_bonus(self, item: IntelItem) -> float:
        bonus = 0.0
        text = (item.title + " " + item.body).lower()
        
        # 1. Tentar pegar de dados já estruturados (High Performance)
        extra = item.extra or {}
        change_pct = extra.get("change_pct") or extra.get("pct_change")
        
        # 2. Backup: Regex para capturar números no texto bruto
        if change_pct is None:
            # Procura por números seguidos de % (ex: 5.5%, -10%, 15 %)
            matches = re.findall(r'([-+]?\d+(?:[\.,]\d+)?)\s*%', text)
            if matches:
                # Pega o maior valor absoluto encontrado
                change_pct = max([abs(float(m.replace(',', '.'))) for m in matches])

        # 3. Cálculo do Bónus baseado na magnitude
        if change_pct:
            val = abs(float(change_pct))
            if val >= 5.0: bonus = 0.25  # Impacto Crítico
            elif val >= 2.0: bonus = 0.15 # Impacto Relevante
            elif val >= 0.5: bonus = 0.05 # Ruído de Mercado
            
        return bonus