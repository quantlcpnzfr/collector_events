# services/collector_events/collector_events/processors/nlp_engine.py
"""
Motor NLP Offline (Local) para extração semântica em tempo real.
Utiliza modelos Small Language Models (SLMs) otimizados para CPU via ONNX.
"""

import logging
import spacy
from typing import Dict, Any, List
from transformers import pipeline

# Configuração do Logger
log = logging.getLogger("LocalNLPEngine")

class LocalNLPEngine:
    """
    Singleton para garantir que os modelos pesam na RAM apenas uma vez 
    por processo do Ray, evitando recarregamentos a cada evento.
    """
    _instance = None

    @classmethod
    def get_instance(cls) -> 'LocalNLPEngine':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        log.info("A iniciar o carregamento dos modelos NLP offline...")
        
        # 1. FinBERT - Análise de Sentimento Financeiro
        log.info("A carregar FinBERT (ProsusAI/finbert)...")
        self.finbert = pipeline(
            "text-classification", 
            model="ProsusAI/finbert",
            device=-1 # Força a utilização da CPU
        )
        
        # 2. DeBERTa - Classificação Zero-Shot para Categorização de Impacto
        log.info("A carregar DeBERTa (cross-encoder/nli-deberta-v3-small)...")
        self.classifier = pipeline(
            "zero-shot-classification", 
            model="cross-encoder/nli-deberta-v3-small",
            device=-1
        )
        
        # Categorias financeiras e geopolíticas mapeadas para o seu sistema
        self.categories = [
            # 🔴 WAR, CONFLICT & MILITARY (Extremo Risco Geopolítico)
            "military drone or missile strike",
            "declaration of war or armed conflict",
            "nuclear threat or radioactive incident",
            "troop mobilization or border skirmish",
            "military coup or government overthrow",
            "naval blockade or airspace closure",

            # 🕵️ INTELLIGENCE & CYBER (Ameaças Invisíveis)
            "intelligence agency report or espionage",
            "covert operation or state-sponsored assassination",
            "state-sponsored cyber attack or infrastructure hack",
            "major data breach or institutional ransomware",

            # 🏦 ECONOMICS, PRICES & WORLD BANKS (Política Monetária)
            "central bank interest rate decision or monetary policy",
            "macroeconomic data release or inflation report",
            "currency intervention or severe devaluation",
            "sudden price spike or hyperinflation warning",

            # 🌋 ENVIRONMENT & DISASTERS (Força Maior / Atos de Deus)
            "massive earthquake or tsunami",
            "catastrophic explosion or industrial disaster",
            "severe extreme weather or hurricane",
            "global pandemic or biological hazard",

            # 📉 MARKET & TRADING (Movimentos de Ativos)
            "stock market crash or massive sell-off",
            "commodity price surge or oil market disruption",
            "crypto market flash crash or major exchange hack",
            "institutional market downgrade or recession warning",
            "major hedge fund collapse or massive margin call",
            "insider trading scandal or massive financial fraud",

            # 🛑 SANCTIONS, SOCIAL & SUPPLY CHAIN (Atrito e Logística)
            "international economic sanctions or trade embargo",
            "critical supply chain disruption or port strike",
            "shipping route stress or maritime chokepoint closure",
            "strategic mineral or semiconductor shortage",
            "civil unrest, massive protests or violent riots",
            "terrorist attack or mass casualty event",
            
            # 🛑 Unknown and uncategorized disruptive events 
            "unprecedented global crisis or major disruptive anomaly", 
            "sudden market shock or black swan event",
            
            # ⚪ RUIDO / EVENTOS NEUTROS (Para o modelo ter onde "jogar" o lixo)
            "generic news or daily politics",
            "sports, entertainment or celebrity gossip"
        ]

        # 3. spaCy NER (Extração de Entidades) + EntityRuler
        log.info("A carregar spaCy NER (en_core_web_sm)...")
        self.nlp = spacy.load("en_core_web_sm")
        
        # Adiciona o Ruler ANTES do NER nativo para forçar as suas regras financeiras
        self.ruler = self.nlp.add_pipe("entity_ruler", before="ner")
        self._inject_market_entities()
        
        log.info("Motor NLP carregado e pronto para inferência.")

    def _inject_market_entities(self):
        """
        Injeta regras estritas (Bancos Centrais, Países do G10, etc.)
        Pode expandir isto para ler do seu unified-countries_main.json.
        """
        patterns = [
            # Bancos Centrais Tier 1
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "ecb"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "fed"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "federal"}, {"LOWER": "reserve"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "boj"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "boe"}]},
            # G10 e potências
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "usa"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "uk"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "japan"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "germany"}]},
        ]
        self.ruler.add_patterns(patterns)

    def extract_features(self, text: str) -> Dict[str, Any]:
        """
        Executa a pipeline completa (Sentimento, Contexto e Entidades).
        """
        if not text or len(text.strip()) < 5:
            return {"sentiment": "neutral", "sentiment_score": 0.0, "category": "generic", "entities": []}

        # Truncar para evitar peso computacional excessivo (500 chars é suficiente para o lead)
        target_text = text[:500]

        try:
            # 1. FinBERT
            sentiment_res = self.finbert(target_text)[0]
            
            # 2. DeBERTa
            context_res = self.classifier(target_text, self.categories)
            top_category = context_res['labels'][0]
            confidence = context_res['scores'][0]

            # 3. spaCy NER
            doc = self.nlp(target_text)
            entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]

            return {
                "sentiment": sentiment_res['label'], # 'positive', 'negative', 'neutral'
                "sentiment_score": sentiment_res['score'],
                "inferred_category": top_category,
                "category_confidence": confidence,
                "entities": entities
            }
        except Exception as e:
            log.error(f"Erro no Motor NLP: {e}")
            # Fallback seguro em caso de falha
            return {"sentiment": "neutral", "sentiment_score": 0.0, "inferred_category": "generic", "entities": []}