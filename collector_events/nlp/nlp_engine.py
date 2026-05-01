# services/collector_events/collector_events/processors/nlp_engine.py
"""
Motor NLP Offline (Local) para extração semântica em tempo real.
Utiliza modelos Small Language Models (SLMs) otimizados para CPU.
Agora inclui GLiNER para extração tática e filtro de Cisnes Brancos.
"""

import logging
import spacy
from typing import Dict, Any, List
from transformers import pipeline
from gliner import GLiNER  # <-- IMPORTAÇÃO DO GLINER

log = logging.getLogger("LocalNLPEngine")

class LocalNLPEngine:
    _instance = None

    @classmethod
    def get_instance(cls) -> 'LocalNLPEngine':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        log.info("A iniciar o carregamento dos modelos NLP offline...")
        
        # 1. FinBERT - Sentimento Financeiro
        self.finbert = pipeline("text-classification", model="ProsusAI/finbert", device=-1)
        
        # 2. DeBERTa - Classificação Zero-Shot
        self.classifier = pipeline("zero-shot-classification", model="cross-encoder/nli-deberta-v3-small", device=-1)
        self.categories = [
            "military drone or missile strike", "military attack or action", "declaration of war or armed conflict",
            "nuclear threat or radioactive incident", "troop mobilization or border skirmish",
            "military coup or government overthrow", "naval blockade or airspace closure",
            "intelligence agency report or espionage", "covert operation or state-sponsored assassination",
            "state-sponsored cyber attack or infrastructure hack", "major data breach or institutional ransomware",
            "central bank interest rate decision or monetary policy", "macroeconomic data release or inflation report",
            "currency intervention or severe devaluation", "sudden price spike or hyperinflation warning",
            "massive earthquake or tsunami", "catastrophic explosion or industrial disaster",
            "severe extreme weather or hurricane", "global pandemic or biological hazard",
            "stock market crash or massive sell-off", "commodity price surge or oil market disruption",
            "crypto market flash crash or major exchange hack", "institutional market downgrade or recession warning",
            "major hedge fund collapse or massive margin call", "insider trading scandal or massive financial fraud",
            "international economic sanctions or trade embargo", "critical supply chain disruption or port strike",
            "shipping route stress or maritime chokepoint closure", "strategic mineral or semiconductor shortage",
            "civil unrest, massive protests or violent riots", "terrorist attack or mass casualty event",
            "unprecedented global crisis or major disruptive anomaly", "sudden market shock or black swan event",
            "generic news or daily politics", "sports, entertainment or celebrity gossip"
        ]

        # 3. spaCy NER (Entidades Core)
        self.nlp = spacy.load("en_core_web_sm")
        self.ruler = self.nlp.add_pipe("entity_ruler", before="ner")
        self._inject_market_entities()

        # 4. GLiNER - Extração Tática e Filtro de Cisne Branco (Cruz Vermelha)
        log.info("A carregar GLiNER (urchade/gliner_small-v2.1)...")
        # Usa um modelo small para manter a latência baixa e não rebentar com a RAM no Ray
        self.gliner = GLiNER.from_pretrained("urchade/gliner_small-v2.1", load_tokenizer=True)
        
        self.gliner_labels = [
            # EIXO MILITAR
            "military actor", 
            "kinetic or military action", 
            "weapon or military vehicle", 
            
            # EIXO CRISE / MAGNITUDE (O Ouro do Forex)
            "macroeconomic infrastructure or strategic target", 
            
            # EIXO CISNE BRANCO / RUÍDO (O Lixo)
            "civilian vehicle or local infrastructure", 
            "localized accident or personal tragedy"
        ]
        
        log.info("Motor NLP carregado e pronto para inferência.")

    def _inject_market_entities(self):
        patterns = [
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "ecb"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "fed"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "federal"}, {"LOWER": "reserve"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "boj"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "boe"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "usa"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "uk"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "japan"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "germany"}]},
        ]
        self.ruler.add_patterns(patterns)

    def extract_features(self, text: str) -> Dict[str, Any]:
        if not text or len(text.strip()) < 5:
            return {"sentiment": "neutral", "sentiment_score": 0.0, "inferred_category": "generic", "entities": [], "gliner_graph": {}}

        import textwrap
        # Truncamento inteligente (não corta palavras no meio como text[:500])
        target_text = textwrap.shorten(text, width=500, placeholder="...")

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

            # 4. GLiNER (A Mágica Tática)
            gliner_raw = self.gliner.predict_entities(target_text, self.gliner_labels)
            
            # Organiza os resultados do GLiNER num dicionário limpo para o EventProcessor
            gliner_graph = {label: [] for label in self.gliner_labels}
            for ent in gliner_raw:
                gliner_graph[ent["label"]].append(ent["text"])

            return {
                "sentiment": sentiment_res['label'],
                "sentiment_score": sentiment_res['score'],
                "inferred_category": top_category,
                "category_confidence": confidence,
                "entities": entities,
                "gliner_graph": gliner_graph # <- NOVO OUTPUT
            }
        except Exception as e:
            log.error(f"Erro no Motor NLP: {e}")
            return {"sentiment": "neutral", "sentiment_score": 0.0, "inferred_category": "generic", "entities": [], "gliner_graph": {}}
        