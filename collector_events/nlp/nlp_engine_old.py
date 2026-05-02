# services/collector_events/collector_events/processors/nlp_engine.py
"""
Motor NLP Offline (Local) para extração semântica em tempo real.
Utiliza modelos Small Language Models (SLMs) otimizados para CPU.
Agora inclui GLiNER para extração tática e filtro de Cisnes Brancos.
"""

import logging
import spacy
from typing import Dict, Any, List
from gliner import GLiNER  
from transformers import AutoTokenizer, pipeline

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
        # MoritzLaurer/deberta-v3-large-zeroshot-v2.0:
        # - Fine-tuned em MNLI + SNLI + ANLI + MultiNLI + VitaminC (datasets adversariais)
        # - Superior ao deberta-v3-small/base e ao facebook/bart-large-mnli em NLI benchmarks
        # - Melhor discriminação entre categorias semanticamente próximas (crítico para forex signals)
        # use_fast=False: ativa byte fallback do sentencepiece para texto multilíngue (árabe, russo, CJK)
        # Limite HARD de 512 tokens nas positional embeddings — sliding window implementada em _classify_with_sliding_window
        
        _ZERO_SHOT_MODEL = "MoritzLaurer/deberta-v3-base-zeroshot-v2.0"
        
        self._deberta_tokenizer = AutoTokenizer.from_pretrained(
            _ZERO_SHOT_MODEL,
            use_fast=True 
        )
        self.classifier = pipeline(
            "zero-shot-classification",
            model=_ZERO_SHOT_MODEL,
            tokenizer=self._deberta_tokenizer,
            device=-1
        )
        self._DEBERTA_CHUNK_TOKENS = 400   # margem de segurança abaixo do limite hard de 512
        self._DEBERTA_OVERLAP_TOKENS = 60  # overlap para não perder contexto entre chunks
        
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
            "localized accident or personal tragedy", 
            
            # EIXO MERCADO 
            "forex signal",
            "currency pair",
            "forex pair", 
            "financial ticker", 
            "currency mentioned",
            "central bank",              # Fed, ECB, BoE, Copom
            "economic indicator",        # NFP, CPI, GDP, Inflation, Payrolls
            "monetary policy signal",    # Hike, Cut, Pause, Hawkish, Dovish
            "geopolitical event",        # Sanctions, Conflict, Trade War, Embargo
            "political leader",          # President, Prime Minister, Treasury Secretary
            "commodity price action",    # Crude Oil, Gold, Brent, XAU, Bullion
            "fiat currency",             # USD, Greenback, EUR, JPY, Cable
            "market sentiment",          # Risk-off, Panic, Rally, Plunge, Sell-off
            "fiscal policy change",      # Taxes, Stimulus, Budget, Deficit
            "strategic infrastructure",  # Pipeline, Refinery, Port, Power Grid
            "institutional actor"        # IMF, World Bank, OPEC, WTO
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

    def _classify_with_sliding_window(self, text: str) -> dict:
        """
        Zero-shot classification sem truncação para textos arbitrariamente longos.

        Estratégia: sliding window de 400 tokens com overlap de 60 tokens.
        Agregação: max-score por label entre todos os chunks.
        Garante que sinais críticos no final do texto (ex: 'I'll attack you today!')
        não sejam perdidos por truncação.
        """
        tokens = self._deberta_tokenizer.encode(text, add_special_tokens=False)

        # Texto curto: inferência direta, sem overhead de chunking
        if len(tokens) <= self._DEBERTA_CHUNK_TOKENS:
            return self.classifier(text, self.categories, multi_label=True)

        # Gera chunks com overlap
        chunks = []
        step = self._DEBERTA_CHUNK_TOKENS - self._DEBERTA_OVERLAP_TOKENS
        for start in range(0, len(tokens), step):
            chunk_tokens = tokens[start : start + self._DEBERTA_CHUNK_TOKENS]
            chunk_text = self._deberta_tokenizer.decode(chunk_tokens, skip_special_tokens=True)
            if chunk_text.strip():
                chunks.append(chunk_text)

        # Classifica cada chunk independentemente
        chunk_results = [
            self.classifier(chunk, self.categories, multi_label=False)
            for chunk in chunks
        ]

        # Agrega: para cada label, guarda o score máximo visto em qualquer chunk.
        # Max-score é correto aqui porque qualquer chunk com sinal crítico
        # deve elevar a classificação final — não queremos média que dilua o sinal.
        label_max_scores: dict[str, float] = {}
        for result in chunk_results:
            for label, score in zip(result["labels"], result["scores"]):
                if score > label_max_scores.get(label, 0.0):
                    label_max_scores[label] = score

        sorted_pairs = sorted(label_max_scores.items(), key=lambda x: x[1], reverse=True)
        return {
            "labels": [label for label, _ in sorted_pairs],
            "scores": [score for _, score in sorted_pairs],
            "sequence": text,
            "chunks_processed": len(chunks),
        }

    def extract_features(self, text: str) -> Dict[str, Any]:
        if not text or len(text.strip()) < 5:
            return {
                "sentiment": "neutral",
                "sentiment_score": 0.0,
                "inferred_category": "generic",
                "category_confidence": 0.0,
                "entities": [],
                "gliner_graph": {}
            }

        try:
            # 1. FinBERT — truncation=True porque o modelo aceita no máximo 512 tokens
            # e sentimento financeiro é capturado bem no início do texto
            sentiment_res = self.finbert(text, truncation=True)[0]

            # 2. DeBERTa via sliding window — SEM truncação, processa texto completo
            # independente do tamanho (Telegram, RSS, páginas web, artigos longos)
            context_res = self._classify_with_sliding_window(text)
            top_category = context_res["labels"][0]
            confidence = context_res["scores"][0]

            # 3. spaCy NER — sem limite de tamanho nativo
            doc = self.nlp(text)
            entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]

            # 4. GLiNER — sem limite de tamanho nativo
            gliner_raw = self.gliner.predict_entities(text, self.gliner_labels)
            
            # Organiza os resultados do GLiNER num dicionário limpo para o EventProcessor
            gliner_graph = {label: [] for label in self.gliner_labels}
            for ent in gliner_raw:
                gliner_graph[ent["label"]].append(ent["text"])

            return {
                "sentiment": sentiment_res["label"],
                "sentiment_score": sentiment_res["score"],
                "inferred_category": top_category,
                "category_confidence": confidence,
                "entities": entities,
                "gliner_graph": gliner_graph,
                "chunks_processed": context_res.get("chunks_processed", 1),
            }
        except Exception as e:
            log.error(f"Erro no Motor NLP: {e}")
            return {
                "sentiment": "neutral",
                "sentiment_score": 0.0,
                "inferred_category": "generic",
                "category_confidence": 0.0,
                "entities": [],
                "gliner_graph": {}
            }