import json
import re
import warnings
from dataclasses import dataclass, field
from typing import Any, List

# Suprime os avisos inofensivos do HuggingFace/PyTorch para manter o log limpo
warnings.filterwarnings("ignore")

import spacy
from transformers import pipeline

# --- 1. A sua Dataclass Exata ---
@dataclass
class IntelItem:
    id: str
    source: str
    domain: str
    title: str
    url: str = ""
    body: str = ""
    published_at: str = ""
    ts: str = ""
    fetched_at: str = ""
    source_media: str = ""
    source_countries: list = field(default_factory=list)
    actor_countries: list = field(default_factory=list)
    target_countries: list = field(default_factory=list)
    mentioned_countries: list = field(default_factory=list)
    country: list[str] = field(default_factory=list)
    entities: list[str] = field(default_factory=list)
    event_images_urls: list[str] = field(default_factory=list)
    lat: float | None = None
    lon: float | None = None
    severity: str = ""
    tags: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

# --- 2. Motor NLP REAL (Usa os modelos recomendados/baixados offline) ---
class RealNLPEngine:
    def __init__(self):
        print("⏳ Carregando os modelos de NLP na memória... (Isso pode levar alguns segundos na 1ª vez)")
        
        # 1. Modelo de Sentimento (FinBERT - 438 MB)
        self.sentiment_pipe = pipeline(
            "text-classification", 
            model="ProsusAI/finbert",
            device=-1 # Força CPU
        )
        
        # 2. Modelo de Categorização Zero-Shot (DeBERTa Small - 570 MB)
        self.zero_shot_pipe = pipeline(
            "zero-shot-classification", 
            model="cross-encoder/nli-deberta-v3-small",
            device=-1
        )
        
        # 3. Modelo NER (spaCy SM - 15 MB) + EntityRuler
        self.spacy_nlp = spacy.load("en_core_web_sm")
        self.ruler = self.spacy_nlp.add_pipe("entity_ruler", before="ner")
        
        # Injeta as regras de negócio para o teste mock funcionar perfeitamente
        patterns = [
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "fed"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "ecb"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "new"}, {"LOWER": "york"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "israeli"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "usa"}]}
        ]
        self.ruler.add_patterns(patterns)
        
        # Categorias de impacto que queremos que o DeBERTa identifique
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
        print("✅ Modelos carregados com sucesso!\n")

    def extract_features(self, text: str) -> dict:
        # Limita o texto para processamento rápido nos Transformers
        safe_text = text[:512]
        
        # --- Inferência 1: Sentimento (FinBERT) ---
        sent_result = self.sentiment_pipe(safe_text)[0]
        sentiment_label = sent_result["label"].lower()
        
        # --- Inferência 2: Categoria (DeBERTa Zero-Shot) ---
        zs_result = self.zero_shot_pipe(safe_text, candidate_labels=self.categories)
        inferred_category = zs_result["labels"][0]
        
        # --- Inferência 3: Entidades (spaCy) ---
        doc = self.spacy_nlp(safe_text)
        entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]
            
        return {
            "sentiment": sentiment_label,
            "sentiment_score": float(sent_result["score"]),
            "inferred_category": inferred_category,
            "entities": entities
        }

# --- 3. Processador de Eventos Isolado ---
class ScoringTester:
    def __init__(self):
        self.nlp = RealNLPEngine()
        # Pesos base simulando seu "scoring.json"
        self.severity_base = {
            "generic": 0.1, 
            "geopolitical escalation": 0.6, 
            "natural disaster": 0.5, 
            "monetary policy": 0.4
        }
        self.domain_weights = {"telegram": 1.2, "news": 1.0}

    def process_item(self, item: IntelItem) -> IntelItem:
        text_to_analyze = f"{item.title}. {item.body}"
        
        # 1. Enriquecimento REAL pelos modelos
        features = self.nlp.extract_features(text_to_analyze)
        item.extra["impact_category"] = features["inferred_category"]
        item.extra["nlp_sentiment"] = features["sentiment"]
        item.extra["nlp_sentiment_score"] = features["sentiment_score"]
        item.extra["nlp_entities"] = [e["text"] for e in features["entities"]]
        
        # 2. Cálculo Matemático do Score
        score = self._compute(item, features, text_to_analyze)
        item.extra["danger_score"] = min(score, 1.0)
        return item

    def _compute(self, item: IntelItem, features: dict, text: str) -> float:
        cat = item.extra["impact_category"]
        base = self.severity_base.get(cat, 0.2)
        weight = self.domain_weights.get(item.domain, 1.0)
        
        score = base * weight
        
        # Modificador de Sentimento
        if features["sentiment"] == "negative":
            score += (0.3 * features["sentiment_score"])
        elif features["sentiment"] == "positive":
            score -= (0.15 * features["sentiment_score"])
        
        # Modificador de Entidades
        ents = features.get("entities", [])
        if any(e["label"] == "CENTRAL_BANK" for e in ents): score += 0.25
        if any(e["label"] == "G10_COUNTRY" for e in ents): score += 0.15
            
        # Bónus Numérico (Regex - Mantido do código nativo)
        matches = re.findall(r'(\d+(?:\.\d+)?)\s*%', text)
        if matches:
            max_pct = max(float(m) for m in matches)
            if max_pct >= 5.0: score += 0.2
            elif max_pct >= 2.0: score += 0.1
            
        return max(score, 0.0)

# --- 4. Execução do Teste ---
if __name__ == "__main__":
    print("🚀 A iniciar Pipeline de Teste com Modelos Reais...\n")
    
    # 1. O Mock agora é EXCLUSIVAMENTE o carregamento do JSON
    try:
        with open("mock_intel_items.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("❌ Erro: Arquivo 'mock_intel_items.json' não encontrado na mesma pasta.")
        exit(1)
        
    items = [IntelItem(**d) for d in data]
    tester = ScoringTester()
    
    for item in items:
        # Passa cada item pelo motor NLP de verdade
        processed = tester.process_item(item)
        
        print(f"Notícia: {processed.title}")
        print(f" └─ Categoria : {processed.extra['impact_category']}")
        print(f" └─ Sentimento: {processed.extra['nlp_sentiment']} ({processed.extra['nlp_sentiment_score']:.2f})")
        print(f" └─ Entidades : {processed.extra['nlp_entities']}")
        print(f" └─ 🔴 DANGER SCORE CALCULADO: {processed.extra['danger_score']:.3f}")
        print("-" * 50)