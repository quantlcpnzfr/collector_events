"""
Motor NLP Offline (Local) para extração semântica em tempo real.
Utiliza modelos Small Language Models (SLMs) otimizados para CPU.
Inclui:
- FinBERT para sentimento financeiro
- DeBERTa/ModernBERT zero-shot com sliding window
- spaCy NER com EntityRuler customizado
- GLiNER com sliding window para extração tática

Notas importantes:
- O default preserva o comportamento funcional mais completo: labels_set=FULL.
- Para reduzir latência, use labels_set=MEDIUM ou labels_set=MINIMUM.
- O zero-shot NLI continua custando aproximadamente: chunks × candidate_labels.
"""

import logging
from enum import Enum
from time import perf_counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import spacy
from gliner import GLiNER
from transformers import AutoTokenizer, pipeline

log = logging.getLogger("LocalNLPEngine")


class LabelSet(str, Enum):
    """Modo de seleção de labels para o zero-shot classifier."""

    MINIMUM = "minimum"
    MEDIUM = "medium"
    FULL = "full"

    @classmethod
    def from_value(cls, value: Union["LabelSet", str, None]) -> "LabelSet":
        """
        Normaliza entrada externa.

        Aceita também o typo comum "minimun" para não quebrar configuração manual.
        """
        if value is None:
            return cls.FULL

        if isinstance(value, cls):
            return value

        normalized = str(value).strip().lower()
        aliases = {
            "min": cls.MINIMUM,
            "minimum": cls.MINIMUM,
            "minimun": cls.MINIMUM,
            "small": cls.MINIMUM,
            "medium": cls.MEDIUM,
            "med": cls.MEDIUM,
            "full": cls.FULL,
            "all": cls.FULL,
            "complete": cls.FULL,
        }

        if normalized not in aliases:
            raise ValueError(
                f"labels_set inválido: {value!r}. Use: minimum, medium ou full."
            )

        return aliases[normalized]


class LocalNLPEngine:
    _instance: Optional["LocalNLPEngine"] = None

    # Modelo mais leve que o large original.
    # Alternativas úteis:
    # - "MoritzLaurer/deberta-v3-large-zeroshot-v2.0"      # máxima qualidade, mais lento
    # - "MoritzLaurer/deberta-v3-base-zeroshot-v2.0"       # bom equilíbrio
    # - "MoritzLaurer/ModernBERT-base-zeroshot-v2.0"       # candidato mais rápido para produção
    # - "MoritzLaurer/deberta-v3-xsmall-zeroshot-v1.1-all-33" # muito rápido, menor qualidade
    DEFAULT_ZERO_SHOT_MODEL = "MoritzLaurer/deberta-v3-base-zeroshot-v2.0"
    DEFAULT_FINBERT_MODEL = "ProsusAI/finbert"
    DEFAULT_GLINER_MODEL = "urchade/gliner_small-v2.1"
    DEFAULT_SPACY_MODEL = "en_core_web_sm"

    ZERO_SHOT_HYPOTHESIS_TEMPLATE = (
        "This market-relevant news report is about {}."
    )

    @classmethod
    def get_instance(cls, *args: Any, **kwargs: Any) -> "LocalNLPEngine":
        """
        Singleton compatível com a versão antiga.

        Se a instância já existir, argumentos novos são ignorados para evitar recarregar
        modelos pesados sem querer.
        """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        elif args or kwargs:
            log.warning(
                "LocalNLPEngine já inicializado; argumentos de get_instance foram ignorados."
            )
        return cls._instance

    def __init__(
        self,
        *,
        zero_shot_model: str = DEFAULT_ZERO_SHOT_MODEL,
        finbert_model: str = DEFAULT_FINBERT_MODEL,
        gliner_model: str = DEFAULT_GLINER_MODEL,
        spacy_model: str = DEFAULT_SPACY_MODEL,
        labels_set: Union[LabelSet, str] = LabelSet.FULL,
        device: int = -1,
        use_fast_tokenizer: bool = False,
        local_files_only: bool = False,
        deberta_chunk_tokens: int = 400,
        deberta_overlap_tokens: int = 60,
        gliner_chunk_tokens: int = 320,
        gliner_overlap_tokens: int = 50,
        enable_timings: bool = True,
    ):
        self.zero_shot_model = zero_shot_model
        self.finbert_model = finbert_model
        self.gliner_model = gliner_model
        self.spacy_model = spacy_model
        self.labels_set = LabelSet.from_value(labels_set)
        self.device = device
        self.local_files_only = local_files_only
        self.enable_timings = enable_timings

        self._DEBERTA_CHUNK_TOKENS = deberta_chunk_tokens
        self._DEBERTA_OVERLAP_TOKENS = deberta_overlap_tokens
        self._GLINER_CHUNK_TOKENS = gliner_chunk_tokens
        self._GLINER_OVERLAP_TOKENS = gliner_overlap_tokens

        if self._DEBERTA_OVERLAP_TOKENS >= self._DEBERTA_CHUNK_TOKENS:
            raise ValueError("DEBERTA overlap precisa ser menor que chunk size.")

        if self._GLINER_OVERLAP_TOKENS >= self._GLINER_CHUNK_TOKENS:
            raise ValueError("GLiNER overlap precisa ser menor que chunk size.")

        log.info(
            "A iniciar o carregamento dos modelos NLP offline/local... "
            "zero_shot=%s | labels_set=%s | device=%s | local_files_only=%s",
            self.zero_shot_model,
            self.labels_set.value,
            self.device,
            self.local_files_only,
        )
        
        finbert_pipeline_kwargs = {
            "task": "text-classification",
            "model": self.finbert_model,
            "device": self.device,
        }
        
        if self.local_files_only:
            finbert_pipeline_kwargs["model_kwargs"] = {
                "local_files_only": True,
            }

        # 1. FinBERT - Sentimento Financeiro
        self.finbert = pipeline(**finbert_pipeline_kwargs)
        

        # 2. Zero-shot classifier - DeBERTa/ModernBERT
        # use_fast_tokenizer=True reduz overhead quando o modelo tiver tokenizer fast compatível.
        # Para casos específicos de fallback multilingual legado, pode ser setado como False.
        self._deberta_tokenizer = AutoTokenizer.from_pretrained(
            self.zero_shot_model,
            use_fast=use_fast_tokenizer,
            local_files_only=self.local_files_only,
        )

        zero_shot_pipeline_kwargs = {
            "task": "zero-shot-classification",
            "model": self.zero_shot_model,
            "tokenizer": self._deberta_tokenizer,
            "device": self.device,
        }
        
        if self.local_files_only:
            zero_shot_pipeline_kwargs["model_kwargs"] = {
                "local_files_only": True,
            }

        self.classifier = pipeline(**zero_shot_pipeline_kwargs)

        self.categories = [
            "military drone or missile strike",
            "military attack or action",
            "declaration of war or armed conflict",
            "nuclear threat or radioactive incident",
            "troop mobilization or border skirmish",
            "military coup or government overthrow",
            "naval blockade or airspace closure",
            "intelligence agency report or espionage",
            "covert operation or state-sponsored assassination",
            "state-sponsored cyber attack or infrastructure hack",
            "major data breach or institutional ransomware",
            "central bank interest rate decision or monetary policy",
            "macroeconomic data release or inflation report",
            "currency intervention or severe devaluation",
            "sudden price spike or hyperinflation warning",
            "massive earthquake or tsunami",
            "catastrophic explosion or industrial disaster",
            "severe extreme weather or hurricane",
            "global pandemic or biological hazard",
            "stock market crash or massive sell-off",
            "commodity price surge or oil market disruption",
            "crypto market flash crash or major exchange hack",
            "institutional market downgrade or recession warning",
            "major hedge fund collapse or massive margin call",
            "insider trading scandal or massive financial fraud",
            "international economic sanctions or trade embargo",
            "critical supply chain disruption or port strike",
            "shipping route stress or maritime chokepoint closure",
            "strategic mineral or semiconductor shortage",
            "civil unrest, massive protests or violent riots",
            "terrorist attack or mass casualty event",
            "unprecedented global crisis or major disruptive anomaly",
            "sudden market shock or black swan event",
            "generic news or daily politics",
            "sports, entertainment or celebrity gossip",
        ]

        self._category_set = set(self.categories)

        # 3. spaCy NER (Entidades Core)
        # Mantido completo porque você sinalizou que está rápido.
        self.nlp = spacy.load(self.spacy_model)
        self.ruler = self.nlp.add_pipe("entity_ruler", before="ner")
        self._inject_market_entities()

        # 4. GLiNER - Extração Tática e Filtro de Cisne Branco
        log.info("A carregar GLiNER (%s)...", self.gliner_model)
        self.gliner = GLiNER.from_pretrained(
            self.gliner_model,
            load_tokenizer=True,
            local_files_only=self.local_files_only,
        )

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
            "central bank",
            "economic indicator",
            "monetary policy signal",
            "geopolitical event",
            "political leader",
            "commodity price action",
            "fiat currency",
            "market sentiment",
            "fiscal policy change",
            "strategic infrastructure",
            "institutional actor",
        ]

        log.info("Motor NLP carregado e pronto para inferência.")

    def _inject_market_entities(self) -> None:
        patterns = [
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "ecb"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "fed"}]},
            {
                "label": "CENTRAL_BANK",
                "pattern": [{"LOWER": "federal"}, {"LOWER": "reserve"}],
            },
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "boj"}]},
            {"label": "CENTRAL_BANK", "pattern": [{"LOWER": "boe"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "usa"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "uk"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "japan"}]},
            {"label": "G10_COUNTRY", "pattern": [{"LOWER": "germany"}]},
            {"label": "COMMODITY", "pattern": [{"LOWER": "gold"}]},
            {"label": "COMMODITY", "pattern": [{"LOWER": "oil"}]},
            {"label": "COMMODITY", "pattern": [{"LOWER": "brent"}]},
            {"label": "COMMODITY", "pattern": [{"LOWER": "wti"}]},
            {"label": "FOREX_PAIR", "pattern": [{"TEXT": {"REGEX": "^[A-Z]{3}/[A-Z]{3}$"}}]},
            {"label": "FOREX_PAIR", "pattern": [{"TEXT": {"REGEX": "^[A-Z]{6}$"}}]},
        ]
        self.ruler.add_patterns(patterns)

    def _empty_response(self) -> Dict[str, Any]:
        return {
            "sentiment": "neutral",
            "sentiment_score": 0.0,
            "inferred_category": "generic",
            "category_confidence": 0.0,
            "entities": [],
            "gliner_graph": {},
        }

    def _unique_keep_order(self, values: Iterable[str]) -> List[str]:
        seen = set()
        unique_values: List[str] = []

        for value in values:
            if value in self._category_set and value not in seen:
                unique_values.append(value)
                seen.add(value)

        return unique_values

    def _preselect_categories(self, text: str) -> List[str]:
        """
        Pré-seleciona labels para reduzir custo do zero-shot.

        FULL: preserva o comportamento completo e usa todas as categorias.
        MEDIUM: usa roteamento por palavras-chave, mantendo boa cobertura.
        MINIMUM: usa roteamento agressivo, menor latência e menor recall.
        """
        if self.labels_set == LabelSet.FULL:
            return list(self.categories)

        t = f" {text.lower()} "
        selected: List[str] = []

        def has_any(terms: Sequence[str]) -> bool:
            return any(term in t for term in terms)

        military_terms = [
            "missile",
            "drone",
            "airstrike",
            "strike",
            "attack",
            "troop",
            "troops",
            "border",
            "war",
            "military",
            "army",
            "navy",
            "blockade",
            "invasion",
            "shelling",
            "bombardment",
            "rocket",
            "fighter jet",
            "airspace",
        ]

        nuclear_terms = [
            "nuclear",
            "radioactive",
            "radiation",
            "uranium",
            "atomic",
            "reactor",
            "dirty bomb",
        ]

        coup_terms = [
            "coup",
            "junta",
            "overthrow",
            "martial law",
            "military takeover",
        ]

        intelligence_terms = [
            "intelligence agency",
            "spy",
            "espionage",
            "covert",
            "assassination",
            "sabotage",
            "cia",
            "mi6",
            "mossad",
            "fsb",
        ]

        cyber_terms = [
            "cyber",
            "hack",
            "hacked",
            "ransomware",
            "malware",
            "data breach",
            "ddos",
            "infrastructure hack",
        ]

        macro_terms = [
            "fed",
            "federal reserve",
            "ecb",
            "boj",
            "boe",
            "central bank",
            "interest rate",
            "rate cut",
            "rate hike",
            "monetary policy",
            "inflation",
            "cpi",
            "ppi",
            "nfp",
            "nonfarm payroll",
            "payrolls",
            "gdp",
            "recession",
            "unemployment",
        ]

        currency_terms = [
            "forex",
            "fx",
            "currency",
            "devaluation",
            "intervention",
            "usd",
            "eur",
            "jpy",
            "gbp",
            "aud",
            "cad",
            "chf",
            "eur/usd",
            "gbp/usd",
            "usd/jpy",
            "xau/usd",
        ]

        market_terms = [
            "stock market",
            "sell-off",
            "crash",
            "flash crash",
            "margin call",
            "hedge fund",
            "downgrade",
            "fraud",
            "insider trading",
            "crypto",
            "exchange hack",
            "bitcoin",
            "btc",
            "ethereum",
            "eth",
        ]

        commodity_terms = [
            "oil",
            "brent",
            "wti",
            "crude",
            "gold",
            "xau",
            "commodity",
            "gas price",
            "natural gas",
            "opec",
        ]

        sanctions_terms = [
            "sanction",
            "sanctions",
            "embargo",
            "tariff",
            "trade war",
            "export ban",
            "import ban",
        ]

        supply_terms = [
            "supply chain",
            "port strike",
            "shipping route",
            "chokepoint",
            "semiconductor",
            "strategic mineral",
            "rare earth",
            "pipeline",
            "refinery",
            "power grid",
            "canal",
            "suez",
            "panama canal",
            "strait of hormuz",
            "red sea",
        ]

        disaster_terms = [
            "earthquake",
            "tsunami",
            "hurricane",
            "typhoon",
            "cyclone",
            "flood",
            "wildfire",
            "eruption",
            "explosion",
            "industrial disaster",
            "pandemic",
            "biological hazard",
            "outbreak",
        ]

        unrest_terms = [
            "protest",
            "protests",
            "riot",
            "riots",
            "civil unrest",
            "terrorist",
            "terror attack",
            "mass casualty",
            "shooting",
            "bombing",
        ]

        noise_terms = [
            "celebrity",
            "sports",
            "football",
            "soccer",
            "basketball",
            "entertainment",
            "movie",
            "music",
            "gossip",
        ]

        if has_any(military_terms):
            selected.extend(
                [
                    "military drone or missile strike",
                    "military attack or action",
                    "declaration of war or armed conflict",
                    "troop mobilization or border skirmish",
                    "naval blockade or airspace closure",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(nuclear_terms):
            selected.extend(
                [
                    "nuclear threat or radioactive incident",
                    "unprecedented global crisis or major disruptive anomaly",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(coup_terms):
            selected.extend(
                [
                    "military coup or government overthrow",
                    "civil unrest, massive protests or violent riots",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(intelligence_terms):
            selected.extend(
                [
                    "intelligence agency report or espionage",
                    "covert operation or state-sponsored assassination",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(cyber_terms):
            selected.extend(
                [
                    "state-sponsored cyber attack or infrastructure hack",
                    "major data breach or institutional ransomware",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(macro_terms):
            selected.extend(
                [
                    "central bank interest rate decision or monetary policy",
                    "macroeconomic data release or inflation report",
                    "institutional market downgrade or recession warning",
                    "stock market crash or massive sell-off",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(currency_terms):
            selected.extend(
                [
                    "currency intervention or severe devaluation",
                    "central bank interest rate decision or monetary policy",
                    "macroeconomic data release or inflation report",
                    "sudden price spike or hyperinflation warning",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(market_terms):
            selected.extend(
                [
                    "stock market crash or massive sell-off",
                    "crypto market flash crash or major exchange hack",
                    "institutional market downgrade or recession warning",
                    "major hedge fund collapse or massive margin call",
                    "insider trading scandal or massive financial fraud",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(commodity_terms):
            selected.extend(
                [
                    "commodity price surge or oil market disruption",
                    "sudden price spike or hyperinflation warning",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(sanctions_terms):
            selected.extend(
                [
                    "international economic sanctions or trade embargo",
                    "critical supply chain disruption or port strike",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(supply_terms):
            selected.extend(
                [
                    "critical supply chain disruption or port strike",
                    "shipping route stress or maritime chokepoint closure",
                    "strategic mineral or semiconductor shortage",
                    "commodity price surge or oil market disruption",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(disaster_terms):
            selected.extend(
                [
                    "massive earthquake or tsunami",
                    "catastrophic explosion or industrial disaster",
                    "severe extreme weather or hurricane",
                    "global pandemic or biological hazard",
                    "unprecedented global crisis or major disruptive anomaly",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(unrest_terms):
            selected.extend(
                [
                    "civil unrest, massive protests or violent riots",
                    "terrorist attack or mass casualty event",
                    "sudden market shock or black swan event",
                ]
            )

        if has_any(noise_terms):
            selected.extend(
                [
                    "sports, entertainment or celebrity gossip",
                    "generic news or daily politics",
                ]
            )

        # Fallbacks obrigatórios para evitar lista vazia e preservar classificação de ruído.
        selected.extend(
            [
                "generic news or daily politics",
                "sports, entertainment or celebrity gossip",
            ]
        )

        if self.labels_set == LabelSet.MEDIUM:
            # MEDIUM mantém mais recall, com algumas labels críticas sempre disponíveis.
            selected.extend(
                [
                    "unprecedented global crisis or major disruptive anomaly",
                    "sudden market shock or black swan event",
                    "central bank interest rate decision or monetary policy",
                    "macroeconomic data release or inflation report",
                    "commodity price surge or oil market disruption",
                    "international economic sanctions or trade embargo",
                ]
            )
            max_labels = 18
        else:
            # MINIMUM é mais agressivo: baixa latência, menor cobertura.
            selected.extend(
                [
                    "sudden market shock or black swan event",
                    "generic news or daily politics",
                ]
            )
            max_labels = 8

        return self._unique_keep_order(selected)[:max_labels]

    def _encode_deberta_tokens(self, text: str) -> List[int]:
        """
        Tokenização única para decisão de tamanho e criação de chunks.

        Observação: o pipeline zero-shot ainda tokeniza internamente os pares NLI
        (texto + hipótese). Aqui evitamos redundância no nosso processo de chunking.
        """
        return self._deberta_tokenizer.encode(text, add_special_tokens=False)

    def _decode_deberta_tokens(self, tokens: Sequence[int]) -> str:
        return self._deberta_tokenizer.decode(tokens, skip_special_tokens=True).strip()

    def _build_token_chunks(
        self,
        tokens: Sequence[int],
        *,
        chunk_tokens: int,
        overlap_tokens: int,
    ) -> List[List[int]]:
        if not tokens:
            return []

        step = chunk_tokens - overlap_tokens
        chunks: List[List[int]] = []

        for start in range(0, len(tokens), step):
            chunk = list(tokens[start : start + chunk_tokens])
            if chunk:
                chunks.append(chunk)

            if start + chunk_tokens >= len(tokens):
                break

        return chunks

    def _build_text_chunks_from_tokens(
        self,
        tokens: Sequence[int],
        *,
        chunk_tokens: int,
        overlap_tokens: int,
    ) -> List[str]:
        token_chunks = self._build_token_chunks(
            tokens,
            chunk_tokens=chunk_tokens,
            overlap_tokens=overlap_tokens,
        )

        chunks: List[str] = []
        for chunk in token_chunks:
            chunk_text = self._decode_deberta_tokens(chunk)
            if chunk_text:
                chunks.append(chunk_text)

        return chunks

    def _classify_with_sliding_window(self, text: str) -> Dict[str, Any]:
        """
        Zero-shot classification sem truncação para textos arbitrariamente longos.

        Estratégia:
        - Tokeniza uma vez para medir/chunkar.
        - Usa labels conforme labels_set: FULL, MEDIUM ou MINIMUM.
        - Usa multi_label=True, porque eventos de mercado/geopolítica não são exclusivos.
        - Agrega max-score por label entre chunks.
        """
        tokens = self._encode_deberta_tokens(text)
        candidate_labels = self._preselect_categories(text)

        if not candidate_labels:
            candidate_labels = ["generic news or daily politics"]

        if len(tokens) <= self._DEBERTA_CHUNK_TOKENS:
            result = self.classifier(
                text,
                candidate_labels,
                multi_label=True,
                hypothesis_template=self.ZERO_SHOT_HYPOTHESIS_TEMPLATE,
            )
            result["chunks_processed"] = 1
            result["candidate_labels_used"] = len(candidate_labels)
            result["labels_set"] = self.labels_set.value
            return result

        chunks = self._build_text_chunks_from_tokens(
            tokens,
            chunk_tokens=self._DEBERTA_CHUNK_TOKENS,
            overlap_tokens=self._DEBERTA_OVERLAP_TOKENS,
        )

        chunk_results = [
            self.classifier(
                chunk,
                candidate_labels,
                multi_label=True,
                hypothesis_template=self.ZERO_SHOT_HYPOTHESIS_TEMPLATE,
            )
            for chunk in chunks
        ]

        # Agrega: para cada label, guarda o score máximo visto em qualquer chunk.
        # Max-score preserva sinais críticos localizados no final/meio de textos longos.
        label_max_scores: Dict[str, float] = {}
        for result in chunk_results:
            for label, score in zip(result["labels"], result["scores"]):
                score_f = float(score)
                if score_f > label_max_scores.get(label, 0.0):
                    label_max_scores[label] = score_f

        sorted_pairs = sorted(
            label_max_scores.items(), key=lambda item: item[1], reverse=True
        )

        return {
            "labels": [label for label, _ in sorted_pairs],
            "scores": [score for _, score in sorted_pairs],
            "sequence": text,
            "chunks_processed": len(chunks),
            "candidate_labels_used": len(candidate_labels),
            "labels_set": self.labels_set.value,
        }

    def _predict_gliner_with_sliding_window(self, text: str) -> Tuple[List[Dict[str, Any]], int]:
        """
        GLiNER com sliding window.

        O GLiNER small trabalha melhor com janela menor. Para manter robustez sem
        depender de atributo interno do GLiNER, usamos o tokenizer do zero-shot
        apenas para segmentar texto em pedaços estáveis. A inferência GLiNER em si
        continua usando o tokenizer interno do próprio GLiNER.
        """
        tokens = self._encode_deberta_tokens(text)

        if len(tokens) <= self._GLINER_CHUNK_TOKENS:
            raw = self.gliner.predict_entities(text, self.gliner_labels)
            return list(raw or []), 1

        chunks = self._build_text_chunks_from_tokens(
            tokens,
            chunk_tokens=self._GLINER_CHUNK_TOKENS,
            overlap_tokens=self._GLINER_OVERLAP_TOKENS,
        )

        all_entities: List[Dict[str, Any]] = []
        seen = set()

        for chunk in chunks:
            raw_entities = self.gliner.predict_entities(chunk, self.gliner_labels) or []

            for ent in raw_entities:
                label = str(ent.get("label", "")).strip()
                ent_text = str(ent.get("text", "")).strip()

                if not label or not ent_text:
                    continue

                # Dedup por label/texto normalizado. Offsets em chunk não são globais.
                key = (label.lower(), ent_text.lower())
                if key in seen:
                    continue

                seen.add(key)
                all_entities.append(dict(ent))

        return all_entities, len(chunks)

    def _build_gliner_graph(self, gliner_raw: Sequence[Dict[str, Any]]) -> Dict[str, List[str]]:
        gliner_graph: Dict[str, List[str]] = {label: [] for label in self.gliner_labels}
        seen_by_label: Dict[str, set] = {label: set() for label in self.gliner_labels}

        for ent in gliner_raw:
            label = ent.get("label")
            ent_text = ent.get("text")

            if label not in gliner_graph or not ent_text:
                continue

            ent_text = str(ent_text).strip()
            key = ent_text.lower()

            if key in seen_by_label[label]:
                continue

            seen_by_label[label].add(key)
            gliner_graph[label].append(ent_text)

        return gliner_graph

    def extract_features(self, text: str) -> Dict[str, Any]:
        if not text or len(text.strip()) < 5:
            return self._empty_response()

        timings: Dict[str, float] = {}

        try:
            # 1. FinBERT — truncation=True porque o modelo aceita no máximo 512 tokens.
            t0 = perf_counter()
            sentiment_res = self.finbert(text, truncation=True)[0]
            timings["finbert"] = perf_counter() - t0

            # 2. Zero-shot via sliding window — sem truncar o texto completo.
            t0 = perf_counter()
            context_res = self._classify_with_sliding_window(text)
            timings["zero_shot"] = perf_counter() - t0

            labels = context_res.get("labels") or ["generic news or daily politics"]
            scores = context_res.get("scores") or [0.0]
            top_category = labels[0]
            confidence = float(scores[0])

            # 3. spaCy NER — mantido completo porque está rápido no teu cenário.
            t0 = perf_counter()
            doc = self.nlp(text)
            entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]
            timings["spacy"] = perf_counter() - t0

            # 4. GLiNER com sliding window para não perder entidades no final do texto.
            t0 = perf_counter()
            gliner_raw, gliner_chunks = self._predict_gliner_with_sliding_window(text)
            gliner_graph = self._build_gliner_graph(gliner_raw)
            timings["gliner"] = perf_counter() - t0

            chunks_processed = int(context_res.get("chunks_processed", 1))
            candidate_labels_used = int(
                context_res.get("candidate_labels_used", len(self.categories))
            )

            if self.enable_timings:
                log.info(
                    "NLP timings | zero_shot_model=%s | finbert=%.3fs | zero_shot=%.3fs | spacy=%.3fs | "
                    "gliner=%.3fs | zero_shot_chunks=%s | gliner_chunks=%s | "
                    "candidate_labels=%s | labels_set=%s | top=%s | conf=%.4f",
                    self.zero_shot_model,
                    timings["finbert"],
                    timings["zero_shot"],
                    timings["spacy"],
                    timings["gliner"],
                    chunks_processed,
                    gliner_chunks,
                    candidate_labels_used,
                    self.labels_set.value,
                    top_category,
                    confidence,
                )

            return {
                "sentiment": sentiment_res["label"],
                "sentiment_score": float(sentiment_res["score"]),
                "inferred_category": top_category,
                "category_confidence": confidence,
                "entities": entities,
                "gliner_graph": gliner_graph,
                "chunks_processed": chunks_processed,
                "gliner_chunks_processed": gliner_chunks,
                "candidate_labels_used": candidate_labels_used,
                "labels_set": self.labels_set.value,
                "nlp_timings": timings,
                # Campos extras úteis para debug/observabilidade sem quebrar os existentes.
                "zero_shot_labels": labels,
                "zero_shot_scores": [float(score) for score in scores],
                "zero_shot_model": self.zero_shot_model
            }

        except Exception as e:
            log.exception("Erro no Motor NLP: %s", e)
            return self._empty_response()
