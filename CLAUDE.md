# collector_events

Coleta de dados fundamentais: notícias, calendário econômico, redes sociais e eventos geopolíticos que impactam o mercado forex.

## ⚠️ Status: conceitual / placeholders

Todos os módulos são placeholders ou implementações parciais. O único módulo com lógica substancial é `currency_strength_calculator.py`, que tem a arquitetura de scoring definida mas depende dos scrapers e NLP para funcionar.

### O que está implementado (com lógica real):
- `currency_strength_calculator.py` — scoring de força por moeda com 4 componentes ponderados, time decay exponencial, momentum e trend. Persiste em `CurrencyStrength` (PostgreSQL) e lê de `news_events` (MongoDB).

### O que são placeholders (apenas classe com pass/print):
- `sentiment_analyzer.py`, `entity_extractor.py`, `impact_classifier.py` — NLP pipeline
- `event_processor.py` — processador central de eventos
- `economic_calendar.py`, `news_scraper.py`, `twitter_scraper.py`, `instagram_scraper.py` — scrapers
- `sites.json` — apenas Investing.com como fonte de exemplo

## Visão de produto

Este serviço é planejado para ser o braço de inteligência fundamental do sistema, complementando a análise técnica das estratégias rule-based. O objetivo é coletar e processar em tempo real:

**Calendário econômico**: decisões de taxa de juros (Fed, BCE, BoE, BoJ), dados de emprego (NFP, unemployment), inflação (CPI, PPI), PIB, balança comercial, PMI. Fontes prioritárias: ForexFactory, Investing.com, Trading Economics, DailyFX.

**Notícias de alto impacto**: anúncios governamentais, políticas monetárias e fiscais, conflitos internacionais e geopolíticos, guerras, desastres naturais, sanções, crises bancárias — qualquer evento que mova o mercado na hora em que acontece. Fontes prioritárias: Reuters, Bloomberg, CNBC, Financial Times, ZeroHedge, e APIs como NewsAPI, Benzinga, Alpha Vantage News.

**Redes sociais**: posts de perfis estrategicamente selecionados no Twitter/X e Instagram — analistas de mercado, economistas, bancos centrais, líderes políticos, jornalistas financeiros. Foco em obtenção de inteligência para auxiliar decisões do sistema, não em volume de dados.

**Commodities e ações**: fontes especializadas em Petróleo (WTI, Brent), Ouro (XAU), e ações de empresas relevantes para correlações com forex. Fontes: OilPrice.com, Kitco, MarketWatch, Yahoo Finance API.

O diferencial é a **velocidade**: capturar a informação o mais rápido possível na medida em que os eventos acontecem, antes que o mercado precifique completamente.

## Estrutura

```
collector_events/
├── collector_events/
│   ├── nlp/                       # Pipeline de NLP (placeholders)
│   │   ├── sentiment_analyzer.py      # Análise de sentimento de texto
│   │   ├── entity_extractor.py        # Extração de entidades (moedas, países, etc)
│   │   └── impact_classifier.py       # Classificação de impacto (high/medium/low)
│   ├── processors/                # Processamento de eventos
│   │   ├── event_processor.py         # Processador central (placeholder)
│   │   └── currency_strength_calculator.py  # Scoring de força por moeda (parcial)
│   ├── scrapers/                  # Coleta de dados (placeholders)
│   │   ├── economic_calendar.py       # Calendário econômico
│   │   ├── news_scraper.py            # Notícias financeiras
│   │   ├── twitter_scraper.py         # Twitter/X
│   │   └── instagram_scraper.py       # Instagram
│   └── sources/
│       └── sites.json                 # Lista de fontes (apenas Investing.com por enquanto)
├── main.py
├── pyproject.toml
└── requirements.txt
```

## Módulos

### CurrencyStrengthCalculator (processors/currency_strength_calculator.py)
Único módulo com lógica substancial. Calcula score de força por moeda (-100 a +100) com 4 componentes ponderados:

- **Política monetária** (40%): hawkish = positivo, dovish = negativo
- **Dados econômicos** (30%): acima do esperado = positivo, abaixo = negativo
- **Geopolítica** (20%): safe havens (USD, JPY, CHF) beneficiadas em crises
- **Sentimento de mercado** (10%): agregado de sentiment NLP

Cada componente aplica **time decay exponencial** (eventos recentes pesam mais). Calcula momentum (taxa de mudança 24h) e determina trend (STRENGTHENING/WEAKENING/STABLE).

Persiste em `CurrencyStrength` (PostgreSQL via SQLAlchemy). Lê eventos de `news_events` (MongoDB). Confiança baseada no número de eventos disponíveis.

Usado pela estratégia `FundamentalDivergence` (signal_generator) para gerar sinais baseados em divergência de força entre moedas.

### NLP Pipeline (nlp/) — placeholders
Pipeline planejado: texto bruto → `EntityExtractor` (moedas, países, organizações) → `SentimentAnalyzer` (positivo/negativo/neutro, -1 a +1) → `ImpactClassifier` (high/medium/low, magnitude 0-1, direção).

### EventProcessor (processors/event_processor.py) — placeholder
Orquestrador central: recebe eventos dos scrapers, passa pelo pipeline NLP, classifica impacto por moeda, e persiste em MongoDB (`news_events`) e PostgreSQL (`EventImpact`).

### Scrapers (scrapers/) — placeholders
Todos retornam dados mock. A implementação real deve considerar: rate limiting, parsing robusto (BeautifulSoup/Scrapy), APIs quando disponíveis (NewsAPI, Twitter API v2), e publicação de eventos processados no MQ para consumo pelo EventProcessor.

## Integração com o sistema

O fluxo planejado:
```
Scrapers → MongoDB (news_events raw)
    ↓
EventProcessor + NLP pipeline
    ↓
PostgreSQL (EventImpact, CurrencyStrength)
    ↓
FundamentalDivergence strategy (via db_session em additional_data)
InterestRateDifferential strategy (via additional_data["interest_rates"])
```

## Dependências

- `forex_shared.models`: CurrencyStrength, EventImpact
- `forex_shared.database`: get_db (SessionLocal)
- `pymongo`: MongoClient para news_events
- `numpy`: time decay exponencial
- NLP (futuro): transformers, spacy, ou API externa (OpenAI, Claude)

## Prioridades de implementação sugeridas

1. `economic_calendar.py` — scraper de ForexFactory ou Investing.com (maior impacto imediato)
2. `news_scraper.py` — integração com NewsAPI ou Benzinga (notícias em tempo real)
3. `sentiment_analyzer.py` — pode usar API externa (Claude/OpenAI) inicialmente
4. `twitter_scraper.py` — Twitter API v2 com lista curada de perfis
5. `event_processor.py` — orquestração completa com publicação MQ
6. `entity_extractor.py` + `impact_classifier.py` — refinamento do pipeline NLP