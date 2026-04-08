# collector_events

Fundamental data collection: news, economic calendar, social media, and geopolitical events that impact the forex market.

## ⚠️ Status: conceptual / placeholders

Most modules are placeholders or partial implementations. The only module with substantial logic is `currency_strength_calculator.py`, which has the scoring architecture defined but depends on the scrapers and NLP pipeline to be fully useful.

### What’s implemented (real logic)

- `currency_strength_calculator.py` — currency strength scoring with 4 weighted components, exponential time decay, momentum, and trend. Persists to `CurrencyStrength` (PostgreSQL) and reads from `news_events` (MongoDB).

### What’s placeholder-only (just class with pass/print)

- `sentiment_analyzer.py`, `entity_extractor.py`, `impact_classifier.py` — NLP pipeline
- `event_processor.py` — central event processor
- `economic_calendar.py`, `news_scraper.py`, `twitter_scraper.py`, `instagram_scraper.py` — scrapers
- `sites.json` — only Investing.com as an example source

## Product vision

This service is planned to be the system’s fundamental intelligence arm, complementing the technical analysis from rule-based strategies. The goal is to collect and process, in real time:

**Economic calendar**: rate decisions (Fed, ECB, BoE, BoJ), labor data (NFP, unemployment), inflation (CPI, PPI), GDP, trade balance, PMI. Priority sources: ForexFactory, Investing.com, Trading Economics, DailyFX.

**High-impact news**: government announcements, monetary and fiscal policy, international and geopolitical conflicts, wars, natural disasters, sanctions, banking crises — anything that moves the market as it happens. Priority sources: Reuters, Bloomberg, CNBC, Financial Times, ZeroHedge, plus APIs like NewsAPI, Benzinga, Alpha Vantage News.

**Social media**: posts from a curated set of profiles on Twitter/X and Instagram — market analysts, economists, central banks, political leaders, financial journalists. Focus is on actionable intelligence to support system decisions, not on sheer volume.

**Commodities and equities**: specialized sources for Oil (WTI, Brent), Gold (XAU), and equities relevant to forex correlations. Sources: OilPrice.com, Kitco, MarketWatch, Yahoo Finance API.

The differentiator is **speed**: capturing information as quickly as possible as events unfold, before the market fully prices it in.

## Structure

```
collector_events/
├── collector_events/
│   ├── nlp/                       # NLP pipeline (placeholders)
│   │   ├── sentiment_analyzer.py      # Text sentiment analysis
│   │   ├── entity_extractor.py        # Entity extraction (currencies, countries, etc.)
│   │   └── impact_classifier.py       # Impact classification (high/medium/low)
│   ├── processors/                # Event processing
│   │   ├── event_processor.py         # Central processor (placeholder)
│   │   └── currency_strength_calculator.py  # Currency strength scoring (partial)
│   ├── scrapers/                  # Data collection (placeholders)
│   │   ├── economic_calendar.py       # Economic calendar
│   │   ├── news_scraper.py            # Financial news
│   │   ├── twitter_scraper.py         # Twitter/X
│   │   └── instagram_scraper.py       # Instagram
│   └── sources/
│       └── sites.json                 # Source list (only Investing.com for now)
├── main.py
├── pyproject.toml
└── requirements.txt
```

## Modules

### CurrencyStrengthCalculator (`processors/currency_strength_calculator.py`)

The only module with substantial logic. Computes a currency strength score (-100 to +100) with 4 weighted components:

- **Monetary policy** (40%): hawkish = positive, dovish = negative
- **Economic data** (30%): above expected = positive, below expected = negative
- **Geopolitics** (20%): safe havens (USD, JPY, CHF) benefit during crises
- **Market sentiment** (10%): aggregated NLP sentiment

Each component applies **exponential time decay** (recent events weigh more). It computes momentum (24h rate of change) and determines trend (STRENGTHENING/WEAKENING/STABLE).

Persists to `CurrencyStrength` (PostgreSQL via SQLAlchemy). Reads events from `news_events` (MongoDB). Confidence is based on the number of available events.

Used by the `FundamentalDivergence` strategy (signal_generator) to generate signals based on strength divergence between currencies.

### NLP Pipeline (`nlp/`) — placeholders

Planned pipeline: raw text → `EntityExtractor` (currencies, countries, organizations) → `SentimentAnalyzer` (positive/negative/neutral, -1 to +1) → `ImpactClassifier` (high/medium/low, magnitude 0–1, direction).

### EventProcessor (`processors/event_processor.py`) — placeholder

Central orchestrator: receives events from scrapers, runs the NLP pipeline, classifies impact by currency, and persists to MongoDB (`news_events`) and PostgreSQL (`EventImpact`).

### Scrapers (`scrapers/`) — placeholders

All scrapers currently return mock data. A real implementation must consider: rate limiting, robust parsing (BeautifulSoup/Scrapy), APIs when available (NewsAPI, Twitter API v2), and publishing processed events to MQ for consumption by EventProcessor.

## System integration

Planned flow:

```
Scrapers → MongoDB (news_events raw)
    ↓
EventProcessor + NLP pipeline
    ↓
PostgreSQL (EventImpact, CurrencyStrength)
    ↓
FundamentalDivergence strategy (via db_session in additional_data)
InterestRateDifferential strategy (via additional_data["interest_rates"])
```

## Dependencies

- `forex_shared.models`: `CurrencyStrength`, `EventImpact`
- `forex_shared.database`: `get_db` (`SessionLocal`)
- `pymongo`: MongoClient for `news_events`
- `numpy`: exponential time decay
- NLP (future): transformers, spacy, or an external API (OpenAI, Claude)

## Suggested implementation priorities

1. `economic_calendar.py` — ForexFactory or Investing.com scraper (highest immediate impact)
2. `news_scraper.py` — NewsAPI or Benzinga integration (real-time news)
3. `sentiment_analyzer.py` — can use an external API (Claude/OpenAI) initially
4. `twitter_scraper.py` — Twitter API v2 with curated profile list
5. `event_processor.py` — full orchestration with MQ publication
6. `entity_extractor.py` + `impact_classifier.py` — NLP pipeline refinement
