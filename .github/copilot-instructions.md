# collector_events — Copilot Instructions

Collects fundamental data: news, economic calendar, social media, and geopolitical events impacting the forex market.

## Status: Conceptual / Placeholders
Most modules are placeholders. Only `currency_strength_calculator.py` has substantial logic.

**Implemented:** `currency_strength_calculator.py`
**Placeholders:** `sentiment_analyzer.py`, `entity_extractor.py`, `impact_classifier.py`, `event_processor.py`, all scrapers

## Key Module

### CurrencyStrengthCalculator (`processors/currency_strength_calculator.py`)
Calculates currency strength score (-100 to +100) with 4 weighted components:
- **Monetary policy** (40%): hawkish = positive, dovish = negative
- **Economic data** (30%): above expected = positive, below = negative
- **Geopolitics** (20%): safe havens (USD, JPY, CHF) benefit in crises
- **Market sentiment** (10%): aggregated NLP sentiment

Each component applies **exponential time decay** (recent events weigh more). Calculates momentum (24h rate of change) and determines trend (STRENGTHENING/WEAKENING/STABLE).

Persists to `CurrencyStrength` (PostgreSQL/SQLAlchemy). Reads from `news_events` (MongoDB). Used by `FundamentalDivergence` strategy in `signal_generator`.

## Planned Architecture

### NLP Pipeline (`nlp/`) — placeholders
Text → `EntityExtractor` → `SentimentAnalyzer` (-1 to +1) → `ImpactClassifier` (high/medium/low + magnitude + direction)

### EventProcessor (`processors/event_processor.py`) — placeholder
Central orchestrator: scrapers → NLP pipeline → classify impact by currency → persist to MongoDB (`news_events`) + PostgreSQL (`EventImpact`)

### Scrapers (`scrapers/`) — placeholders
All return mock data. Real implementation must handle: rate limiting, BeautifulSoup/Scrapy parsing, APIs (NewsAPI, Twitter API v2), MQ publication.

## Integration Flow
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
- `forex_shared.database`: `SessionLocal`
- `pymongo`: MongoClient for `news_events`
- `numpy`: exponential time decay

## Implementation Priority
1. `economic_calendar.py` — ForexFactory or Investing.com scraper (highest immediate impact)
2. `news_scraper.py` — NewsAPI or Benzinga integration
3. `sentiment_analyzer.py` — external API (Claude/OpenAI) initially
4. `twitter_scraper.py` — Twitter API v2 with curated profile list
5. `event_processor.py` — full orchestration with MQ publication
6. `entity_extractor.py` + `impact_classifier.py` — NLP pipeline refinement
