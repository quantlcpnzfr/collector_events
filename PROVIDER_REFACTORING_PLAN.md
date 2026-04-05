# Collector Events — Provider Refactoring Plan

## Current State
`collector_events/extractors/` has 13 HuggingFace dataset extractors.
4 of them are market data (not events): Yahoo Finance, Forex Daily Price, OHLCV 1m, Forex USDJPY.

## Proposed Architecture

### Phase 1: Move ExtractorProvider base to shared_lib
```
shared_lib/forex_shared/providers/datasets/
├── __init__.py
├── base_provider.py              ← ExtractorProvider (ABC) + DatasetSplitInfo
├── huggingface_base_provider.py  ← BaseHuggingFaceDatasetProvider + FlatHuggingFaceDatasetProvider
└── factory.py                    ← DatasetProviderFactory
```
All 13 extractors stay in `collector_events` but inherit from shared_lib base.

### Phase 2: Create realtime providers structure
```
shared_lib/forex_shared/providers/realtime/
├── __init__.py
├── base_crawler.py               ← BaseCrawler (ABC) — scrape/fetch from web
├── base_social_provider.py       ← BaseSocialProvider (ABC) — social media APIs
├── base_news_provider.py         ← BaseNewsProvider (ABC) — news RSS/API
└── factory.py                    ← RealtimeSourceFactory
```

Concrete implementations stay in `collector_events/`:
```
collector_events/
├── crawlers/
│   ├── news/
│   │   ├── reuters_crawler.py
│   │   ├── bloomberg_crawler.py
│   │   └── forexfactory_crawler.py
│   └── calendar/
│       └── economic_calendar_crawler.py
├── social/
│   ├── twitter_provider.py       ← Twitter/X API
│   └── instagram_provider.py     ← Instagram scraper
├── extractors/                   ← HuggingFace datasets (existing, cleaned)
│   ├── huggingface/              ← only event-related extractors (9 of 13)
│   └── factory.py
└── processors/                   ← NLP, sentiment, currency strength
```

### Phase 3: Move market data extractors
Move Yahoo Finance, Forex Daily Price, OHLCV 1m, Forex USDJPY extractors out of
collector_events into shared_lib/providers/datasets/ as they are market data, not events.

## Provider type summary
| Provider Base           | Purpose                    | Location (base)         | Location (impl)              |
|------------------------|----------------------------|-------------------------|------------------------------|
| IMarketDataProvider    | Historical market OHLCV    | shared_lib/market_data/ | shared_lib/market_data/      |
| ILiveMarketDataProvider| Live market + streaming    | shared_lib/market_data/ | shared_lib/market_data/      |
| ExtractorProvider      | Dataset extraction (batch) | shared_lib/datasets/    | collector_events/extractors/ |
| BaseCrawler            | Web scraping (realtime)    | shared_lib/realtime/    | collector_events/crawlers/   |
| BaseSocialProvider     | Social media APIs          | shared_lib/realtime/    | collector_events/social/     |
| BaseNewsProvider       | News feeds/APIs            | shared_lib/realtime/    | collector_events/crawlers/   |
