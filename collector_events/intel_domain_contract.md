# Intel Domain Contracts & Data Models

## Overview
The `shared_lib/forex_shared/domain/intel.py` file dictates the data boundaries for the global intelligence pipeline. Every extractor must output this schema, and every downstream trading service must consume it.

## 1. IntelItem
The fundamental unit of extracted intelligence.
- **Base Fields**: `id`, `source`, `domain`, `title`, `url`, `body`.
- **Time**: `published_at` (canonical UTC ISO-8601), `ts` (legacy alias), `fetched_at`.
- **Geographic Enrichment**: Uses `CountryRef` (ISO-2 code, name, ISO-4217 currency).
  - Categorized into: `source_countries`, `actor_countries`, `target_countries`, `mentioned_countries`.
  - The legacy `country` list of strings is kept automatically synchronized with `mentioned_countries` for backward compatibility.
- **Extensibility**: The `extra` dict field holds dynamic NLP outputs (`danger_score`, `impact_category`, `gliner_tactical`, multidimensional scores).
- **Derived Properties**: Exposes `affected_currencies` to map involved countries to tradable FX assets.

## 2. ExtractionResult
The envelope returned by an extractor's `run()` method to the orchestrator.
- **Fields**: `source`, `domain`, `items` (List of `IntelItem`), `elapsed_ms`, `fetched_at`.
- **Error State**: Includes an `error` string. Even if a fetch fails, an `ExtractionResult` is yielded to ensure the orchestrator can audit the failure in MongoDB (`intel_runs`) and Redis (`seed-meta`).

## 3. GlobalTag
The actionable downstream signal produced by `GlobalTagEmitter`.
- **Fields**: `asset` (e.g. `EURUSD`), `bias` (e.g. `bullish`, `bearish`), `risk_score` (0.0 to 1.0), `trigger_event_id`, `expires_at`.
- **Routing**: 
  - Saved to Redis as `alert_global:{asset}`.
  - Pushed to RabbitMQ topic `intel.global_tags`.
- **Consumers**:
  - `session_manager`: Injects into strategy `additional_data`.
  - `executor_trading`: Blocks trades or applies confidence penalties based on active tags.
  - `risk_manager`: Emits macro alerts.

## Constants
- **IntelDomain**: Conflict, cyber, economic, market, environment, etc.
- **IntelSeverity**: HIGH, MEDIUM, LOW.
- **IntelBias**: strong_bullish, bullish, neutral, bearish, strong_bearish.