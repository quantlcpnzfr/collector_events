# GlobalIntel Architecture & Pipeline

## Overview
The `globalintel` pipeline inside `collector_events` manages the extraction, NLP enrichment, routing, and persistence of fundamental/geopolitical intelligence.

## Data Flow
1. **Extraction**: `BaseExtractor` subclasses (managed by `ExtractorFactory`) fetch data. Scheduled by `IntelOrchestrator`.
2. **NLP Inference**: `IntelOrchestrator` sends raw text to `EventProcessor`.
3. **LocalNLPEngine**: Extracts features locally using CPU-bound SLMs (FinBERT for sentiment, DeBERTa for zero-shot categorization via sliding windows, spaCy for NER, GLiNER for tactical graphs).
4. **Scoring**: `EventProcessor` calculates multidimensional axes (Geopolitical Severity, Market Impact, Noise, Directional Confidence) to build a `trade_emit_score` and an `oracle_review_score`.
5. **Emission**: `GlobalTagEmitter` decides to either publish actionable `GlobalTags` to RabbitMQ (`intel.global_tags`) or send ambiguous/complex events to human/LLM review (`intel.oracle.review`).
6. **Persistence**:
   - `IntelCache` (Redis) stores immediate JSON results with a TTL.
   - `IntelMongoStore` (MongoDB) deduplicates events using a `fingerprint` (UUID5) and skips unchanged items using a `hash` (SHA-256).

## Testing
- `TestOrchestrator` (`main_test.py`) with `TestExtractor` (`test_extractor.py`) allow sequential, dry-run simulation of the NLP and emitter pipeline against a static `mock_intel_items_big.json` file without side effects.