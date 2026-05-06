# Telegram Channels Refactor Backlog

Generated: 2026-05-06

Scope:
- Source catalog: [telegram_channels.json](</c:/Projects/forex_system/services/collector_events/collector_events/sources/telegram_channels.json>)
- Review input: [telegram_channels_osint_trade_review.md](</c:/Projects/forex_system/services/collector_events/collector_events/sources/telegram_channels_osint_trade_review.md>)
- Runtime builder: [osint_extractor.py](</c:/Projects/forex_system/services/collector_events/collector_events/events_extractors/osint_extractor.py>)
- Worker/session consumer: [session.py](</c:/Projects/forex_system/services/collector_events/collector_events/extractors/osint_telegram/session.py>)

## Objectives

1. Make the Telegram source catalog machine-trustworthy.
2. Separate source governance from raw collection.
3. Prevent runtime startup/resolution behavior from degrading as the catalog grows.
4. Route different source families into the correct downstream pipelines.

## Current State

- `telegram_channels.json` is structurally consistent but contains stale `counts`.
- The catalog uses `camelCase` fields and is consumed by multiple local loaders.
- Runtime startup is vulnerable to resolution storms and Telethon flood waits.
- Trust/routing semantics are still too coarse for Oracle-facing use.
- Finance tape, cyber, conflict OSINT, crypto, and forex retail signals are mixed in one flat catalog.

## Sprint Roadmap

## P0 - Catalog Integrity and Contract Stabilization

Goal:
Make the catalog self-validating and safe to evolve without breaking the extractor.

### Deliverables

- [x] Add a catalog validator script for `telegram_channels.json`
- [x] Recompute `counts` from `channels` instead of maintaining them manually
- [ ] Add top-level schema metadata for the catalog
- [x] Define canonical enums for topic, source role, routing family, bias risk, authenticity class
- [x] Add additive governance defaults without breaking current consumers
- [x] Preserve backward compatibility with existing `camelCase` field usage

### Implementation Tasks

1. Validation tooling
   - Create `validate_telegram_channels.py`
   - Validate required fields
   - Validate duplicates
   - Validate `publicUrl`
   - Validate enum candidates
   - Recompute and optionally rewrite `counts`

2. Root catalog metadata
   - Add `schemaVersion`
   - Add `catalogType`
   - Add `governance`
   - Add `generated counts` support via validator

3. Additive governance model
   - Introduce optional per-channel fields:
     - `sourceRole`
     - `routingFamily`
     - `biasRisk`
     - `verificationRequired`
     - `authenticityClass`
     - `canTriggerTrade`
     - `canInfluenceSentiment`
     - `sendToTranslator`
     - `sendToOracle`
   - Keep them optional during P0
   - Provide derived defaults in tooling/runtime builder

4. Runtime compatibility
   - Update runtime builder to preserve governance fields when present
   - Derive safe defaults for runtime-generated channel inventories

### Acceptance Criteria

- Validator passes on current catalog
- `counts` in the file match actual entries
- Current extractor still runs with no contract break
- Runtime file builder can carry governance fields forward

### Risks

- Over-normalizing field names too early may break current consumers
- Treating governance defaults as final truth too early may hide real editorial review work

## P1 - Runtime Resolution Safety and Channel Lifecycle

Goal:
Reduce Telethon startup pain and make the collector operate predictably with a larger catalog.

### Deliverables

- [x] Channel entity cache / resolved handle cache
- [x] Incremental startup resolution
- [x] Per-channel runtime state
- [x] Priority-first startup scheduling
- [x] Flood-wait-aware backoff strategy

### Implementation Tasks

1. Resolution cache
   - Persist resolved input entities by handle
   - Reuse cached entities before calling `get_entity()`

2. Startup scheduler
   - Resolve P0 channels first
   - Resolve batches incrementally
   - Defer lower-priority sets until healthy

3. Runtime state
   - Add:
     - `lastResolvedAt`
     - `lastSuccessAt`
     - `lastFloodWaitAt`
     - `resolutionFailures`
     - `lastBackfillCount`

4. Safe resolution strategy
   - Honor flood waits explicitly
   - Backoff on repeated failures
   - Avoid full-catalog resolve on every cold start

### Acceptance Criteria

- Startup flood waits are materially reduced
- P0 channel collection remains stable under restart
- Failed resolutions no longer poison the whole startup flow

### P1 Completion Notes

- Added persistent `telegram_entity_cache` and `telegram_channel_state` collections.
- Warm restart now skips failed handles during their cooldown window instead of retrying them every startup.
- Resolution metrics are now explicit in session logs and snapshots:
  - `cache_hit_count`
  - `cache_miss_count`
  - `skipped_resolution_count`
  - `live_resolve_count`
  - `resolution_duration_seconds`
  - `startup_duration_seconds`
- Cached warm-start behavior measured on 2026-05-06:
  - `73` cached channel resolutions reused
  - `14` failed handles skipped by cooldown
  - `0` flood waits on warm restart
  - resolution phase completed in about `28s`
  - full session startup completed in about `29s`

## P2 - Source Governance and Routing Separation

Goal:
Make the catalog express how a source should be used, not just what it is called.

### Deliverables

- [x] Channel authenticity classification
- [x] Source family routing classification
- [x] P0/P1/P2 deployment priority embedded in the catalog
- [x] Separate routing path for forex retail signal channels

### Implementation Tasks

1. Source classification
   - Add values such as:
     - `official_brand`
     - `independent_reporter`
     - `osint_aggregator`
     - `narrative_partisan`
     - `market_tape`
     - `retail_signal`
     - `unofficial_mirror`

2. Routing families
   - Introduce:
     - `osint_conflict`
     - `osint_geopolitics`
     - `osint_middleeast`
     - `cyber_news`
     - `finance_tape`
     - `crypto_flow`
     - `forex_retail_signal`

3. Catalog curation pass
   - Reclassify known entries:
     - `financialjuice` -> likely `unofficial_mirror`
     - `binance_announcements` -> `official_brand`
     - `thehackernews` -> `official_brand`
     - `OSINTdefender` -> `osint_aggregator`
     - `rybar`, `intel_slava`, `DDGeopolitics`, `zerohedge` -> high-bias lane

4. Separate forex signal handling
   - Remove `forex_signals` channels from factual OSINT/Oracle path
   - Route to sentiment/contrarian analysis only

### Acceptance Criteria

- No retail signal source is treated like factual OSINT
- Each channel has an explicit trust/routing class

### P2 Progress Notes

- Catalog curation completed for the 14 problem handles found during P1 runtime measurement.
- Handle migrations applied:
  - `intel_slava` -> `intelslava`
  - `firstsquawk` -> `firstsquaw`
  - `lebanon24` -> `lebanon_24`
  - `unitedsignals` -> `UnitedSignalsFX`
  - `prosignalsfx` -> `prosignalsfx_1`
- Disabled dead or non-Telegram handles:
  - `cybersecboardrm`
  - `CYBERWARCOM`
  - `thecyberwire`
  - `syriatracker`
  - `disclosewt` (superseded by existing `disclosetv`)
  - `bell_geo`
  - `deita`
  - `intel_sky`
  - `crypto_pump_alerts`
- Classification improvements applied:
  - `intelslava` -> `authenticityClass=narrative_partisan`
  - `firstsquaw` -> `authenticityClass=official_brand`
  - `lebanon_24` -> `authenticityClass=unofficial_mirror`
  - `crypto_pump_alerts` -> `authenticityClass=retail_signal`
- Deployment priority is now embedded in the catalog as `deploymentPriority`:
  - `p0` = core startup set
  - `p1` = standard active set
  - `p2` = lower-priority or sentiment-only set
  - `disabled` = excluded from runtime startup
- Current priority distribution:
  - `p0`: 10
  - `p1`: 59
  - `p2`: 9
  - `disabled`: 9
- Forex retail signal channels remain sentiment-only with `sendToOracle=false`.
- Post-curation runtime validation on 2026-05-06:
  - `78` active channels
  - `5` live resolves for migrated handles
  - `73` cache hits
  - `0` resolution failures
  - `0` flood waits
  - startup completed in about `23s`
- Warm-cache validation after priority embedding on 2026-05-06:
  - `78` resolved channels
  - `78` cache hits
  - `0` cache misses
  - `0` failures
  - `0` flood waits
  - startup completed in about `24s`

## P3 - Enrichment, Dedupe, and Source Scoring

Goal:
Deliver scored and clustered events instead of raw Telegram posts.

### Deliverables

- [x] Source scoring model
- [x] Dedupe layer
- [x] Cross-source event clustering
- [ ] Translation-aware enrichment
- [ ] Entity/country/asset resolution before Oracle

### Implementation Tasks

1. Scoring
   - Base trust
   - Confirmation count
   - Bias penalty
   - Recency
   - Originality
   - Translation confidence

2. Dedupe
   - Normalized text hash
   - URL match
   - Timestamp bucketing

3. Clustering
   - Group related posts across channels into one event narrative

4. Enrichment
   - Preserve `original_body`
   - Translate only when needed
   - Add structured entity/country/asset features

### Acceptance Criteria

- Oracle receives enriched, deduped, scored events
- Source score influences downstream routing

### P3 Progress Notes

- Implemented first-pass source scoring directly in the Telegram extractor.
- `IntelItem.extra` now carries:
  - `source_score`
  - `source_authenticity_class`
  - `source_bias_risk`
  - `source_role`
  - `routing_family`
  - `deployment_priority`
  - `verification_required`
  - `fingerprint_key`
- Implemented exact-content dedupe using normalized text fingerprints stored in MongoDB collection:
  - `telegram_content_fingerprint`
- Implemented first-pass similarity clustering using rolling MongoDB-backed cluster state:
  - `telegram_content_cluster`
- Improved clustering quality beyond raw token overlap:
  - composite similarity uses token overlap, token containment, character n-gram overlap, and character containment
  - candidate search is narrowed by rolling time window, topic, and routing family
- Dedupe behavior is intentionally conservative for this phase:
  - exact normalized-text match only
  - scoped by domain fingerprint key
  - bounded by configurable dedupe window
- Clustering behavior for this phase:
  - near-identical posts are kept, not suppressed
  - each post is enriched with cluster metrics
  - attention grows with repeated emits and accumulated source score
- `IntelItem.extra` now also carries:
  - `cluster_id`
  - `cluster_emit_count`
  - `cluster_channel_count`
  - `cluster_weighted_attention`
  - `cluster_first_seen_at`
  - `cluster_last_seen_at`
  - `cluster_velocity_per_hour`
  - `cluster_similarity`
  - `cluster_is_new`
  - `exact_duplicate`
  - `exact_emit_count`
  - `exact_channel_count`
- Runtime validation on 2026-05-06:
  - extractor completed a 100-message run successfully
  - output file contained the new scoring metadata
  - fingerprint collection populated with 100 entries during the run
  - cluster collection populated with 99 entries during the run
  - at least 1 cluster reached `emit_count > 1`
  - warm-cache startup remained healthy with `0` failures and `0` flood waits
- Improved clustering validation on 2026-05-06:
  - extractor completed another 100-message run successfully
  - multi-channel attention clusters appeared with `channel_count > 1`
  - confirmed examples included overlap between:
    - `thecradlemedia` and `MiddleEastNow_Breaking`
  - top multi-channel clusters showed:
    - `emit_count = 2`
    - `channel_count = 2`
    - `weighted_attention ≈ 1.13`

## P4 - Quality Metrics and Source Feedback Loop

Goal:
Operationalize source management with real evidence.

### Deliverables

- [ ] Source quality dashboard
- [ ] Duplication and yield metrics
- [ ] Oracle usefulness feedback
- [ ] Automatic downgrade/disable heuristics

### Metrics

- collection success rate
- flood-wait rate
- message yield per channel
- duplication ratio
- translation rate
- cross-source confirmation rate
- Oracle usefulness rate

### Acceptance Criteria

- Source priority can be justified quantitatively
- Noisy channels can be downgraded using historical evidence

## P0 Implementation Notes

P0 should be additive and low-risk:

- keep `camelCase` fields in the source file
- allow optional governance fields
- add validator + generated counts first
- derive defaults in runtime builder rather than forcing manual annotation of all 87 channels immediately

## Immediate Decisions

- Treat `financialjuice` as unverified/unofficial until official affiliation is proven
- Keep `binance_announcements`, `thehackernews`, and `citeam` in high-value lanes
- Keep `VahidOnline` high-value but not self-confirming
- Move `forex_signals` into a separate routing family as soon as P2 starts

## Execution Order

1. P0 validator and counts rewrite
2. P0 runtime governance field passthrough
3. P1 resolution cache and safe startup
4. P2 channel governance curation
5. P3 scoring and clustering
6. P4 metrics and feedback
