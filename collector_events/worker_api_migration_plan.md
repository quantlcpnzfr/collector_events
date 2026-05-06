# Collector Events Worker API Migration Plan

## Purpose

Converge `collector_events` on a shared runtime contract without breaking the current `globalintel` orchestrator path that already works well for the classic extractors.

This plan treats the codebase as a **hybrid architecture**:

- `globalintel/*` extractors remain orchestrator-driven for now
- `osint_telegram` is already refactored into `worker_api`
- `translation` is already refactored into `worker_api`
- `nlp` should now join `worker_api` so translated Intel items can enter the same enrichment pipeline

## Current Reality

### Stable legacy path

The `globalintel` orchestrator currently runs many non-Telegram extractors successfully:

- instantiate extractors through `ExtractorFactory`
- run them on schedule
- produce `ExtractionResult / IntelItem`
- enrich with `EventProcessor`
- persist/cache/tag results

This path does **not** depend on translation, and it should remain intact while we migrate.

### New worker-api path

The new message-driven path currently looks like:

`osint_telegram` -> `intel.events.{domain}` -> `translation` -> `intel.translated.{domain}`

Telegram extraction is already ahead of the rest in runtime design and now adds:

- source governance metadata
- source score
- exact-duplicate signals
- cross-message cluster metrics

Those signals are stamped into `IntelItem.extra` and survive serialization through MQ and Mongo.

## Canonical Contract Goal

All pipelines, legacy or worker-api, should converge on the same normalized item contract:

- core payload is always `IntelItem`
- enrichment is carried inside `IntelItem.extra`
- downstream processors should work whether the item came from:
  - a `globalintel` extractor
  - `osint_telegram`
  - future worker-api extractors

## Contract Layers

### Base fields

Available to all extractors:

- `id`
- `source`
- `domain`
- `title`
- `body`
- `published_at`
- `tags`
- `country` / country refs

### Translation fields

Only added when the translation stage runs:

- `extra.translation_source`
- `extra.original_body`

### Source-governance fields

Expected from Telegram now, and gradually adaptable to other extractors:

- `extra.source_score`
- `extra.source_role`
- `extra.source_authenticity_class`
- `extra.source_bias_risk`
- `extra.routing_family`
- `extra.deployment_priority`
- `extra.verification_required`

### Attention / repetition fields

Expected from Telegram now, and adaptable later to other high-volume feeds:

- `extra.fingerprint_key`
- `extra.exact_duplicate`
- `extra.exact_emit_count`
- `extra.exact_channel_count`
- `extra.cluster_id`
- `extra.cluster_emit_count`
- `extra.cluster_channel_count`
- `extra.cluster_weighted_attention`
- `extra.cluster_first_seen_at`
- `extra.cluster_last_seen_at`
- `extra.cluster_velocity_per_hour`
- `extra.cluster_similarity`
- `extra.cluster_is_new`

### NLP fields

Should be written by both orchestrator-based NLP and worker-api NLP:

- `extra.impact_category`
- `extra.danger_score`
- `extra.nlp_features`
- `extra.domain_weight`
- `extra.source_signal_adjustment`
- `extra.cluster_signal_adjustment`
- `extra.attention_score`
- `extra.oracle_review_candidate`

## Migration Phases

### P0 - Contract convergence

Status: in progress / mostly done for Telegram

Deliverables:

- preserve `IntelItem.extra` across MQ
- keep translation additive and non-destructive
- stabilize source-governance and clustering fields

Exit criteria:

- translated payload still carries Telegram source/clustering signals
- downstream consumers can rely on `extra` as the main enrichment envelope

### P1 - Add NLP to worker_api

Goal:

Introduce an NLP worker that consumes `intel.translated.#` and republishes enriched items.

Deliverables:

- `collector_events.nlp.session`
- `collector_events.nlp.worker`
- `collector_events.nlp.main`
- default input topic: `intel.translated.#`
- default output topic: `intel.enriched.{domain}`
- mark output with `event_type=INTEL_ITEM_ENRICHED`

Notes:

- This does not replace the orchestrator NLP path
- It supplements it for message-driven extractors, especially Telegram

Exit criteria:

- translated Telegram items can be consumed by worker-api NLP
- enriched items retain source/clustering/translation metadata

### P2 - Make `EventProcessor` source-aware

Goal:

Teach NLP scoring to use upstream metadata when available.

Deliverables:

- adjust score using `source_score`
- penalize high-bias / retail-signal / partisan sources
- add attention bonuses from cluster metrics
- add verification penalty when required
- keep behavior backward compatible when fields are missing

Exit criteria:

- non-Telegram legacy items still score normally
- Telegram items gain source/attention-aware scoring

### P3 - Shared enriched topic contract

Goal:

Make `intel.enriched.{domain}` the common event shape for downstream consumers.

Deliverables:

- Oracle-facing services consume enriched items instead of raw translated items
- routing decisions based on `extra.oracle_review_candidate`
- optional Mongo update of final enriched state

Exit criteria:

- Oracle no longer needs to infer source quality from raw text alone

### P4 - Migrate more extractors into worker_api

Goal:

Gradually move suitable `globalintel` extractors to worker_api while preserving the orchestrator for the rest.

Candidate order:

1. high-frequency social/text sources
2. feed-like extractors with small per-item payloads
3. slower API extractors only when operationally useful

Exit criteria:

- more extractors share the same MQ enrichment path
- orchestrator remains available for extractors where batch scheduling is still the best fit

## Recommended Runtime Topology

### Legacy extractors

`globalintel extractor -> orchestrator -> EventProcessor -> cache/store/tagging`

### Telegram / worker-api extractors

`osint_telegram -> intel.events.{domain}`

If translation needed:

`intel.events.{domain} -> translation -> intel.translated.{domain} -> nlp worker -> intel.enriched.{domain}`

If translation not needed in a future worker-api extractor:

`extractor -> intel.events.{domain} -> nlp worker (optional alternate topic/session) -> intel.enriched.{domain}`

## Design Rules

1. Do not force translation into legacy extractors that do not need it.
2. Do not make NLP recompute source clustering that the extractor already knows.
3. Prefer carrying computed metadata in `IntelItem.extra` over re-querying Mongo on the hot path.
4. Use Mongo lookups only as fallback or offline analytics, not as mandatory per-message runtime logic.
5. Keep the orchestrator path alive while the worker-api path matures.

## Immediate Next Steps

1. Add the worker-api NLP consumer for `intel.translated.#`
2. Update `EventProcessor` to consume source/clustering metadata from `extra`
3. Publish enriched items to `intel.enriched.{domain}`
4. Later decide whether Oracle should consume `intel.enriched.{domain}` directly or through a filtered review topic
