# WorldMonitor Full Sources Configuration Report

Generated at: 2026-04-29T06:33:17.117Z

This report drops the previous globalintel comparison and treats WorldMonitor as the reference corpus for a new Python extractor architecture. All counts below point to the source-of-truth file used for extraction, and the JSON configs in `collector_events/sources` are intended to be consumed directly by Python collectors, routers, scoring jobs, and LLM prompt builders.

## Generated Config Files

- `services/collector_events/collector_events/sources/telegram_channels.json`
- `services/collector_events/collector_events/sources/news_feeds_client.json`
- `services/collector_events/collector_events/sources/news_feeds_server.json`
- `services/collector_events/collector_events/sources/sites.json`
- `services/collector_events/collector_events/sources/source_metadata.json`
- `services/collector_events/collector_events/sources/rss_allowed_domains.json`
- `services/collector_events/collector_events/sources/advisory_sources.json`
- `services/collector_events/collector_events/sources/seed_endpoint_sources.json`
- `services/collector_events/collector_events/sources/social_sources.json`
- `services/collector_events/collector_events/sources/ai_prompts.json`
- `services/collector_events/collector_events/sources/all_sources.json`
- `services/collector_events/collector_events/sources/telegram_relay_python_guide.json`
- `services/collector_events/collector_events/sources/source_manifest.json`

## Source Counts

| Source | Count | Source of truth | Config |
| --- | --- | --- | --- |
| Telegram OSINT channels | 45 | worldmonitor/data/telegram-channels.json | services/collector_events/collector_events/sources/telegram_channels.json |
| Client news/intel feed records | 523 | worldmonitor/src/config/feeds.ts | services/collector_events/collector_events/sources/news_feeds_client.json |
| Server news/intel feed records | 267 | worldmonitor/server/worldmonitor/news/v1/_feeds.ts | services/collector_events/collector_events/sources/news_feeds_server.json |
| Travel/security advisory feeds | 23 | worldmonitor/scripts/seed-security-advisories.mjs | services/collector_events/collector_events/sources/advisory_sources.json |
| Seed scripts scanned | 93 | worldmonitor/scripts | services/collector_events/collector_events/sources/seed_endpoint_sources.json |
| Literal seed-script URLs | 254 | seed-*.mjs | services/collector_events/collector_events/sources/seed_endpoint_sources.json |
| Social velocity sources | 2 | worldmonitor/scripts/ais-relay.cjs | services/collector_events/collector_events/sources/social_sources.json |
| X/Twitter ingestion accounts | 0 | targeted code search | services/collector_events/collector_events/sources/social_sources.json |
| RSS relay allowlist domains | 295 | worldmonitor/shared/rss-allowed-domains.json | services/collector_events/collector_events/sources/rss_allowed_domains.json |
| AI prompt configs | 10 | worldmonitor/server/worldmonitor/news/v1/_shared.ts, worldmonitor/server/worldmonitor/news/v1/summarize-article.ts, worldmonitor/server/worldmonitor/intelligence/v1/deduction-prompt.ts, worldmonitor/server/worldmonitor/intelligence/v1/chat-analyst-prompt.ts, worldmonitor/src/services/analysis-framework-store.ts, worldmonitor/src/services/summarization.ts, worldmonitor/src/services/daily-market-brief.ts, worldmonitor/scripts/seed-insights.mjs, worldmonitor/scripts/seed-forecasts.mjs | services/collector_events/collector_events/sources/ai_prompts.json |
| Built-in analysis frameworks | 5 | worldmonitor/src/services/analysis-framework-store.ts | services/collector_events/collector_events/sources/ai_prompts.json |
| Consolidated source records | 941 | all files above | services/collector_events/collector_events/sources/all_sources.json |

## Recommended Python Config Usage

Use `source_manifest.json` as the entrypoint. For collector implementation, load the family-specific files rather than only `all_sources.json`:

- `telegram_channels.json`: public Telegram OSINT channels plus relay behavior, rate limits, filters, and Python port notes.
- `sites.json`, `news_feeds_client.json`, `news_feeds_server.json`: RSS/Atom/Google News feed configs, grouped by category, variant, language, tier, source type, and risk profile.
- `advisory_sources.json`: government, embassy, CDC/ECDC/WHO travel and health security feeds.
- `seed_endpoint_sources.json`: literal API/CSV/RSS/HTML endpoints and env vars discovered across `seed-*.mjs` jobs. Dynamic URLs still need script-level review when you port each extractor.
- `social_sources.json`: Reddit social velocity sources and explicit note that no X/Twitter ingestion account list was found.
- `ai_prompts.json`: prompt families, scenarios, conditions, output formats, built-in analytical frameworks, and strict JSON schemas used by WorldMonitor.
- `source_metadata.json`: source tiers, source types, and state-media/propaganda risk profile metadata.
- `rss_allowed_domains.json`: domains permitted through the WorldMonitor RSS relay/proxy.

## Capture Methods Found

- `telegram_mtproto_relay`: GramJS client polls public Telegram channels through MTProto, then serves a local relay endpoint.
- `direct_rss_or_atom`: normal HTTP GET against RSS/Atom feeds.
- `google_news_rss`: Google News RSS search URLs, usually as fallback for blocked/cloud-hostile sources or broad query feeds.
- `railway_rss_proxy` / `rss_or_atom_via_relay_allowlist`: proxy fetch through a Railway relay when origin sites block serverless/cloud IPs.
- `http_fetch`, `csv`, `graphql`, `html_or_regex_scrape`, `yahoo_finance`, `gdelt_api`: detected from seed scripts and recorded per script in `seed_endpoint_sources.json`.
- `reddit_hot_json`: Reddit hot-post JSON for r/worldnews and r/geopolitics, used by Social Velocity.
- `llm_api`: not a data source itself, but prompt-driven transformations were found in summarize, deduction, chat analyst, insights, and forecast/market implications pipelines.

## Telegram Relay Mechanics

Source files: `worldmonitor/scripts/ais-relay.cjs`, `worldmonitor/api/telegram-feed.js`, `worldmonitor/server/worldmonitor/intelligence/v1/list-telegram-feed.ts`, `worldmonitor/scripts/telegram/session-auth.mjs`, `worldmonitor/docs/data-sources.mdx`.

WorldMonitor does not read Telegram through the Bot API. It uses a user-session MTProto client with GramJS and a `StringSession`. The Railway relay loads `worldmonitor/data/telegram-channels.json`, waits by default 60 seconds on startup, connects with `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and `TELEGRAM_SESSION`, then polls enabled public channels sequentially. Per channel it calls `getEntity(handle)`, then `getMessages(entity, { limit, minId })`. It skips media-only posts, truncates text to 800 chars by default, deduplicates by `channel:message_id`, stores a rolling 200-item buffer, and exposes `/telegram/feed?limit=&topic=&channel=`.

For Python, the closest equivalent is Telethon: create a session with your own Telegram API credentials, poll public channels from `telegram_channels.json`, persist `cursor_by_handle`, normalize messages into the same output shape, and expose a FastAPI route. A direct `https://t.me/s/<handle>` scraper can be used only as a low-reliability fallback because coverage and markup stability are worse than MTProto. Avoid running the same Telegram StringSession in multiple workers at once; WorldMonitor explicitly guards against `AUTH_KEY_DUPLICATED` after container restarts.

## AI Prompt Inventory

`ai_prompts.json` contains the prompt families discovered in code, including article summarization, deduction, chat analyst domain emphasis, daily market brief fallback rules, seed insight headline selection, strict JSON forecast extraction, impact expansion, 72-hour simulation templates, market implication cards, and the built-in Dalio/Buffett/geopolitical/PMESII/red-team frameworks.

## Notes For Trading-Oriented Port

Treat these configs as signal inputs, not trading decisions. A reasonable Python pipeline is: collect raw source items, normalize timestamps and domains, deduplicate by canonical URL/message ID/title hash, classify by category/topic/region, score reliability with `tier` and `riskProfile`, then send compact evidence packets into your own OpenAI prompts for market-impact hypotheses. Keep raw source text, source URL, and source-of-truth config IDs with every generated signal so backtests and live decisions remain auditable.
