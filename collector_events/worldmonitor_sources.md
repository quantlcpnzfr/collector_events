# Worldmonitor source audit for collector_events/globalintel

Generated: 2026-04-29

Scope: local repository scan only. Main comparison target:

- `C:\Projects\forex_system\worldmonitor`
- `C:\Projects\forex_system\services\collector_events\collector_events\globalintel`
- `C:\Projects\forex_system\docs\API_KEYS_REPORT.md`
- `C:\Projects\forex_system\docs\worldmonitor\Developer_Guide_Data_Sources.md`

## Executive summary

The largest gap is Telegram OSINT.

- `worldmonitor` has a product-managed Telegram source file with 45 channels: 37 in `full`, 8 in `tech`, 0 in `finance`.
- `globalintel` currently configures 18 Telegram handles and overlaps with `worldmonitor` on only 1 handle: `Middle_East_Spectator`.
- `globalintel/social/telegram.py` does not talk to Telegram directly. It only calls a relay URL: `GET {TELEGRAM_RELAY_URL}/telegram/feed`.
- The current `API_KEYS_REPORT.md` documents `TELEGRAM_RELAY_URL` and `TELEGRAM_RELAY_SECRET`, but does not document the relay-side Telegram credentials used by `worldmonitor`: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION`.

News/RSS is the second largest gap.

- `worldmonitor/server/worldmonitor/news/v1/_feeds.ts` has 251 unique source names.
- `worldmonitor/src/config/feeds.ts` has 486 unique source names and is a broader UI/client superset.
- `globalintel/feeds/rss_extractor.py` has 41 RSS source names.
- Exact-name coverage gap: 226 server news sources missing from `globalintel`; 453 client/UI feed names missing from `globalintel`.

Twitter/X finding:

- I did not find a real Twitter/X ingestion pipeline in either `worldmonitor` or `globalintel`.
- There are X/Twitter links for sharing, metadata, author/contact, and UI text, but no `api.twitter.com` ingestion, no bearer-token workflow, no monitored accounts list, no `snscrape`, and no Nitter-style scraper.

## Source-of-truth files

### Worldmonitor

| Area | File | Role |
|---|---|---|
| Telegram channel config | `worldmonitor/data/telegram-channels.json` | Canonical product-managed Telegram channel list. |
| Telegram relay implementation | `worldmonitor/scripts/ais-relay.cjs` | GramJS Telegram client, polling loop, `/telegram/feed` endpoint. |
| Telegram API proxy | `worldmonitor/api/telegram-feed.js` | Edge/API proxy to relay. |
| Telegram server RPC | `worldmonitor/server/worldmonitor/intelligence/v1/list-telegram-feed.ts` | Server wrapper around relay endpoint. |
| Telegram client service | `worldmonitor/src/services/telegram-intel.ts` | Frontend data service. |
| Server news/RSS config | `worldmonitor/server/worldmonitor/news/v1/_feeds.ts` | Production digest source list by variant/category. |
| Client news/RSS config | `worldmonitor/src/config/feeds.ts` | Large client/UI feed config, source types, propaganda/source-risk metadata. |
| RSS domain allowlist | `worldmonitor/shared/rss-allowed-domains.json` | Shared list of allowed RSS domains. |
| API RSS allowlist copy | `worldmonitor/api/_rss-allowed-domains.js` | Runtime allowlist copy for API proxy. |
| RSS proxy | `worldmonitor/api/rss-proxy.js` | Direct fetch plus relay fallback/allowlist behavior. |
| Server digest implementation | `worldmonitor/server/worldmonitor/news/v1/list-feed-digest.ts` | RSS/Atom fetch, parse, cache, classify, story tracking. |
| Source tier metadata | `worldmonitor/server/_shared/source-tiers.ts` | Tier/risk metadata for sources. |
| Seed scripts | `worldmonitor/scripts/seed-*.mjs` | Source-specific ingestion jobs for conflict, cyber, markets, aviation, climate, trade, sanctions, etc. |

### Globalintel Python conversion

| Area | File | Role |
|---|---|---|
| Factory/schedule | `services/collector_events/collector_events/globalintel/extractor_factory.py` | Instantiates and schedules extractors. |
| Telegram/social config | `services/collector_events/collector_events/globalintel/config/social.json` | Reddit + Telegram handles. |
| Telegram relay consumer | `services/collector_events/collector_events/globalintel/social/telegram.py` | Calls relay endpoint; no direct Telegram client. |
| RSS/news extractor | `services/collector_events/collector_events/globalintel/feeds/rss_extractor.py` | Python RSS/Atom fetcher. |
| Advisory config | `services/collector_events/collector_events/globalintel/config/advisories.json` | Travel/security/health RSS feeds. |
| Conflict config | `services/collector_events/collector_events/globalintel/config/conflict.json` | ACLED/GDELT config. |
| Cyber config | `services/collector_events/collector_events/globalintel/config/cyber.json` | Feodo, URLhaus, C2 tracker, OTX, AbuseIPDB. |
| Economic config | `services/collector_events/collector_events/globalintel/config/economic.json` | FRED, Eurostat, FOMC config. |
| Environment config | `services/collector_events/collector_events/globalintel/config/environment.json` | USGS, FIRMS, GDACS, EONET. |
| Market config | `services/collector_events/collector_events/globalintel/config/market.json` | Yahoo, fear/greed, Polymarket, Kalshi. |
| Reference config | `services/collector_events/collector_events/globalintel/config/reference.json` | Symbols, commodities, crypto, central bank metadata. |
| Sanctions config | `services/collector_events/collector_events/globalintel/config/sanctions.json` | OFAC SDN and Consolidated URLs. |
| Supply chain config | `services/collector_events/collector_events/globalintel/config/supply_chain.json` | Chokepoints, minerals, shipping routes. |
| Trade config | `services/collector_events/collector_events/globalintel/config/trade.json` | WTO and UN Comtrade endpoints. |

## OSINT Telegram priority

### Capture method in worldmonitor

`worldmonitor/scripts/ais-relay.cjs` is the important implementation.

- Direct Telegram capture via GramJS, not web scraping.
- Uses `TelegramClient` plus `StringSession`.
- Required relay-side env:
  - `TELEGRAM_API_ID`
  - `TELEGRAM_API_HASH`
  - `TELEGRAM_SESSION`
- Optional runtime env:
  - `TELEGRAM_CHANNEL_SET` default `full`
  - `TELEGRAM_POLL_INTERVAL_MS` default 60000
  - `TELEGRAM_MAX_FEED_ITEMS` default 200
  - `TELEGRAM_MAX_TEXT_CHARS` default 800
  - `TELEGRAM_RATE_LIMIT_MS` default 800
  - `TELEGRAM_STARTUP_DELAY_MS` default 60000
- Poll loop:
  - loads channel bucket from `data/telegram-channels.json`
  - resolves each handle with `client.getEntity(handle)`
  - fetches messages with `client.getMessages(entity, { limit, minId })`
  - skips media-only messages with no text
  - normalizes item URL as `https://t.me/{handle}/{message_id}`
  - stores rolling in-memory feed sorted by timestamp
  - serves `GET /telegram/feed?limit=&topic=&channel=`
- Safety behavior:
  - per-channel timeout: 15s
  - handles `FLOOD_WAIT`
  - disables on `AUTH_KEY_DUPLICATED` and asks for a new `TELEGRAM_SESSION`

### Capture method in globalintel

`globalintel/social/telegram.py` is a relay consumer.

- It requires:
  - `TELEGRAM_RELAY_URL`
  - `TELEGRAM_RELAY_SECRET`
- It calls:
  - `GET {relay_url}/telegram/feed?limit=...`
  - `Authorization: Bearer {shared_secret}`
- It does not call Telegram directly.
- It currently does not use the `telegram_channels` list from `config/social.json` to filter relay requests by channel. It only fetches the relay feed globally.

### Worldmonitor Telegram channels, `full` set

Source file: `worldmonitor/data/telegram-channels.json`

Metadata found:

- `version`: `1`
- `updatedAt`: `2026-03-29T00:00:00Z`
- note: `Product-managed curated list. Not user-configurable.`

| Handle | Name | Topic | Tier | Region | In globalintel |
|---|---|---:|---:|---|---|
| `VahidOnline` | Vahid Online | geopolitics | 1 | iran | no |
| `abualiexpress` | Abu Ali Express | middleeast | 2 | middleeast | no |
| `AuroraIntel` | Aurora Intel | conflict | 2 | global | no |
| `BNONews` | BNO News | breaking | 2 | global | no |
| `ClashReport` | Clash Report | conflict | 2 | global | no |
| `DeepStateUA` | DeepState | conflict | 2 | ukraine | no |
| `DefenderDome` | The Defender Dome | conflict | 2 | global | no |
| `englishabuali` | Abu Ali Express EN | middleeast | 2 | middleeast | no |
| `IranIntl_En` | Iran International EN | geopolitics | 2 | iran | no |
| `kpszsu` | Air Force of the Armed Forces of Ukraine | breaking | 2 | ukraine | no |
| `LiveUAMap` | LiveUAMap | breaking | 2 | global | no |
| `OSINTdefender` | OSINTdefender | conflict | 2 | global | no |
| `OsintUpdates` | Osint Updates | breaking | 2 | global | no |
| `bellingcat` | Bellingcat | osint | 3 | global | no |
| `CyberDetective` | CyberDetective | cyber | 3 | global | no |
| `GeopoliticalCenter` | GeopoliticalCenter | geopolitics | 3 | global | no |
| `Middle_East_Spectator` | Middle East Spectator | middleeast | 3 | middleeast | yes |
| `MiddleEastNow_Breaking` | Middle East Now Breaking | middleeast | 3 | middleeast | no |
| `nexta_tv` | NEXTA | geopolitics | 3 | europe | no |
| `OSINTIndustries` | OSINT Industries | osint | 3 | global | no |
| `Osintlatestnews` | OSIntOps News | osint | 3 | global | no |
| `osintlive` | OSINT Live | osint | 3 | global | no |
| `OsintTv` | OsintTV | geopolitics | 3 | global | no |
| `spectatorindex` | The Spectator Index | breaking | 3 | global | no |
| `wfwitness` | Witness | breaking | 3 | global | no |
| `war_monitor` | monitor | breaking | 3 | ukraine | no |
| `nayaforiraq` | Naya for Iraq | geopolitics | 3 | middleeast | no |
| `yediotnews25` | Yedioth News | breaking | 3 | middleeast | no |
| `DDGeopolitics` | DD Geopolitics | geopolitics | 3 | global | no |
| `FotrosResistancee` | Fotros Resistance | conflict | 3 | iran | no |
| `RezistanceTrench1` | Resistance Trench | conflict | 3 | middleeast | no |
| `geopolitics_prime` | Geopolitics Prime | geopolitics | 3 | global | no |
| `thecradlemedia` | The Cradle | middleeast | 3 | middleeast | no |
| `LebUpdate` | Lebanon Update | breaking | 3 | middleeast | no |
| `middleeastobserver` | Middle East Observer | geopolitics | 2 | middleeast | no |
| `MiddleEastEye_TG` | Middle East Eye | geopolitics | 2 | middleeast | no |
| `dragonwatch` | Dragon Watch | geopolitics | 2 | asia | no |

### Worldmonitor Telegram channels, `tech` set

| Handle | Name | Topic | Tier | Region | In globalintel |
|---|---|---:|---:|---|---|
| `thehackernews` | The Hacker News | cyber | 2 | global | no |
| `cybersecboardrm` | Cyber Security News | cyber | 3 | global | no |
| `securelist` | Securelist | cyber | 2 | global | no |
| `DarkWebInformer` | Dark Web Informer | cyber | 3 | global | no |
| `CYBERWARCOM` | CyberWar | cyber | 3 | global | no |
| `thecyberwire` | The CyberWire | cyber | 2 | global | no |
| `vxunderground` | vx-underground | cyber | 2 | global | no |
| `falconfeeds` | FalconFeeds | cyber | 2 | global | no |

### Globalintel Telegram channels currently configured

Source file: `globalintel/config/social.json`

Configured handles:

`intelooperr`, `nexaborisov`, `militaryblog`, `operativnozsu`, `legitimniy`, `readovkanews`, `rybar`, `SBUkraine`, `UkrainianLandForces`, `CyberBoroshno`, `maboroshchuk`, `aaborishpol`, `Middle_East_Spectator`, `MEObserverr`, `SyrianObservatory`, `WW3Monitor`, `NewResistance1`, `saborisk`.

These Python-only handles are not in `worldmonitor/data/telegram-channels.json`:

`intelooperr`, `nexaborisov`, `militaryblog`, `operativnozsu`, `legitimniy`, `readovkanews`, `rybar`, `SBUkraine`, `UkrainianLandForces`, `CyberBoroshno`, `maboroshchuk`, `aaborishpol`, `MEObserverr`, `SyrianObservatory`, `WW3Monitor`, `NewResistance1`, `saborisk`.

Recommendation: keep these Python-only channels as an extra/local bucket, but import all 45 worldmonitor channels first because they are product-managed and already have topic/tier metadata.

### Telegram gaps to fix first

1. Add the 36 missing `full` channels and 8 missing `tech` channels to Python config or to a new merged config file.
2. Add topic/tier/region/max_items metadata to Python config. `social.json` currently has a flat handle list with less source-quality metadata.
3. Make `TelegramRelayExtractor` use `channel` and `topic` relay query params, or explicitly document that the relay handles all channel filtering.
4. Update `API_KEYS_REPORT.md` for relay server credentials:
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `TELEGRAM_SESSION`
   - optional polling/rate-limit variables listed above.
5. Fix the sample relay URL in docs if needed. The actual relay path used by the code is `/telegram/feed`, not `/messages`.

## News, RSS, and news-site sources

### Worldmonitor capture method

Worldmonitor news ingestion is mostly RSS/Atom and Google News RSS, not a browser crawler.

Important behavior:

- Direct HTTP fetch first with Chrome-like `User-Agent`.
- Accepts RSS/XML content types.
- Server digest timeout: about 8s per feed.
- Batch concurrency: about 20.
- Overall digest deadline: about 25s.
- Per-feed cache key pattern: `rss:feed:v1:{variant}:{feed.url}` with TTL about 3600s.
- Digest cache key pattern: `news:digest:v1:{variant}:{lang}` with TTL about 900s.
- Fallback path uses relay/proxy for difficult feeds.
- Google News RSS is used heavily for outlets without stable/direct RSS.
- `api/rss-proxy.js` enforces a domain allowlist and has relay-only domains for feeds known to fail direct fetch.

The main source files are:

- `worldmonitor/server/worldmonitor/news/v1/_feeds.ts` - server digest sources.
- `worldmonitor/src/config/feeds.ts` - larger client/UI source config.
- `worldmonitor/shared/rss-allowed-domains.json` and `worldmonitor/api/_rss-allowed-domains.js` - allowlisted domains.
- `worldmonitor/api/rss-proxy.js` - direct/proxy fetch implementation.
- `worldmonitor/server/worldmonitor/news/v1/list-feed-digest.ts` - server fetch/parse/cache/classification logic.

### Globalintel capture method

`globalintel/feeds/rss_extractor.py`:

- Uses `aiohttp`.
- Fetches RSS/Atom directly.
- Parses XML with ElementTree.
- Uses concurrency and per-feed timeout.
- Caches a combined payload under `feeds:rss-digest:v1` with TTL around 900s.
- Does not currently implement the same domain allowlist, relay fallback, story tracking, per-feed cache, or AI/source-tier classification pipeline from `worldmonitor`.

### Count comparison

| Source list | Unique source names |
|---|---:|
| `worldmonitor/server/worldmonitor/news/v1/_feeds.ts` | 251 |
| `worldmonitor/src/config/feeds.ts` | 486 |
| `globalintel/feeds/rss_extractor.py` | 41 |

### Globalintel RSS/news sources currently present

`Reuters World`, `AP News`, `BBC World`, `CNN World`, `NPR`, `WSJ`, `France 24 EN`, `EuroNews EN`, `Le Monde EN`, `DW News EN`, `BBC Russian`, `Meduza`, `Kyiv Independent`, `TASS`, `RT`, `BBC Middle East`, `Al Jazeera`, `Guardian ME`, `BBC Asia`, `The Diplomat`, `Nikkei Asia`, `Xinhua`, `BBC Africa`, `News24 SA`, `BBC LatAm`, `Guardian Americas`, `CNBC`, `Yahoo Finance`, `Financial Times`, `Federal Reserve`, `SEC`, `UN News`, `WHO`, `Foreign Policy`, `Foreign Affairs`, `RAND`, `CrisisWatch`, `IAEA`, `War on the Rocks`, `Jamestown`, `Oil & Gas`.

### Important worldmonitor news groups not fully represented in globalintel

This is not the full 486-name client list; the complete lists live in the two config files above. These are the important missing groups found in code:

| Group | Examples present in worldmonitor and missing/underrepresented in globalintel |
|---|---|
| US mainstream/policy | PBS NewsHour, ABC News, CBS News, NBC News, Politico, The Hill, Axios, Fox News. |
| Government/security | White House, State Dept, Pentagon, Treasury, DOJ, DHS, FEMA, CISA. |
| Europe regional | Tagesschau, ANSA, NOS Nieuws, SVT Nyheter, El Pais, El Mundo, BBC Mundo, Der Spiegel, Die Zeit, Corriere della Sera, Repubblica, TVN24, Kathimerini. |
| Middle East | Oman Observer, BBC Persian, The National, Al Arabiya, Iran International, Fars News, Haaretz, Arab News, Asharq Business, Asharq News, Rudaw. |
| Africa | Africanews, Jeune Afrique, Premium Times, Vanguard Nigeria, Channels TV, Daily Trust, ThisDay, Sahel crisis feeds. |
| Latin America | Primicias, Infobae Americas, El Universo, Clarin, O Globo, Folha de S.Paulo, Mexico News Daily, Mexico Security, InSight Crime, France24 LatAm. |
| Asia/Pacific | CNA, NDTV, South China Morning Post, The Hindu, Japan Today, Asahi Shimbun, Indian Express, Bangkok Post, Thai PBS, VnExpress, Yonhap, Chosun Ilbo, ABC Australia, Guardian Australia. |
| Defense/intel | Defense One, The War Zone, Defense News, Military Times, Task & Purpose, USNI News, gCaptain, Oryx OSINT, Bellingcat, Krebs Security, Arms Control Association, Bulletin of Atomic Scientists. |
| Tech/AI/startups | Hacker News, Ars Technica, The Verge, MIT Tech Review, VentureBeat AI, ArXiv AI/ML, TechCrunch, Crunchbase News, YC Blog, a16z, First Round Review, Sequoia Blog, Sifted, Tech.eu, Tech in Asia, TechCabal, Inc42. |
| Cyber RSS | The Hacker News, Securelist, Dark Reading, Ransomware.live, Krebs Security and related cyber/news feeds. |
| Finance/crypto | MarketWatch, Reuters Business, Seeking Alpha, Forex News, Bond Market, Gold/Metals, CoinDesk, Cointelegraph, The Block, Decrypt, Blockworks, The Defiant, Bitcoin Magazine, DL News, CryptoSlate, Wu Blockchain. |
| Energy/commodities | Reuters Energy, Nuclear Energy, Mining and resources feeds, oil/gas Google News queries. |
| Layoffs/startup distress | Layoffs.fyi, TechCrunch Layoffs, Google News layoffs queries. |

## Twitter/X accounts

No operational Twitter/X source ingestion was found.

Observed references are not source capture:

- share URLs such as `https://x.com/intent/tweet`
- contact/social links such as `https://x.com/eliehabib`
- OpenGraph/Twitter card metadata
- localized UI labels for "twitter" or "X"

Missing if desired:

- a Twitter/X account config file
- API bearer token config
- `api.twitter.com` calls
- scraper implementation
- account allowlist
- rate-limit/backoff handling
- legal/ToS review for scraping

## Globalintel subdirectory inventory and gap analysis

### `advisories`

Python:

- Config: `globalintel/config/advisories.json`
- Extractor: `globalintel/advisories/advisory.py`
- Method: RSS/Atom direct fetch.
- Sources include US State Dept, UK FCDO, selected US Embassy security feeds, CDC, WHO, ECDC.

Worldmonitor:

- Script: `worldmonitor/scripts/seed-security-advisories.mjs`
- Method: RSS/Atom via direct fetch or relay proxy.
- Actual code sources include:
  - US State Dept `https://travel.state.gov/_res/rss/TAsTWs.xml`
  - UK FCDO `https://www.gov.uk/foreign-travel-advice.atom`
  - US Embassy Thailand, UAE, Germany, Ukraine, Mexico, India, Pakistan, Colombia, Poland, Bangladesh, Italy, Dominican Republic, Myanmar
  - CDC Travel Notices
  - ECDC Epidemiological Updates, Threats Report, Risk Assessments, Avian Influenza, Publications
  - WHO News
  - WHO Africa Emergencies

Gaps:

- Python uses a different embassy set: Iraq, Afghanistan, Ukraine, Israel, Haiti, South Sudan, Niger, Chad, Libya, Sudan, Yemen, Somalia, Syria.
- Worldmonitor's actual embassy list should be added if the goal is parity.
- Some Python URLs use country names like `ukraine.usembassy.gov`; worldmonitor uses shorter country-code domains like `ua.usembassy.gov`. These should be verified.
- Docs mention some government feeds that are not in the current seed script; code should be treated as source of truth.

### `conflict`

Python:

- `ACLEDExtractor`
- `GDELTIntelExtractor`
- `GDELTUnrestExtractor`
- `UCDPExtractor`
- `UnrestMergeExtractor`
- `DisplacementExtractor`
- `GPSJammingExtractor`

Main sources/methods:

- ACLED REST API with OAuth/static token.
- GDELT Doc API and GKG GeoJSON/API.
- UCDP GED API.
- UNHCR population API.
- Wingbits GPS jam API.

Worldmonitor relevant scripts:

- `seed-conflict-intel.mjs`
- `seed-gdelt-intel.mjs`
- `seed-unrest-events.mjs`
- `seed-ucdp-events.mjs`
- `seed-displacement-summary.mjs`
- `seed-iran-events.mjs`
- `seed-military-flights.mjs` for OpenSky/Wingbits military flight context.

Worldmonitor extra sources:

- HAPI/HDX conflict-events coordination context.
- PizzINT dashboard and GDELT pair tension API.
- LiveUAMap-style Iran events, implemented as manual/static seed in `seed-iran-events.mjs`.
- More operational flight context through OpenSky/Wingbits in aviation/military scripts.

Gaps:

- `globalintel/config/conflict.json` points to `https://api.acleddata.com/...`, while some Python code hardcodes `https://acleddata.com/...`. This should be reconciled.
- HAPI/HDX, PizzINT, and LiveUAMap/manual Iran layer are not represented in Python.
- Worldmonitor has more cross-source conflict correlation scripts that are not present in the Python folder.

### `cyber`

Python:

- Config: `globalintel/config/cyber.json`
- Extractor: `globalintel/cyber/cyber_threats.py`
- Sources:
  - Feodo Tracker
  - URLhaus
  - C2 Tracker GitHub CSV
  - AlienVault OTX
  - AbuseIPDB

Worldmonitor:

- Script: `worldmonitor/scripts/seed-cyber-threats.mjs`
- Uses:
  - Feodo JSON blocklist
  - URLhaus recent API
  - C2IntelFeeds CSV
  - OTX IPv4 indicators export
  - AbuseIPDB blacklist
  - IP enrichment via `ipinfo.io` and `freeipapi.com`
- Client feeds also include cyber RSS such as `Ransomware.live`.

Gaps:

- Python uses a different C2 feed URL (`montysecurity/C2-Tracker`) than worldmonitor (`drb-ra/C2IntelFeeds`).
- Python does not include the same IP geolocation enrichment fallback.
- `Ransomware.live` is present in worldmonitor RSS/client config but not in Python RSS list.
- Python should add source-version metadata and rate-limit/caching parity for OTX/AbuseIPDB.

### `economic`

Python:

- FRED release/calendar data.
- BIS policy/exchange/credit.
- ECB FX/yield/stress.
- BLS.
- World Bank.
- EIA crude/natural gas.
- GIE EU gas.
- FAO food price.
- Economic stress from FRED/Yahoo.
- Fuel price extractor exists but is marked placeholder.

Worldmonitor relevant scripts include:

- `seed-economic-calendar.mjs`
- `seed-bis-data.mjs`
- `seed-bls-series.mjs`
- `seed-ecb-fx-rates.mjs`
- `seed-ecb-short-rates.mjs`
- `seed-eurostat-country-data.mjs`
- `seed-fao-food-price-index.mjs`
- `seed-fsi-eu.mjs`
- `seed-gie-gas-storage.mjs`
- `seed-usa-spending.mjs`
- `seed-imf-macro.mjs`
- `seed-jodi-oil.mjs`
- `seed-jodi-gas.mjs`
- `seed-iea-oil-stocks.mjs`
- `seed-owid-energy-mix.mjs`
- `seed-national-debt.mjs`
- `seed-bigmac.mjs`
- `seed-grocery-basket.mjs`
- `seed-fuel-prices.mjs`

Gaps:

- Python has good coverage for core macro APIs, but lacks many worldmonitor economic/energy panels: USA Spending, IMF macro, JODI oil/gas, IEA oil stocks, OWID energy mix, national debt, Big Mac, grocery basket, electricity prices, fuel price production implementation.
- `globalintel/config/economic.json` includes Eurostat config, but parity should be checked against actual extractor coverage.

### `environment`

Python:

- USGS earthquakes.
- NASA FIRMS country CSV.
- GDACS RSS.
- NASA EONET.

Worldmonitor:

- `seed-earthquakes.mjs` uses USGS GeoJSON.
- `seed-fire-detections.mjs` uses NASA FIRMS area CSV for VIIRS SNPP/NOAA20/NOAA21.
- `seed-natural-events.mjs` uses EONET, GDACS MAP API, and NOAA/NHC tropical weather ArcGIS layers.
- `seed-climate-anomalies.mjs` uses Open-Meteo archive.
- `seed-climate-disasters.mjs`, `seed-climate-news.mjs`, `seed-climate-ocean-ice.mjs`, `seed-co2-monitoring.mjs`, `seed-health-air-quality.mjs`, `seed-weather-alerts.mjs`.
- Air quality uses OpenAQ v3 and optional WAQI.

Gaps:

- Python lacks NOAA/NHC tropical weather.
- Python FIRMS uses country CSV; worldmonitor uses area/bbox and multiple FIRMS sources.
- Python lacks Open-Meteo climate anomalies, climate disasters/news, ocean/ice, CO2 monitoring, OpenAQ/WAQI air quality, and weather alerts.

### `feeds`

Python:

- `RSSFeedExtractor` with 41 feeds.
- Direct RSS/Atom fetch.

Worldmonitor:

- 251 server source names in `_feeds.ts`.
- 486 client source names in `src/config/feeds.ts`.
- Domain allowlist plus relay-only handling.
- Google News RSS source strategy.
- Source type/risk metadata and digest classification.

Gaps:

- Expand Python RSS source inventory significantly.
- Add proxy/relay fallback for known hard feeds.
- Add allowlist to avoid arbitrary outbound fetch if this is exposed via API.
- Add source tier/source type/propaganda-risk metadata or import from worldmonitor.
- Consider per-feed cache instead of only aggregate cache.

### `market`

Python:

- Yahoo chart for FX, stocks, ETFs, commodities, country indices.
- Finnhub quote/earnings when key is present.
- CoinGecko/CoinPaprika for crypto.
- Polymarket Gamma API.
- Kalshi API.
- CNN fear/greed, AAII, Barchart, Yahoo symbols.
- CFTC COT.

Worldmonitor:

- `seed-market-quotes.mjs`: Finnhub plus Yahoo fallback.
- `seed-commodity-quotes.mjs`: Alpha Vantage/Yahoo style commodity flow.
- `seed-crypto-quotes.mjs`: CoinGecko plus CoinPaprika fallback.
- `seed-fear-greed.mjs`: Yahoo, Barchart, CNN dataviz, AAII, FRED macro signals.
- `seed-prediction-markets.mjs`: Polymarket Gamma and Kalshi.
- `seed-etf-flows.mjs`, `seed-cot.mjs`, `seed-gulf-quotes.mjs`, `seed-stablecoin-markets.mjs`, `seed-token-panels.mjs`, `seed-crypto-sectors.mjs`.

Gaps:

- Python is broadly aligned on common market APIs, but should compare exact symbols and sector/group configs in `reference.json` against worldmonitor market config files.
- Worldmonitor has more token/sector/panel coverage and more fallback behavior.
- Prediction market Kalshi endpoint differs: Python config uses `https://api.elections.kalshi.com/v1/events`, while worldmonitor uses `https://api.elections.kalshi.com/trade-api/v2/events`.

### `sanctions`

Python:

- OFAC SDN XML.
- OFAC Consolidated XML.
- Aggregates country pressure.

Worldmonitor:

- `seed-sanctions-pressure.mjs`
- Uses OFAC Advanced XML:
  - `sdn_advanced.xml`
  - `cons_advanced.xml`
- Stream parses large XML.
- Builds entity index, country counts, vessel/aircraft counts, new-entry tracking, and pressure state.

Gaps:

- Python uses publication preview XML URLs, not the same Advanced XML URLs.
- Python appears to aggregate country counts but lacks full entity index, vessel/aircraft enrichment, new-entry pressure state, and stateful diff tracking.

### `social`

Python:

- Reddit velocity extractor.
- Telegram relay extractor.
- Config: `social.json`

Worldmonitor:

- Telegram relay with GramJS.
- Product-managed Telegram channel list.
- News/RSS and Telegram are separate paths.
- No real Twitter/X ingestion found.

Gaps:

- Telegram channel parity is the highest priority.
- Python relay client should support channel/topic filters.
- Reddit does not appear to be a major worldmonitor parity source compared with Telegram and RSS/news.

### `supply_chain`

Python:

- Curated chokepoints, critical minerals, shipping routes.
- Shipping rates/stress extractors, including FRED BDIY for stress.

Worldmonitor:

- `seed-portwatch.mjs`: IMF PortWatch ArcGIS Daily Chokepoints Data.
- `seed-portwatch-disruptions.mjs`: IMF PortWatch disruptions ArcGIS layer.
- `seed-portwatch-port-activity.mjs`
- `seed-portwatch-chokepoints-ref.mjs`
- `seed-supply-chain-trade.mjs`: FRED, WTO, Shanghai Shipping Exchange, Baltic Dry Index page, Budget Lab tariffs, US Treasury customs duties.
- `seed-chokepoint-baselines.mjs`, `seed-chokepoint-flows.mjs`, `seed-hormuz.mjs`.

Gaps:

- Python lacks IMF PortWatch ArcGIS feeds.
- Python lacks Shanghai Shipping Exchange and Budget Lab tariff scrape/parse.
- Python lacks richer chokepoint baseline/flow and Hormuz-specific seed logic.
- Python shipping rates are more static/synthetic compared with worldmonitor's mixed API/scrape sources.

### `trade`

Python:

- WTO timeseries API.
- UN Comtrade API.
- Tariff trends.

Worldmonitor:

- `seed-trade-flows.mjs`: UN Comtrade public preview API.
- `seed-supply-chain-trade.mjs`: WTO, tariff indicators, Treasury customs duties, Budget Lab tariff HTML parse, shipping indicators.

Gaps:

- Python Comtrade endpoint differs from worldmonitor's public preview endpoint.
- Python lacks Budget Lab tariff parsing and US Treasury customs duties.
- Python has WTO coverage but should be checked against worldmonitor indicator IDs and reporter lists.

### `reference`

Python:

- `reference.json` has stock symbols, commodities, crypto IDs, DeFi/AI/other token IDs, stablecoins, sector ETFs, BTC spot ETFs, Gulf symbols, crypto sectors, central banks, BIS codes.

Worldmonitor:

- Similar reference data appears spread across frontend config and seed utilities rather than one equivalent file.

Gaps:

- Reconcile exact market symbols, crypto IDs, token sectors, and Gulf symbols against worldmonitor config files to avoid silent drift.

### `data`

Python:

- Contains output/test data artifacts, not primary source config.

Worldmonitor:

- `worldmonitor/data/telegram-channels.json` is a primary source config and should be imported or mirrored.

Gaps:

- Do not treat generated data dumps as source truth. For Telegram, use `worldmonitor/data/telegram-channels.json`.

## Worldmonitor source domains not currently mapped to globalintel subdirs

The Python `globalintel` tree covers many source classes, but worldmonitor has several additional source families that would require new extractors or new subdirectories:

| Domain | Worldmonitor examples | Capture method |
|---|---|---|
| Aviation | `seed-aviation.mjs`, `seed-airport-delays.mjs`, `seed-military-flights.mjs` | AviationStack API, FAA NAS status XML/API, ICAO NOTAM API, OpenSky API/OAuth, Wingbits API, aviation RSS. |
| Webcams | `seed-webcams.mjs` | Windy Webcams API v3. |
| Internet/outages | `seed-internet-outages.mjs`, `seed-service-statuses.mjs` | Service APIs/status pages, likely mixed HTTP/API. |
| Infrastructure | `seed-infra.mjs`, `seed-submarine-cables.mjs`, `seed-military-bases.mjs` | Curated/static plus APIs depending on layer. |
| Climate/health | `seed-climate-*`, `seed-health-air-quality.mjs`, `seed-weather-alerts.mjs` | Open-Meteo, OpenAQ, WAQI, NOAA/NHC, other RSS/API. |
| Research/events | `seed-research.mjs`, `seed-defense-patents.mjs` | Curated events plus public APIs/scrapes. |
| Thermal/escalation/correlation | `seed-thermal-escalation.mjs`, `seed-cross-source-signals.mjs`, `seed-correlation.mjs` | Derived analytics over existing feeds. |

## Documentation drift

Observed drift between docs and code:

- `Developer_Guide_Data_Sources.md` says Telegram has 26 curated channels. Actual `worldmonitor/data/telegram-channels.json` has 45 channels.
- `API_KEYS_REPORT.md` documents only `TELEGRAM_RELAY_URL` and `TELEGRAM_RELAY_SECRET` for Python relay consumption. It does not document the relay implementation credentials required by `worldmonitor/scripts/ais-relay.cjs`.
- News/feed counts in docs should be treated as approximate. The actual code currently exposes 251 unique server source names and 486 unique client config source names.
- Some Python configs use endpoints that differ from worldmonitor scripts, notably ACLED, Kalshi, OFAC, Comtrade, and C2 tracker.

## Recommended implementation order

1. Telegram parity:
   - Import all 45 `worldmonitor` Telegram channels.
   - Preserve the 17 Python-only Telegram handles as an extra bucket.
   - Add metadata fields: `topic`, `tier`, `region`, `max_items`.
   - Update relay extractor to filter by `channel`/`topic` or document all-channel relay behavior.

2. Telegram documentation:
   - Update `docs/API_KEYS_REPORT.md` with relay-side GramJS credentials.
   - Correct sample endpoint to `/telegram/feed`.
   - Add operational warning about `AUTH_KEY_DUPLICATED` and one active session per Telegram StringSession.

3. RSS/news parity:
   - Import server `_feeds.ts` sources first, then decide whether the larger client `src/config/feeds.ts` should be mirrored.
   - Add `rss-allowed-domains` equivalent.
   - Add relay fallback behavior for feeds that fail direct fetch.
   - Add source tier/type/risk metadata.

4. Cyber parity:
   - Align Feodo/URLhaus/C2 source URLs with worldmonitor.
   - Add `Ransomware.live` via RSS/news config.
   - Add IP enrichment fallback if operationally useful.

5. Advisory parity:
   - Add worldmonitor embassy and health feed set.
   - Verify the Python embassy URL patterns before running in production.

6. Trade/supply-chain parity:
   - Add IMF PortWatch ArcGIS feeds.
   - Add Budget Lab tariff parsing and Treasury customs duties.
   - Align WTO/Comtrade endpoints and reporter/indicator lists.

7. Optional new domains:
   - Aviation/OpenSky/Wingbits/FAA/ICAO.
   - Climate/Open-Meteo/OpenAQ/WAQI/NOAA.
   - Webcams/Windy.
   - Infrastructure/submarine cables/service status.

