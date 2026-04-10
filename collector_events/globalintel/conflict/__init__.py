"""Conflict intelligence extractors: ACLED and GDELT.

Data sources (mirrors worldmonitor seed-conflict-intel.mjs + seed-gdelt-intel.mjs):

ACLED — Armed Conflict Location & Event Data
    Auth: OAuth2 token from ``POST /oauth/token`` (email+password, grant_type=password,
          client_id=acled).  Falls back to static ``ACLED_ACCESS_TOKEN``.
    API:  ``GET https://acleddata.com/api/acled/read``
    Data: battles, explosions, violence against civilians — 30-day window, 500 events max.
    20 monitored conflict countries matching worldmonitor.

GDELT — Global Database of Events, Language, and Tone
    Doc API:  ``GET https://api.gdeltproject.org/api/v2/doc/doc``  (intel articles)
    GKG API:  ``GET https://api.gdeltproject.org/api/v1/gkg_geojson`` (unrest geolocation)
    6 intel topics: military, cyber, nuclear, sanctions, intelligence, maritime.
    Retry: 3 retries with exponential backoff (60s→120s→240s).
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

# ─── ACLED ───────────────────────────────────────────────────────────

ACLED_OAUTH_URL = "https://acleddata.com/oauth/token"
ACLED_API_URL = "https://acleddata.com/api/acled/read"
ACLED_CONFLICT_COUNTRIES = [
    "Afghanistan", "Central African Republic", "Democratic Republic of Congo",
    "Ethiopia", "Iraq", "Libya", "Mali", "Myanmar", "Mozambique", "Niger",
    "Nigeria", "Pakistan", "Somalia", "Sudan", "South Sudan", "Syria",
    "Ukraine", "Yemen", "Haiti", "Burkina Faso",
]
ACLED_CONFLICT_TYPES = [
    "Battles",
    "Explosions/Remote violence",
    "Violence against civilians",
]


class ACLEDExtractor(BaseExtractor):
    """Fetches conflict events from ACLED with OAuth2 authentication.

    Auth flow (mirrors worldmonitor acled-oauth.mjs):
        1. POST /oauth/token  {username, password, grant_type=password, client_id=acled}
           → access_token
        2. Falls back to static access_token if OAuth fails.
        3. Falls back to legacy email+key params if both fail.

    Severity: HIGH if fatalities > 0 or riot/clash sub-type, else MEDIUM.
    """

    SOURCE = "acled"
    DOMAIN = "conflict"
    REDIS_KEY = "conflict:acled:v1"
    TTL_SECONDS = 900  # 15 min — matches worldmonitor

    def __init__(
        self,
        email: str = "",
        api_key: str = "",
        access_token: str = "",
        password: str = "",
        limit: int = 500,
    ):
        super().__init__()
        self._email = email
        self._api_key = api_key
        self._access_token = access_token
        self._password = password or api_key  # password for OAuth, falls back to api_key
        self._limit = limit

    async def _get_oauth_token(self, session: aiohttp.ClientSession) -> str | None:
        """Attempt OAuth2 password grant to obtain a fresh access_token."""
        if not self._email or not self._password:
            return None
        try:
            payload = {
                "username": self._email,
                "password": self._password,
                "grant_type": "password",
                "client_id": "acled",
            }
            async with session.post(ACLED_OAUTH_URL, data=payload) as resp:
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    token = body.get("access_token")
                    if token:
                        return token
        except Exception as exc:
            logger.debug("ACLED OAuth failed: %s", exc)
        return None

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Resolve auth — OAuth → static token → legacy email+key
        token = await self._get_oauth_token(session)
        if not token:
            token = self._access_token

        since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        params: dict = {
            "event_date": f"{since}|",
            "event_date_where": "BETWEEN",
            "event_type": "|".join(ACLED_CONFLICT_TYPES),
            "limit": str(self._limit),
        }
        if token:
            params["acled_access"] = token
        else:
            if self._email:
                params["email"] = self._email
            if self._api_key:
                params["key"] = self._api_key

        async with session.get(ACLED_API_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        events = data.get("data", [])
        items: list[IntelItem] = []
        for ev in events:
            fatalities = int(ev.get("fatalities", 0) or 0)
            sub_type = (ev.get("sub_event_type") or "").lower()
            severity = "HIGH" if fatalities > 0 or "riot" in sub_type or "clash" in sub_type else "MEDIUM"
            items.append(IntelItem(
                id=f"acled:{ev.get('data_id', '')}",
                source="acled",
                domain="conflict",
                title=f"{ev.get('event_type', '')}: {ev.get('notes', '')[:200]}",
                lat=_safe_float(ev.get("latitude")),
                lon=_safe_float(ev.get("longitude")),
                country=ev.get("country", ""),
                ts=ev.get("event_date", ""),
                severity=severity,
                tags=[ev.get("event_type", ""), ev.get("sub_event_type", "")],
                extra={
                    "event_type": ev.get("event_type", ""),
                    "sub_event_type": ev.get("sub_event_type", ""),
                    "fatalities": fatalities,
                    "actor1": ev.get("actor1", ""),
                    "actor2": ev.get("actor2", ""),
                    "admin1": ev.get("admin1", ""),
                    "source": ev.get("source", ""),
                },
            ))
        return items


# ─── GDELT intelligence articles ────────────────────────────────────

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

GDELT_TOPICS: dict[str, str] = {
    "military": '(military exercise OR troop deployment OR airstrike OR "military operation")',
    "cyber": '(cyberattack OR "data breach" OR ransomware OR "cyber espionage")',
    "nuclear": '(nuclear weapon OR nuclear test OR ICBM OR "nuclear program")',
    "sanctions": '(sanctions OR embargo OR "trade restriction" OR "asset freeze")',
    "intelligence": '(espionage OR intelligence OR spy OR surveillance OR "covert operation")',
    "maritime": '(naval OR warship OR "freedom of navigation" OR "South China Sea")',
}


class GDELTIntelExtractor(BaseExtractor):
    """Fetches intelligence articles from GDELT Doc API.

    Mirrors worldmonitor seed-gdelt-intel.mjs:
      - 6 thematic topics with boolean queries
      - 75 articles per topic, 14-day window
      - Retry with exponential backoff (60s → 120s → 240s) on failures
      - 20s delay between topics to respect GDELT rate limits
    """

    SOURCE = "gdelt"
    DOMAIN = "conflict"
    REDIS_KEY = "intelligence:gdelt-intel:v1"
    TTL_SECONDS = 86400  # 24h

    GDELT_MAX_RETRIES = 3
    GDELT_BACKOFF_BASE_S = 60  # 60s → 120s → 240s

    def __init__(self, topics: dict[str, str] | None = None, rate_limit_s: float = 20):
        super().__init__()
        self._topics = topics or GDELT_TOPICS
        self._rate_limit_s = rate_limit_s

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        all_items: list[IntelItem] = []
        for topic_key, query in self._topics.items():
            articles = await self._fetch_topic(session, topic_key, query)
            all_items.extend(articles)
            if self._rate_limit_s > 0:
                await asyncio.sleep(self._rate_limit_s)
        return all_items

    async def _fetch_topic(
        self,
        session: aiohttp.ClientSession,
        topic_key: str,
        query: str,
    ) -> list[IntelItem]:
        """Fetch a single GDELT topic with exponential backoff retry."""
        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": "75",
            "format": "json",
            "sort": "datedesc",
            "timespan": "14d",
        }
        last_exc: Exception | None = None
        for attempt in range(self.GDELT_MAX_RETRIES):
            try:
                async with session.get(GDELT_DOC_API, params=params) as resp:
                    if resp.status == 429:
                        wait = self.GDELT_BACKOFF_BASE_S * (2 ** attempt)
                        logger.warning("GDELT 429 on %s — backoff %ds", topic_key, wait)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                return self._parse_articles(data, topic_key)
            except Exception as exc:
                last_exc = exc
                wait = self.GDELT_BACKOFF_BASE_S * (2 ** attempt)
                logger.warning("GDELT %s attempt %d failed: %s — retry in %ds", topic_key, attempt + 1, exc, wait)
                await asyncio.sleep(wait)

        logger.error("GDELT topic %s all retries exhausted: %s", topic_key, last_exc)
        return []

    def _parse_articles(self, data: dict, topic_key: str) -> list[IntelItem]:
        items: list[IntelItem] = []
        for art in data.get("articles", []):
            url = art.get("url", "")
            items.append(IntelItem(
                id=f"gdelt:{topic_key}:{url[:80]}",
                source="gdelt",
                domain="conflict",
                title=art.get("title", "")[:300],
                url=url,
                ts=art.get("seendate", ""),
                tags=[topic_key],
                extra={
                    "topic": topic_key,
                    "tone": art.get("tone", 0),
                    "source_domain": art.get("domain", ""),
                    "language": art.get("language", ""),
                    "image": art.get("socialimage", ""),
                },
            ))
        return items


# ─── GDELT Unrest geolocation ───────────────────────────────────────

GDELT_GKG_URL = "https://api.gdeltproject.org/api/v1/gkg_geojson"

# Keyword sets for severity classification matching worldmonitor
_RIOT_KEYWORDS = {"riot", "clash", "violent protest", "looting", "arson"}


class GDELTUnrestExtractor(BaseExtractor):
    """Fetches protest/unrest geolocation data from GDELT GKG.

    Mirrors worldmonitor's GDELT component of seed-unrest-events.mjs:
      - Query: ``protest OR riot OR demonstration OR strike``
      - maxrows=2500 → aggregate by 0.1° grid
      - Severity: count>100 or riot/clash → HIGH, count<25 → LOW, else MEDIUM
      - Filtered: count >= 5 (matches worldmonitor threshold)
    """

    SOURCE = "gdelt_unrest"
    DOMAIN = "conflict"
    REDIS_KEY = "unrest:gdelt-gkg:v1"
    TTL_SECONDS = 16200  # 4.5h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        params = {
            "QUERY": "protest OR riot OR demonstration OR strike",
            "OUTPUTTYPE": "2",
            "MAXROWS": "2500",
        }
        try:
            async with session.get(GDELT_GKG_URL, params=params) as resp:
                if resp.status != 200:
                    logger.warning("GDELT GKG returned %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("GDELT GKG failed: %s", exc)
            return []

        # Aggregate by 0.1° grid
        grid: dict[str, dict] = {}
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [0, 0])
            lat = coords[1] if len(coords) > 1 else 0
            lon = coords[0] if len(coords) > 0 else 0
            count = int(props.get("count", 0) or 0)
            name = props.get("name", "Unknown")

            # Grid key at 0.1° resolution (~10 km)
            grid_key = f"{round(lat * 10) / 10:.1f}:{round(lon * 10) / 10:.1f}"
            if grid_key in grid:
                grid[grid_key]["count"] += count
                if len(name) > len(grid[grid_key]["name"]):
                    grid[grid_key]["name"] = name  # keep longest name
            else:
                grid[grid_key] = {"lat": lat, "lon": lon, "name": name, "count": count}

        items: list[IntelItem] = []
        for gk, cell in grid.items():
            count = cell["count"]
            if count < 5:  # worldmonitor threshold
                continue
            name_lower = cell["name"].lower()
            if count > 100 or any(k in name_lower for k in _RIOT_KEYWORDS):
                severity = "HIGH"
            elif count < 25:
                severity = "LOW"
            else:
                severity = "MEDIUM"
            items.append(IntelItem(
                id=f"gdelt_unrest:{gk}",
                source="gdelt_unrest",
                domain="conflict",
                title=cell["name"],
                lat=cell["lat"],
                lon=cell["lon"],
                severity=severity,
                tags=["unrest", "gdelt"],
                extra={"count": count, "grid": gk},
            ))
        # Sort: severity desc then count desc
        _sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        items.sort(key=lambda x: (_sev_order.get(x.severity, 9), -x.extra.get("count", 0)))
        return items


# ─── helpers ─────────────────────────────────────────────────────────

def _safe_float(val: object) -> float | None:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
