"""Dual-source unrest/protest merge extractor: ACLED protests + GDELT GKG.

Mirrors worldmonitor seed-unrest-events.mjs — the authoritative implementation
for global protest/unrest tracking:

1. Fetches ACLED *protest* events (event_type=Protests, 30-day window, 500 cap).
2. Fetches GDELT GKG geojson (protest+riot+demonstration+strike, 2500 rows).
3. Deduplicates on a 0.1° lat/lon grid + same-day key.
   - ACLED always wins over GDELT in the same cell.
   - Multi-source agreement → CONFIDENCE_HIGH.
4. Severity: fatalities>0 OR riot → HIGH; protest → MEDIUM; else LOW.
   GDELT: count>100 or riot/clash keyword → HIGH, count<25 → LOW.
5. Output sorted by severity (HIGH > MEDIUM > LOW) then recency descending.
"""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_loggerimport math
from datetime import datetime, timedelta, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = get_logger(__name__)

# ─── APIs ────────────────────────────────────────────────────────────

ACLED_OAUTH_URL = "https://acleddata.com/oauth/token"
ACLED_API_URL = "https://acleddata.com/api/acled/read"
GDELT_GKG_URL = "https://api.gdeltproject.org/api/v1/gkg_geojson"

_RIOT_KEYWORDS = frozenset({"riot", "clash", "violent protest", "looting", "arson"})


def _grid_key(lat: float, lon: float, date_str: str) -> str:
    """Generate a deduplication key at ~10 km (0.1°) resolution + same day."""
    glat = round(lat * 10) / 10
    glon = round(lon * 10) / 10
    return f"{glat:.1f}:{glon:.1f}:{date_str[:10]}"


class UnrestMergeExtractor(BaseExtractor):
    """Merges ACLED protest events with GDELT GKG unrest geolocations.

    Deduplication: 0.1° grid + same-day matching.
    ACLED always takes priority over GDELT in the same cell.
    Both sources confirming the same location → confidence = HIGH.
    """

    SOURCE = "unrest_merged"
    DOMAIN = "conflict"
    REDIS_KEY = "unrest:events:v1"
    TTL_SECONDS = 16200  # 4.5 hours

    def __init__(
        self,
        acled_email: str = "",
        acled_api_key: str = "",
        acled_access_token: str = "",
    ):
        super().__init__()
        self._email = acled_email
        self._api_key = acled_api_key
        self._access_token = acled_access_token

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Fetch both sources concurrently
        acled_task = self._fetch_acled(session)
        gdelt_task = self._fetch_gdelt(session)
        acled_raw, gdelt_raw = await asyncio.gather(
            acled_task, gdelt_task, return_exceptions=True,
        )
        if isinstance(acled_raw, Exception):
            logger.warning("UnrestMerge ACLED failed: %s", acled_raw)
            acled_raw = []
        if isinstance(gdelt_raw, Exception):
            logger.warning("UnrestMerge GDELT failed: %s", gdelt_raw)
            gdelt_raw = []

        # Merge with deduplication
        return self._merge(acled_raw, gdelt_raw)

    # ─── ACLED protests ──────────────────────────────────────────────

    async def _get_oauth_token(self, session: aiohttp.ClientSession) -> str | None:
        if not self._email or not self._api_key:
            return None
        try:
            payload = {
                "username": self._email,
                "password": self._api_key,
                "grant_type": "password",
                "client_id": "acled",
            }
            async with session.post(ACLED_OAUTH_URL, data=payload) as resp:
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    return body.get("access_token")
        except Exception:
            pass
        return None

    async def _fetch_acled(
        self, session: aiohttp.ClientSession,
    ) -> list[dict]:
        """Fetch ACLED protest events (30-day window, max 500)."""
        token = await self._get_oauth_token(session)
        if not token:
            token = self._access_token

        since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        params: dict = {
            "event_date": f"{since}|",
            "event_date_where": "BETWEEN",
            "event_type": "Protests",
            "limit": "500",
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
        return data.get("data", [])

    # ─── GDELT GKG ──────────────────────────────────────────────────

    async def _fetch_gdelt(self, session: aiohttp.ClientSession) -> list[dict]:
        """Fetch GDELT GKG protest geolocations, aggregate by 0.1° grid."""
        params = {
            "QUERY": "protest OR riot OR demonstration OR strike",
            "OUTPUTTYPE": "2",
            "MAXROWS": "2500",
        }
        async with session.get(GDELT_GKG_URL, params=params) as resp:
            if resp.status != 200:
                return []
            data = await resp.json(content_type=None)

        # Aggregate by 0.1° grid
        grid: dict[str, dict] = {}
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [0, 0])
            lat = coords[1] if len(coords) > 1 else 0
            lon = coords[0] if len(coords) > 0 else 0
            count = int(props.get("count", 0) or 0)
            name = props.get("name", "Unknown")
            gk = _grid_key(lat, lon, today)
            if gk in grid:
                grid[gk]["count"] += count
            else:
                grid[gk] = {"lat": lat, "lon": lon, "name": name, "count": count, "date": today}
        return [v for v in grid.values() if v["count"] >= 5]

    # ─── Merge ───────────────────────────────────────────────────────

    def _merge(
        self,
        acled_events: list[dict],
        gdelt_cells: list[dict],
    ) -> list[IntelItem]:
        """Grid-based deduplication: ACLED priority, multi-source → HIGH confidence."""
        merged: dict[str, IntelItem] = {}
        acled_keys: set[str] = set()

        # ACLED first (priority)
        for ev in acled_events:
            lat = _safe_float(ev.get("latitude"))
            lon = _safe_float(ev.get("longitude"))
            if lat is None or lon is None:
                continue
            date_str = ev.get("event_date", "")[:10]
            gk = _grid_key(lat, lon, date_str)
            acled_keys.add(gk)

            fatalities = int(ev.get("fatalities", 0) or 0)
            sub_type = (ev.get("sub_event_type") or "").lower()
            if fatalities > 0 or "riot" in sub_type or "clash" in sub_type:
                severity = "HIGH"
            else:
                severity = "MEDIUM"

            merged[gk] = IntelItem(
                id=f"unrest:acled:{ev.get('data_id', '')}",
                source="acled",
                domain="conflict",
                title=f"{ev.get('sub_event_type', 'Protest')}: {ev.get('notes', '')[:200]}",
                lat=lat,
                lon=lon,
                country=ev.get("country", ""),
                ts=date_str,
                severity=severity,
                tags=["unrest", "protest", "acled"],
                extra={
                    "source_provider": "acled",
                    "sub_event_type": ev.get("sub_event_type", ""),
                    "fatalities": fatalities,
                    "actor1": ev.get("actor1", ""),
                    "notes": ev.get("notes", "")[:300],
                },
            )

        # GDELT — only add if not already covered by ACLED
        for cell in gdelt_cells:
            gk = _grid_key(cell["lat"], cell["lon"], cell["date"])
            if gk in acled_keys:
                # Multi-source confirmation → boost existing ACLED entry
                if gk in merged:
                    merged[gk].extra["multi_source"] = True
                    merged[gk].extra["confidence"] = "HIGH"
                    merged[gk].extra["gdelt_count"] = cell["count"]
                continue

            count = cell["count"]
            name_lower = cell["name"].lower()
            if count > 100 or any(k in name_lower for k in _RIOT_KEYWORDS):
                severity = "HIGH"
            elif count < 25:
                severity = "LOW"
            else:
                severity = "MEDIUM"

            merged[gk] = IntelItem(
                id=f"unrest:gdelt:{gk}",
                source="gdelt_gkg",
                domain="conflict",
                title=cell["name"],
                lat=cell["lat"],
                lon=cell["lon"],
                ts=cell["date"],
                severity=severity,
                tags=["unrest", "protest", "gdelt"],
                extra={
                    "source_provider": "gdelt",
                    "count": count,
                    "grid": gk,
                },
            )

        # Sort: severity desc → date desc
        _sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        items = list(merged.values())
        items.sort(key=lambda x: (_sev_order.get(x.severity, 9), x.ts or ""), reverse=False)
        # re-sort so HIGH first, then within severity by recency desc
        items.sort(key=lambda x: (_sev_order.get(x.severity, 9), -(hash(x.ts) if x.ts else 0)))
        return items


def _safe_float(val: object) -> float | None:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
