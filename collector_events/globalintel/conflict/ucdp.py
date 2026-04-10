"""Uppsala Conflict Data Program (UCDP) georeferenced event extractor.

Mirrors worldmonitor seed-ucdp-events.mjs:
    API:  ``GET https://ucdpapi.pcr.uu.se/api/gedevents/{version}``
    Auth: none (public API)
    Data: georeferenced conflict events worldwide

Version probing: tries current-year version first (e.g. 26.1 for 2026),
    then falls back to previous year versions.
Pagination: fetches last MAX_PAGES pages from the newest, pagesize=1000.
Trailing window: 365 days from the newest event date.
Violence types: STATE_BASED (1), NON_STATE (2), ONE_SIDED (3).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

UCDP_BASE = "https://ucdpapi.pcr.uu.se/api/gedevents"
UCDP_PAGE_SIZE = 1000
MAX_PAGES = 6
MAX_EVENTS = 2000

VIOLENCE_TYPES = {
    1: "STATE_BASED",
    2: "NON_STATE",
    3: "ONE_SIDED",
}


def _probe_versions() -> list[str]:
    """Generate version strings to probe, newest first."""
    year_short = datetime.now(timezone.utc).year - 2000
    return [f"{y}.1" for y in range(year_short, year_short - 4, -1)]


class UCDPExtractor(BaseExtractor):
    """Fetches georeferenced conflict events from UCDP GED API.

    Mirrors worldmonitor seed-ucdp-events.mjs:
      - Probes API versions (26.1 → 25.1 → 24.1 → 23.1)
      - Fetches last 6 pages (newest first), pagesize=1000, cap 2000 events
      - Filters to 365-day trailing window from most recent event
      - Classifies violence type: STATE_BASED, NON_STATE, ONE_SIDED
    """

    SOURCE = "ucdp"
    DOMAIN = "conflict"
    REDIS_KEY = "conflict:ucdp-events:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        version = await self._find_version(session)
        if not version:
            logger.error("UCDP: no working API version found")
            return []

        # Discover total pages via first request
        url = f"{UCDP_BASE}/{version}"
        params = {"pagesize": str(UCDP_PAGE_SIZE), "page": "0"}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                first_page = await resp.json(content_type=None)
        except Exception as exc:
            logger.error("UCDP first page failed: %s", exc)
            return []

        total_pages = first_page.get("TotalPages", 1)
        all_events: list[dict] = list(first_page.get("Result", []))

        # Fetch last MAX_PAGES pages (most recent data)
        start_page = max(0, total_pages - MAX_PAGES)
        pages_to_fetch = list(range(start_page, total_pages))
        # We already have page 0 if it's in range; skip if so
        if 0 in pages_to_fetch:
            pages_to_fetch.remove(0)

        for pg in pages_to_fetch:
            if len(all_events) >= MAX_EVENTS:
                break
            try:
                async with session.get(url, params={"pagesize": str(UCDP_PAGE_SIZE), "page": str(pg)}) as resp:
                    if resp.status != 200:
                        continue
                    page_data = await resp.json(content_type=None)
                all_events.extend(page_data.get("Result", []))
            except Exception as exc:
                logger.warning("UCDP page %d failed: %s", pg, exc)

        # Cap at MAX_EVENTS
        all_events = all_events[:MAX_EVENTS]

        if not all_events:
            return []

        # Find newest event date → trailing 365-day window
        dates = []
        for ev in all_events:
            ds = ev.get("date_start") or ev.get("date_end")
            if ds:
                try:
                    dates.append(datetime.strptime(ds[:10], "%Y-%m-%d"))
                except ValueError:
                    pass
        if dates:
            newest = max(dates)
            cutoff = newest - timedelta(days=365)
        else:
            cutoff = datetime.now(timezone.utc) - timedelta(days=365)

        items: list[IntelItem] = []
        for ev in all_events:
            ds = ev.get("date_start") or ev.get("date_end", "")
            try:
                event_date = datetime.strptime(ds[:10], "%Y-%m-%d")
                if event_date < cutoff:
                    continue
            except (ValueError, TypeError):
                pass

            lat = _safe_float(ev.get("latitude"))
            lon = _safe_float(ev.get("longitude"))
            vtype_num = int(ev.get("type_of_violence", 0) or 0)
            vtype = VIOLENCE_TYPES.get(vtype_num, "UNKNOWN")
            deaths_a = int(ev.get("deaths_a", 0) or 0)
            deaths_b = int(ev.get("deaths_b", 0) or 0)
            deaths_civ = int(ev.get("deaths_civilians", 0) or 0)
            deaths_unknown = int(ev.get("deaths_unknown", 0) or 0)
            total_deaths = deaths_a + deaths_b + deaths_civ + deaths_unknown

            # Severity based on deaths and violence type
            if total_deaths >= 25 or vtype == "ONE_SIDED":
                severity = "HIGH"
            elif total_deaths > 0:
                severity = "MEDIUM"
            else:
                severity = "LOW"

            country = ev.get("country", "")
            side_a = ev.get("side_a", "")
            side_b = ev.get("side_b", "")

            items.append(IntelItem(
                id=f"ucdp:{ev.get('id', '')}",
                source="ucdp",
                domain="conflict",
                title=f"{side_a} vs {side_b}" if side_b else side_a,
                lat=lat,
                lon=lon,
                country=country,
                ts=ds[:10] if ds else "",
                severity=severity,
                tags=["conflict", vtype.lower()],
                extra={
                    "violence_type": vtype,
                    "side_a": side_a,
                    "side_b": side_b,
                    "deaths_a": deaths_a,
                    "deaths_b": deaths_b,
                    "deaths_civilians": deaths_civ,
                    "deaths_unknown": deaths_unknown,
                    "total_deaths": total_deaths,
                    "source_article": ev.get("source_article", ""),
                    "source_original": ev.get("source_original", ""),
                    "where_coordinates": ev.get("where_coordinates", ""),
                    "adm_1": ev.get("adm_1", ""),
                    "adm_2": ev.get("adm_2", ""),
                    "dyad_name": ev.get("dyad_name", ""),
                    "conflict_name": ev.get("conflict_name", ""),
                },
            ))
        return items

    async def _find_version(self, session: aiohttp.ClientSession) -> str | None:
        """Probe UCDP API versions, newest first (mirrors worldmonitor)."""
        for ver in _probe_versions():
            try:
                url = f"{UCDP_BASE}/{ver}"
                async with session.get(url, params={"pagesize": "1", "page": "0"}) as resp:
                    if resp.status == 200:
                        logger.info("UCDP: using API version %s", ver)
                        return ver
            except Exception:
                pass
        return None


def _safe_float(val: object) -> float | None:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
