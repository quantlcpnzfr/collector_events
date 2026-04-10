"""UNHCR displacement and GPS interference extractors.

Displacement — UNHCR Population API
    Mirrors worldmonitor seed-displacement.mjs:
    API:  ``GET https://api.unhcr.org/population/v1/population/``
    Auth: none (public)
    Data: refugee and displacement population by origin/asylum country
    Paginated: up to 25 pages, 10000 per page, year-based fallback.

GPS Interference — Wingbits ADS-B
    Mirrors worldmonitor seed-gpsjam-intel.mjs:
    API:  ``GET https://customer-api.wingbits.com/v1/gps/jam``
    Auth: ``X-API-Key`` header
    Data: GPS jamming/spoofing zones from ADS-B aircraft data, H3 hex resolution
    Classification: npAvg ≤ 0.5 → HIGH, ≤ 1.0 → MEDIUM, else LOW
    12 conflict-region bounding boxes for prioritisation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

# ─── UNHCR Displacement ─────────────────────────────────────────────

UNHCR_API = "https://api.unhcr.org/population/v1/population/"
UNHCR_MAX_PAGES = 25
UNHCR_PAGE_SIZE = 10000


class DisplacementExtractor(BaseExtractor):
    """Fetches refugee/displacement data from UNHCR HAPI.

    Mirrors worldmonitor seed-displacement.mjs:
      - Year-based query (current year, falls back to previous 2 years)
      - Paginated (up to 25 pages × 10 000 records)
      - Aggregates by country of origin + country of asylum
      - Reports total refugees, asylum seekers, IDPs per country
    """

    SOURCE = "unhcr_displacement"
    DOMAIN = "conflict"
    REDIS_KEY = "displacement:summary:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        year = datetime.now(timezone.utc).year
        all_rows: list[dict] = []

        # Try current year, fallback to previous years
        for attempt_year in range(year, year - 3, -1):
            rows = await self._fetch_year(session, attempt_year)
            if rows:
                all_rows = rows
                break

        if not all_rows:
            logger.warning("UNHCR: no data found for years %d-%d", year - 2, year)
            return []

        # Aggregate by country of origin
        by_origin: dict[str, dict] = {}
        for row in all_rows:
            coo = row.get("coo_name") or row.get("coo", "Unknown")
            if coo not in by_origin:
                by_origin[coo] = {
                    "coo_iso": row.get("coo_iso", ""),
                    "refugees": 0,
                    "asylum_seekers": 0,
                    "idps": 0,
                    "total": 0,
                    "destinations": set(),
                }
            entry = by_origin[coo]
            entry["refugees"] += int(row.get("refugees", 0) or 0)
            entry["asylum_seekers"] += int(row.get("asylum_seekers", 0) or 0)
            entry["idps"] += int(row.get("idps", 0) or 0)
            entry["total"] += int(row.get("total_population", 0) or 0)
            coa = row.get("coa_name") or row.get("coa", "")
            if coa:
                entry["destinations"].add(coa)

        items: list[IntelItem] = []
        for country, agg in by_origin.items():
            total = agg["total"]
            if total < 1000:
                continue  # Filter noise

            if total >= 1_000_000:
                severity = "HIGH"
            elif total >= 100_000:
                severity = "MEDIUM"
            else:
                severity = "LOW"

            # Convert set to list for serialisation
            dests = sorted(agg["destinations"])

            items.append(IntelItem(
                id=f"unhcr:{agg['coo_iso']}",
                source="unhcr_displacement",
                domain="conflict",
                title=f"{country}: {total:,} displaced persons",
                country=country,
                severity=severity,
                tags=["displacement", "refugees", "unhcr"],
                extra={
                    "coo_iso": agg["coo_iso"],
                    "refugees": agg["refugees"],
                    "asylum_seekers": agg["asylum_seekers"],
                    "idps": agg["idps"],
                    "total": total,
                    "top_destinations": dests[:10],
                },
            ))

        items.sort(key=lambda x: -x.extra.get("total", 0))
        return items

    async def _fetch_year(
        self,
        session: aiohttp.ClientSession,
        year: int,
    ) -> list[dict]:
        """Paginated fetch for a single year."""
        all_rows: list[dict] = []
        for page in range(1, UNHCR_MAX_PAGES + 1):
            params = {
                "year": str(year),
                "limit": str(UNHCR_PAGE_SIZE),
                "page": str(page),
                "coo_all": "true",
                "coa_all": "true",
            }
            try:
                async with session.get(UNHCR_API, params=params) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json(content_type=None)
            except Exception as exc:
                logger.warning("UNHCR page %d year %d failed: %s", page, year, exc)
                break

            rows = data.get("items", data.get("data", []))
            if not rows:
                break
            all_rows.extend(rows)
            # Check for pagination exhaustion
            pagination = data.get("pagination", {})
            total_pages = pagination.get("pages", page)
            if page >= total_pages:
                break

        return all_rows


# ─── GPS Jamming / Interference ──────────────────────────────────────

WINGBITS_API = "https://customer-api.wingbits.com/v1/gps/jam"

# 12 conflict-region bounding boxes (worldmonitor gpsjam regions)
GPS_REGIONS: dict[str, dict] = {
    "ukraine": {"min_lat": 44.0, "max_lat": 53.0, "min_lon": 22.0, "max_lon": 41.0},
    "baltics": {"min_lat": 53.5, "max_lat": 60.0, "min_lon": 20.0, "max_lon": 29.0},
    "syria_iraq": {"min_lat": 32.0, "max_lat": 38.0, "min_lon": 35.0, "max_lon": 48.0},
    "israel_palestine": {"min_lat": 29.5, "max_lat": 33.5, "min_lon": 34.0, "max_lon": 36.0},
    "iran": {"min_lat": 25.0, "max_lat": 40.0, "min_lon": 44.0, "max_lon": 64.0},
    "yemen": {"min_lat": 12.0, "max_lat": 19.0, "min_lon": 42.0, "max_lon": 54.0},
    "east_med": {"min_lat": 33.0, "max_lat": 42.0, "min_lon": 24.0, "max_lon": 37.0},
    "black_sea": {"min_lat": 41.0, "max_lat": 47.0, "min_lon": 27.0, "max_lon": 42.0},
    "south_china_sea": {"min_lat": 5.0, "max_lat": 23.0, "min_lon": 105.0, "max_lon": 122.0},
    "korea": {"min_lat": 33.0, "max_lat": 43.0, "min_lon": 124.0, "max_lon": 131.0},
    "red_sea": {"min_lat": 12.0, "max_lat": 30.0, "min_lon": 32.0, "max_lon": 44.0},
    "sahel": {"min_lat": 10.0, "max_lat": 20.0, "min_lon": -5.0, "max_lon": 15.0},
}


def _classify_zone(lat: float, lon: float) -> str:
    """Find which conflict region a coordinate falls in, if any."""
    for name, bbox in GPS_REGIONS.items():
        if (bbox["min_lat"] <= lat <= bbox["max_lat"]
                and bbox["min_lon"] <= lon <= bbox["max_lon"]):
            return name
    return ""


class GPSJammingExtractor(BaseExtractor):
    """Fetches GPS jamming/spoofing data from Wingbits ADS-B network.

    Mirrors worldmonitor seed-gpsjam-intel.mjs:
      - H3 hex data from ``customer-api.wingbits.com/v1/gps/jam``
      - Severity: npAvg ≤ 0.5 → HIGH, ≤ 1.0 → MEDIUM, else LOW
      - Classified into 12 conflict-region bounding boxes
      - Requires ``WINGBITS_API_KEY`` env var
    """

    SOURCE = "gps_jamming"
    DOMAIN = "conflict"
    REDIS_KEY = "intelligence:gpsjam:v2"
    TTL_SECONDS = 172800  # 48h

    def __init__(self, api_key: str = ""):
        super().__init__()
        self._api_key = api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("GPSJammingExtractor: no WINGBITS_API_KEY configured")
            return []

        headers = {"X-API-Key": self._api_key}
        async with session.get(WINGBITS_API, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        hexes = data if isinstance(data, list) else data.get("data", data.get("hexes", []))

        for h in hexes:
            lat = _safe_float(h.get("lat") or h.get("latitude"))
            lon = _safe_float(h.get("lon") or h.get("longitude"))
            if lat is None or lon is None:
                continue

            np_avg = float(h.get("npAvg", h.get("np_avg", 99)))

            if np_avg <= 0.5:
                severity = "HIGH"
            elif np_avg <= 1.0:
                severity = "MEDIUM"
            else:
                severity = "LOW"

            region = _classify_zone(lat, lon)
            if not region and severity == "LOW":
                continue  # Skip low-severity outside conflict zones

            h3_index = h.get("h3Index") or h.get("h3", "")

            items.append(IntelItem(
                id=f"gpsjam:{h3_index or f'{lat:.3f}:{lon:.3f}'}",
                source="gps_jamming",
                domain="conflict",
                title=f"GPS interference: {region or 'unknown region'} (npAvg={np_avg:.2f})",
                lat=lat,
                lon=lon,
                severity=severity,
                tags=["gps", "jamming", "adsb"],
                extra={
                    "h3_index": h3_index,
                    "np_avg": np_avg,
                    "sample_count": h.get("sampleCount") or h.get("sample_count", 0),
                    "region": region,
                    "conflict_zone": bool(region),
                },
            ))

        # Sort: severity desc then np_avg ascending (worst interference first)
        _sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        items.sort(key=lambda x: (_sev_order.get(x.severity, 9), x.extra.get("np_avg", 99)))
        return items


def _safe_float(val: object) -> float | None:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
