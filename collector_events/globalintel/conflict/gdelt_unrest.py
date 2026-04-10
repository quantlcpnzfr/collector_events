"""GDELT unrest geolocation extractor."""

from __future__ import annotations

import logging

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = logging.getLogger(__name__)

_CFG = _load("conflict.json")["gdelt"]
_GKG_URL = _CFG["gkg_url"]
_RIOT_KEYWORDS: set[str] = set(_CFG["riot_keywords"])


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
    TTL_SECONDS = 16200

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        params = {
            "QUERY": "protest OR riot OR demonstration OR strike",
            "OUTPUTTYPE": "2",
            "MAXROWS": "2500",
        }
        try:
            async with session.get(_GKG_URL, params=params) as resp:
                if resp.status != 200:
                    logger.warning("GDELT GKG returned %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("GDELT GKG failed: %s", exc)
            return []

        grid: dict[str, dict] = {}
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [0, 0])
            lat = coords[1] if len(coords) > 1 else 0
            lon = coords[0] if len(coords) > 0 else 0
            count = int(props.get("count", 0) or 0)
            name = props.get("name", "Unknown")

            grid_key = f"{round(lat * 10) / 10:.1f}:{round(lon * 10) / 10:.1f}"
            if grid_key in grid:
                grid[grid_key]["count"] += count
                if len(name) > len(grid[grid_key]["name"]):
                    grid[grid_key]["name"] = name
            else:
                grid[grid_key] = {"lat": lat, "lon": lon, "name": name, "count": count}

        items: list[IntelItem] = []
        for gk, cell in grid.items():
            count = cell["count"]
            if count < 5:
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
        _sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        items.sort(key=lambda x: (_sev_order.get(x.severity, 9), -x.extra.get("count", 0)))
        return items
