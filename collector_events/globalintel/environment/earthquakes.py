"""USGS earthquake extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.config import _load
from collector_events.globalintel.utils import safe_float

logger = get_logger(__name__)

_CFG = _load("environment.json")
_USGS_EARTHQUAKE_URL = _CFG["usgs_earthquake_url"]


class USGSEarthquakeExtractor(BaseExtractor):
    """Fetches significant earthquakes from USGS GeoJSON feed."""

    SOURCE = "usgs"
    DOMAIN = "environment"
    REDIS_KEY = "environment:earthquakes:v1"
    TTL_SECONDS = 3600

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        try:
            async with session.get(_USGS_EARTHQUAKE_URL) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("USGS earthquake failed: %s", exc)
            return []

        for feat in data.get("features", []):
            props = feat.get("properties", {})
            geo = feat.get("geometry", {}).get("coordinates", [0, 0, 0])
            mag = safe_float(props.get("mag")) or 0

            if mag >= 6:
                severity = "HIGH"
            elif mag >= 5:
                severity = "MEDIUM"
            else:
                severity = "LOW"

            items.append(IntelItem(
                id=f"usgs:{props.get('code', feat.get('id', ''))}",
                source="usgs",
                domain="environment",
                title=props.get("title", f"M{mag} Earthquake"),
                url=props.get("url", ""),
                ts=str(props.get("time", "")),
                lat=safe_float(geo[1]) if len(geo) > 1 else None,
                lon=safe_float(geo[0]) if len(geo) > 0 else None,
                country=[props.get("place")] if props.get("place") else [],
                severity=severity,
                tags=["earthquake", "natural_disaster"],
                extra={
                    "magnitude": mag,
                    "depth_km": safe_float(geo[2]) if len(geo) > 2 else None,
                    "tsunami": props.get("tsunami", 0),
                    "felt": props.get("felt"),
                    "alert": props.get("alert"),
                    "significance": props.get("sig"),
                },
            ))
        return items
