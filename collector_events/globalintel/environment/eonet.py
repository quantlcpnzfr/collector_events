"""NASA EONET natural events extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_CFG = _load("environment.json")
_EONET_API = _CFG["eonet_api"]


class NASAEONETExtractor(BaseExtractor):
    """Fetches open natural events from NASA EONET (wildfires, storms, etc)."""

    SOURCE = "eonet"
    DOMAIN = "environment"
    REDIS_KEY = "environment:eonet:v1"
    TTL_SECONDS = 14400  # 4h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        try:
            params = {"status": "open", "limit": "50"}
            async with session.get(_EONET_API, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("EONET fetch failed: %s", exc)
            return []

        for event in data.get("events", []):
            title = event.get("title", "")
            categories = event.get("categories", [])
            category = categories[0].get("title", "") if categories else ""

            # Latest geometry point
            geometries = event.get("geometry", event.get("geometries", []))
            lat, lon = None, None
            if geometries:
                coords = geometries[-1].get("coordinates", [])
                if len(coords) >= 2:
                    lon, lat = coords[0], coords[1]

            items.append(IntelItem(
                id=f"eonet:{event.get('id', '')}",
                source="eonet",
                domain="environment",
                title=title[:300],
                url=event.get("link", ""),
                lat=lat,
                lon=lon,
                tags=["eonet", category.lower()],
                extra={
                    "category": category,
                    "closed": event.get("closed"),
                    "sources": [s.get("id", "") for s in event.get("sources", [])],
                },
            ))
        return items
