"""NASA FIRMS fire extractor."""

from __future__ import annotations

import csv
import io
import logging

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load
from ..utils import safe_float

logger = logging.getLogger(__name__)

_CFG = _load("environment.json")
_FIRMS_API_BASE = _CFG["firms_api_base"]
_DEFAULT_COUNTRIES: list[str] = _CFG["firms_default_countries"]


class NASAFireExtractor(BaseExtractor):
    """Fetches active fire data from NASA FIRMS (VIIRS, last 48h)."""

    SOURCE = "nasa_firms"
    DOMAIN = "environment"
    REDIS_KEY = "environment:fires:v1"
    TTL_SECONDS = 7200

    def __init__(self, api_key: str = "", countries: list[str] | None = None):
        super().__init__()
        self._api_key = api_key
        self._countries = countries or _DEFAULT_COUNTRIES

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("NASA FIRMS API key not configured")
            return []

        items: list[IntelItem] = []
        for country in self._countries:
            try:
                url = f"{_FIRMS_API_BASE}/{self._api_key}/VIIRS_SNPP_NRT/{country}/2"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    text = await resp.text()

                reader = csv.DictReader(io.StringIO(text))
                count = 0
                for row in reader:
                    confidence = row.get("confidence", "nominal")
                    frp = safe_float(row.get("frp"))

                    if confidence == "high" or (frp and frp > 50):
                        severity = "HIGH"
                    elif confidence == "nominal":
                        severity = "MEDIUM"
                    else:
                        severity = "LOW"

                    lat = safe_float(row.get("latitude"))
                    lon = safe_float(row.get("longitude"))

                    items.append(IntelItem(
                        id=f"firms:{country}:{row.get('acq_date', '')}:{lat}:{lon}",
                        source="nasa_firms",
                        domain="environment",
                        title=f"Fire: {country} ({lat}, {lon})",
                        ts=f"{row.get('acq_date', '')}T{row.get('acq_time', '0000')[:2]}:{row.get('acq_time', '0000')[2:]}",
                        lat=lat,
                        lon=lon,
                        country=country,
                        severity=severity,
                        tags=["fire", "wildfire"],
                        extra={
                            "frp": frp,
                            "confidence": confidence,
                            "brightness": safe_float(row.get("bright_ti4")),
                            "satellite": row.get("satellite", ""),
                        },
                    ))
                    count += 1
                    if count >= 51:
                        break
            except Exception as exc:
                logger.warning("FIRMS %s failed: %s", country, exc)
        return items
