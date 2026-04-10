"""Economic calendar extractor (FRED, FOMC, Eurostat)."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = logging.getLogger(__name__)

_CFG = _load("economic.json")
_FRED_RELEASE_URL = _CFG["fred"]["release_url"]
_FRED_RELEASES: dict[str, str] = _CFG["fred"]["releases"]
_EUROSTAT_API = _CFG["eurostat"]["api_url"]
_EUROSTAT_DATASETS: dict[str, str] = _CFG["eurostat"]["datasets"]
_FOMC_URL = _CFG["fomc_url"]


class EconomicCalendarExtractor(BaseExtractor):
    """FRED releases + FOMC meetings + Eurostat macro data."""

    SOURCE = "economic_calendar"
    DOMAIN = "economic"
    REDIS_KEY = "economic:econ-calendar:v1"
    TTL_SECONDS = 129600  # 36h

    def __init__(self, fred_api_key: str = ""):
        super().__init__()
        self._fred_key = fred_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        if self._fred_key:
            items.extend(await self._fetch_fred(session))
            items.extend(await self._fetch_eurostat(session))
        items.extend(await self._fetch_fomc_dates(session))
        return items

    async def _fetch_fred(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for release_id, name in _FRED_RELEASES.items():
            try:
                url = f"{_FRED_RELEASE_URL}/{release_id}/dates"
                params = {
                    "api_key": self._fred_key,
                    "file_type": "json",
                    "limit": "5",
                    "sort_order": "desc",
                }
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                for rd in data.get("release_dates", []):
                    items.append(IntelItem(
                        id=f"fred:{release_id}:{rd.get('date', '')}",
                        source="fred",
                        domain="economic",
                        title=f"FRED: {name} ({rd.get('date', '')})",
                        ts=rd.get("date", ""),
                        tags=["fred", "release", name.lower().replace(" ", "_")],
                        extra={
                            "release_id": int(release_id),
                            "release_name": name,
                            "date": rd.get("date", ""),
                        },
                    ))
            except Exception as exc:
                logger.warning("FRED release %s (%s) failed: %s", release_id, name, exc)
        return items

    async def _fetch_fomc_dates(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        try:
            async with session.get(_FOMC_URL) as resp:
                if resp.status != 200:
                    return []
                html = await resp.text()
            dates = re.findall(r"\d{4}-\d{2}-\d{2}", html)
            now = datetime.now(timezone.utc).date()
            for d in sorted(set(dates)):
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if dt >= now:
                    items.append(IntelItem(
                        id=f"fomc:{d}",
                        source="fomc",
                        domain="economic",
                        title=f"FOMC Meeting: {d}",
                        ts=d,
                        severity="HIGH",
                        tags=["fomc", "fed", "interest_rate"],
                        extra={"date": d, "event": "FOMC Meeting"},
                    ))
        except Exception as exc:
            logger.warning("FOMC scrape failed: %s", exc)
        return items

    async def _fetch_eurostat(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for dataset_id, name in _EUROSTAT_DATASETS.items():
            try:
                url = f"{_EUROSTAT_API}/{dataset_id}"
                params = {"format": "JSON", "lang": "en"}
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                label = data.get("label", name)
                items.append(IntelItem(
                    id=f"eurostat:{dataset_id}",
                    source="eurostat",
                    domain="economic",
                    title=f"Eurostat: {label}",
                    tags=["eurostat", dataset_id],
                    extra={
                        "dataset": dataset_id,
                        "label": label,
                        "updated": data.get("updated", ""),
                    },
                ))
            except Exception as exc:
                logger.debug("Eurostat %s failed: %s", dataset_id, exc)
        return items
