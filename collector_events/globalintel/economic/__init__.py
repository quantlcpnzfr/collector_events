"""Economic calendar & data extractors.

Data sources (mirrors worldmonitor seed-economic-calendar.mjs):

FRED — Federal Reserve Economic Data
    URL: https://api.stlouisfed.org/fred/release/dates
    Auth: api_key parameter
    Release IDs: 10 (CPI), 50 (Employment / NFP), 53 (GDP), 54 (PCE), 336 (Retail Sales)

FOMC meeting dates — scraped from Federal Reserve website
    URL: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
    Auth: none

ECB Governing Council dates — scraped from ECB website
    URL: https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html
    Auth: none

Eurostat — EU statistics
    URL: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}
    Auth: none
    Datasets: prc_hicp_manr (HICP/inflation), une_rt_m (unemployment), namq_10_gdp (GDP)
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

FRED_RELEASE_URL = "https://api.stlouisfed.org/fred/release/dates"
FRED_RELEASES = {
    10: "CPI (Consumer Price Index)",
    50: "Employment Situation (NFP)",
    53: "GDP (Gross Domestic Product)",
    54: "PCE (Personal Consumption Expenditures)",
    336: "Advance Retail Sales",
}

EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASETS = {
    "prc_hicp_manr": "HICP Monthly Inflation (EU)",
    "une_rt_m": "Unemployment Rate (EU)",
    "namq_10_gdp": "GDP Quarterly (EU)",
}


class EconomicCalendarExtractor(BaseExtractor):
    """Fetches upcoming economic events from FRED + FOMC + Eurostat.

    Post-collection structure per item:
        id, source="economic_calendar", domain="economic",
        title=<event_name>, ts=<date>,
        extra={release_id, release_name, source_institution}
    """

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
        items.extend(await self._fetch_fomc_dates(session))
        items.extend(await self._fetch_eurostat(session))
        return items

    async def _fetch_fred(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for release_id, release_name in FRED_RELEASES.items():
            try:
                params = {
                    "release_id": str(release_id),
                    "api_key": self._fred_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": "5",
                }
                async with session.get(FRED_RELEASE_URL, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                for rd in data.get("release_dates", []):
                    date = rd.get("date", "")
                    items.append(IntelItem(
                        id=f"fred:{release_id}:{date}",
                        source="fred",
                        domain="economic",
                        title=f"FRED: {release_name}",
                        ts=date,
                        tags=["fed", "economic_data"],
                        extra={
                            "release_id": release_id,
                            "release_name": release_name,
                            "source_institution": "Federal Reserve",
                        },
                    ))
            except Exception as exc:
                logger.warning("FRED release %d failed: %s", release_id, exc)
        return items

    async def _fetch_fomc_dates(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        """Scrape FOMC meeting dates from the Federal Reserve website."""
        url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                html = await resp.text()
            # Simple regex extraction for meeting dates (yyyy-mm-dd pattern)
            dates = re.findall(r"\d{4}-\d{2}-\d{2}", html)
            items: list[IntelItem] = []
            seen: set[str] = set()
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            for d in dates:
                if d not in seen and d >= now:
                    seen.add(d)
                    items.append(IntelItem(
                        id=f"fomc:{d}",
                        source="fomc",
                        domain="economic",
                        title=f"FOMC Meeting: {d}",
                        ts=d,
                        tags=["fomc", "fed", "interest_rate"],
                        extra={"source_institution": "Federal Reserve"},
                    ))
            return items
        except Exception as exc:
            logger.warning("FOMC scrape failed: %s", exc)
            return []

    async def _fetch_eurostat(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for dataset_code, label in EUROSTAT_DATASETS.items():
            try:
                url = f"{EUROSTAT_API}/{dataset_code}"
                params = {"format": "JSON", "lang": "en"}
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                updated = data.get("updated", "")
                items.append(IntelItem(
                    id=f"eurostat:{dataset_code}:{updated}",
                    source="eurostat",
                    domain="economic",
                    title=f"Eurostat: {label}",
                    ts=updated,
                    tags=["eu", "economic_data"],
                    extra={
                        "dataset": dataset_code,
                        "source_institution": "Eurostat",
                        "label": data.get("label", ""),
                    },
                ))
            except Exception as exc:
                logger.warning("Eurostat %s failed: %s", dataset_code, exc)
        return items
