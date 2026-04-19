"""World Bank Open Data extractor.

Upstream: https://api.worldbank.org/v2/
Auth: none
Format: JSON (format=json)

Mirrors worldmonitor ListWorldBankIndicators.
Key indicators: NY.GDP.MKTP.CD (GDP), FP.CPI.TOTL.ZG (Inflation),
                SL.UEM.TOTL.ZS (Unemployment), BX.KLT.DINV.CD.WD (FDI).
"""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem

logger = get_logger(__name__)

WB_API = "https://api.worldbank.org/v2"

# Key development indicators
WB_INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP Growth (%)",
    "FP.CPI.TOTL.ZG": "Inflation, CPI (%)",
    "SL.UEM.TOTL.ZS": "Unemployment (%)",
    "BX.KLT.DINV.CD.WD": "Foreign Direct Investment (US$)",
    "GC.DOD.TOTL.GD.ZS": "Central Govt Debt (% of GDP)",
    "BN.CAB.XOKA.CD": "Current Account Balance (US$)",
}

# Top economies to fetch
WB_COUNTRIES = ["USA", "CHN", "JPN", "DEU", "GBR", "IND", "FRA", "BRA", "CAN", "AUS"]


class WorldBankExtractor(BaseExtractor):
    """Fetches key World Bank development indicators for major economies.

    One IntelItem per country-indicator pair (latest available year).
    """

    SOURCE = "world_bank"
    DOMAIN = "economic"
    REDIS_KEY = "economic:world-bank:v1"
    TTL_SECONDS = 604800  # 7 days — data updates slowly

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        import asyncio

        items: list[IntelItem] = []
        sem = asyncio.Semaphore(3)

        async def _get_indicator(code: str, label: str) -> list[IntelItem]:
            async with sem:
                countries_str = ";".join(WB_COUNTRIES)
                url = f"{WB_API}/country/{countries_str}/indicator/{code}"
                params = {"format": "json", "per_page": "100", "mrnev": "1"}  # most recent non-empty value
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return []
                        data = await resp.json(content_type=None)
                    if not isinstance(data, list) or len(data) < 2:
                        return []
                    results = []
                    for entry in data[1]:
                        cc = entry.get("country", {}).get("id", "")
                        value = entry.get("value")
                        year = entry.get("date")
                        if value is None:
                            continue
                        results.append(IntelItem(
                            id=f"wb:{code}:{cc}",
                            source=self.SOURCE,
                            domain=self.DOMAIN,
                            title=f"{label}: {entry.get('country', {}).get('value', cc)}",
                            country=[cc],
                            tags=["world_bank", "macro", "development"],
                            extra={
                                "indicator_code": code,
                                "indicator_name": label,
                                "country_code": cc,
                                "value": value,
                                "year": year,
                            },
                        ))
                    return results
                except Exception as exc:
                    logger.debug("World Bank %s failed: %s", code, exc)
                    return []

        tasks = [_get_indicator(code, label) for code, label in WB_INDICATORS.items()]
        results = await asyncio.gather(*tasks)
        for batch in results:
            items.extend(batch)
        return items
