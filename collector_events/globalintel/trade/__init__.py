"""Trade intelligence extractors: WTO, tariffs, Comtrade, trade barriers.

Mirrors worldmonitor handlers:
  GetTradeRestrictions → trade:wto:restrictions:v1
  GetTariffTrends      → trade:tariff:trends:v1
  GetTradeFlows        → trade:comtrade:flows:v1
  GetTradeBarriers     → trade:barriers:v1
  GetCustomsRevenue    → trade:customs:v1
  ListComtradeFlows    → trade:comtrade:detailed:v1

Upstream:
  WTO: https://api.wto.org/timeseries/v1/ (requires API key)
  UN Comtrade: https://comtradeapi.un.org/data/v1/get (public with limits)
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)


class WtoTradeRestrictionExtractor(BaseExtractor):
    """Fetches trade restriction measures from WTO.

    Upstream: WTO Timeseries API — trade-monitoring / restrictive measures.
    Auth: requires WTO API key.
    """

    SOURCE = "wto_restrictions"
    DOMAIN = "trade"
    REDIS_KEY = "trade:wto:restrictions:v1"
    TTL_SECONDS = 604800  # 7 days

    def __init__(self, wto_api_key: str = ""):
        super().__init__()
        self._api_key = wto_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("WTO API key not set — skipping trade restrictions")
            return []

        headers = {"Ocp-Apim-Subscription-Key": self._api_key}
        url = "https://api.wto.org/timeseries/v1/data"
        params = {
            "i": "TP_TM_MEASURES",  # Trade Monitoring Measures
            "r": "all",
            "ps": "last",
            "max": "100",
        }
        async with session.get(url, params=params, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        dataset = data.get("Dataset", [])
        items: list[IntelItem] = []
        for row in dataset:
            reporter = row.get("ReportingEconomyCode", "")
            value = row.get("Value")
            items.append(IntelItem(
                id=f"wto:restriction:{reporter}:{row.get('Year', '')}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"Trade Restrictions: {row.get('ReportingEconomy', reporter)}",
                country=reporter,
                tags=["wto", "trade", "restrictions"],
                extra={
                    "reporter": row.get("ReportingEconomy"),
                    "reporter_code": reporter,
                    "year": row.get("Year"),
                    "indicator": row.get("IndicatorCode"),
                    "value": value,
                },
            ))
        return items


class ComtradeFlowExtractor(BaseExtractor):
    """Fetches bilateral trade flow data from UN Comtrade.

    Upstream: https://comtradeapi.un.org/data/v1/get
    Auth: optional subscription key (higher limits).
    Tracks top commodity flows between G20 economies.
    """

    SOURCE = "comtrade_flows"
    DOMAIN = "trade"
    REDIS_KEY = "trade:comtrade:flows:v1"
    TTL_SECONDS = 604800  # 7 days

    REPORTERS = ["840", "156", "276", "392", "826"]  # US, China, Germany, Japan, UK
    PARTNERS = ["0"]  # World (aggregate)

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        sem = asyncio.Semaphore(2)  # Comtrade rate limits

        async def _get_reporter(reporter: str) -> list[IntelItem]:
            async with sem:
                url = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
                params = {
                    "reporterCode": reporter,
                    "partnerCode": "0",  # World
                    "period": "2023",    # Latest annual
                    "cmdCode": "TOTAL",
                    "flowCode": "M,X",   # Import+Export
                    "maxRecords": "10",
                }
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return []
                        data = await resp.json(content_type=None)
                except Exception as exc:
                    logger.debug("Comtrade %s failed: %s", reporter, exc)
                    return []

                results = []
                for row in data.get("data", []):
                    flow = "Export" if row.get("flowCode") == "X" else "Import"
                    value = row.get("fobvalue") or row.get("cifvalue")
                    results.append(IntelItem(
                        id=f"comtrade:{reporter}:{row.get('flowCode', '')}:{row.get('period', '')}",
                        source=self.SOURCE,
                        domain=self.DOMAIN,
                        title=f"{row.get('reporterDesc', reporter)} {flow}: ${value / 1e9:.1f}B" if value else "",
                        country=reporter,
                        tags=["comtrade", "trade", "bilateral"],
                        extra={
                            "reporter": row.get("reporterDesc"),
                            "reporter_code": reporter,
                            "partner": row.get("partnerDesc"),
                            "flow": flow,
                            "value_usd": value,
                            "period": row.get("period"),
                        },
                    ))
                return results

        tasks = [_get_reporter(r) for r in self.REPORTERS]
        results = await asyncio.gather(*tasks)
        for batch in results:
            items.extend(batch)
        return items


class TariffTrendExtractor(BaseExtractor):
    """Fetches tariff trends from WTO.

    Tracks average applied tariff rates for major economies over time.
    """

    SOURCE = "tariff_trends"
    DOMAIN = "trade"
    REDIS_KEY = "trade:tariff:trends:v1"
    TTL_SECONDS = 604800  # 7 days

    def __init__(self, wto_api_key: str = ""):
        super().__init__()
        self._api_key = wto_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("WTO API key not set — skipping tariff trends")
            return []

        headers = {"Ocp-Apim-Subscription-Key": self._api_key}
        url = "https://api.wto.org/timeseries/v1/data"
        # TP_D_APPLIED = Applied tariff rates
        economies = "840,156,826,276,392,356"  # US, CN, UK, DE, JP, IN
        params = {
            "i": "TP_A_TAR_01",
            "r": economies,
            "ps": "2019,2020,2021,2022,2023",
            "max": "200",
        }
        async with session.get(url, params=params, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        for row in data.get("Dataset", []):
            reporter = row.get("ReportingEconomy", "")
            items.append(IntelItem(
                id=f"tariff:{row.get('ReportingEconomyCode', '')}:{row.get('Year', '')}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"Avg Tariff: {reporter} ({row.get('Year', '')})",
                country=row.get("ReportingEconomyCode"),
                tags=["tariff", "wto", "trade"],
                extra={
                    "reporter": reporter,
                    "reporter_code": row.get("ReportingEconomyCode"),
                    "year": row.get("Year"),
                    "avg_tariff": row.get("Value"),
                },
            ))
        return items
