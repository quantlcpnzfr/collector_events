"""Commodity quote extractor.

Mirrors worldmonitor ListCommodityQuotes → market:commodities:v1
Upstream: Yahoo Finance chart API.
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..reference import COMMODITIES

logger = logging.getLogger(__name__)

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"


class CommodityQuoteExtractor(BaseExtractor):
    """Fetches commodity prices from Yahoo Finance.

    Covers metals (Gold, Silver, Platinum, Palladium, Copper),
    energy (Crude, NatGas, Brent, RBOB, Heating Oil),
    agriculture (Corn, Wheat, Soybeans, Sugar, Coffee, Cotton, OJ, Cocoa),
    and major FX pairs.
    """

    SOURCE = "commodity_quotes"
    DOMAIN = "market"
    REDIS_KEY = "market:commodities:v1"
    TTL_SECONDS = 300  # 5min

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        sem = asyncio.Semaphore(5)
        items: list[IntelItem] = []

        async def _get_commodity(entry: dict) -> IntelItem | None:
            symbol = entry["symbol"]
            name = entry["name"]
            category = entry.get("category", "other")
            async with sem:
                try:
                    url = f"{YAHOO_CHART}/{symbol}"
                    async with session.get(url, params={"interval": "1d", "range": "5d"}) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json(content_type=None)
                    result = data.get("chart", {}).get("result", [{}])[0]
                    meta = result.get("meta", {})
                    price = meta.get("regularMarketPrice", 0)
                    prev = meta.get("previousClose", 0)
                    change = ((price - prev) / prev * 100) if prev else 0
                    currency = meta.get("currency", "USD")
                    closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                    sparkline = [c for c in closes if c is not None][-5:]
                    return IntelItem(
                        id=f"commodity:{symbol}",
                        source=self.SOURCE,
                        domain=self.DOMAIN,
                        title=f"{name}: {price:,.2f} {currency}",
                        tags=["commodity", category],
                        extra={
                            "symbol": symbol,
                            "name": name,
                            "category": category,
                            "price": price,
                            "change": round(change, 2),
                            "currency": currency,
                            "sparkline": sparkline,
                        },
                    )
                except Exception as exc:
                    logger.debug("Commodity %s failed: %s", symbol, exc)
                    return None
                finally:
                    await asyncio.sleep(0.15)  # Stagger Yahoo requests

        tasks = [_get_commodity(c) for c in COMMODITIES]
        results = await asyncio.gather(*tasks)
        items = [r for r in results if r is not None]
        return items
