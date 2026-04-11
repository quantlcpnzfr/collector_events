"""GCC/Gulf market quote extractor.

Mirrors worldmonitor ListGulfQuotes → market:gulf:v1
Upstream: Yahoo Finance (GCC indices, currencies, oil).
"""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..reference import GULF_SYMBOLS

logger = get_logger(__name__)

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"


class GulfQuoteExtractor(BaseExtractor):
    """Fetches Gulf/GCC market data.

    14 symbols: TASI, QSI, DFM, ADX, Muscat, Bahrain, Kuwait,
    SAR, AED, BHD, OMR, QAR, KWD, Brent.
    """

    SOURCE = "gulf_quotes"
    DOMAIN = "market"
    REDIS_KEY = "market:gulf:v1"
    TTL_SECONDS = 3600  # 1h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        sem = asyncio.Semaphore(5)
        items: list[IntelItem] = []

        async def _get(entry: dict) -> IntelItem | None:
            symbol = entry["symbol"]
            name = entry["name"]
            category = entry.get("category", "index")
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
                    return IntelItem(
                        id=f"gulf:{symbol}",
                        source=self.SOURCE,
                        domain=self.DOMAIN,
                        title=f"{name}: {price:,.2f}",
                        tags=["gulf", "gcc", category],
                        extra={
                            "symbol": symbol, "name": name,
                            "category": category,
                            "price": price, "change": round(change, 2),
                        },
                    )
                except Exception as exc:
                    logger.debug("Gulf %s failed: %s", symbol, exc)
                    return None
                finally:
                    await asyncio.sleep(0.15)

        tasks = [_get(e) for e in GULF_SYMBOLS]
        results = await asyncio.gather(*tasks)
        items = [r for r in results if r is not None]
        return items
