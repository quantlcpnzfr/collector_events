"""BTC Spot ETF flow extractor.

Mirrors worldmonitor ListEtfFlows → market:etf:flows:v1
Upstream: Aggregated from public filings / SoSoValue-style APIs.
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..reference import BTC_SPOT_ETFS

logger = logging.getLogger(__name__)

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"


class BtcEtfFlowExtractor(BaseExtractor):
    """Fetches BTC spot ETF prices and NAV data.

    Tracks 10 BTC spot ETFs: IBIT, FBTC, ARKB, BITB, HODL, BRRR,
    EZBC, BTCO, BTCW, GBTC.

    Note: actual daily flow data requires specialized APIs (SoSoValue,
    BitMEX Research, etc.). This extractor provides price performance
    as a proxy; real flow data can be added when API access is available.
    """

    SOURCE = "btc_etf_flows"
    DOMAIN = "market"
    REDIS_KEY = "market:etf:flows:v1"
    TTL_SECONDS = 3600  # 1h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        sem = asyncio.Semaphore(5)
        items: list[IntelItem] = []

        async def _get_etf(etf: dict) -> IntelItem | None:
            symbol = etf["symbol"]
            name = etf["name"]
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
                    volume = result.get("indicators", {}).get("quote", [{}])[0].get("volume", [])
                    latest_vol = [v for v in volume if v is not None]
                    return IntelItem(
                        id=f"btc_etf:{symbol}",
                        source=self.SOURCE,
                        domain=self.DOMAIN,
                        title=f"{symbol}: ${price:.2f} ({change:+.2f}%)" if price else symbol,
                        tags=["etf", "bitcoin", "btc"],
                        extra={
                            "symbol": symbol, "name": name,
                            "price": price, "change": round(change, 2),
                            "volume": latest_vol[-1] if latest_vol else None,
                        },
                    )
                except Exception as exc:
                    logger.debug("BTC ETF %s failed: %s", symbol, exc)
                    return None
                finally:
                    await asyncio.sleep(0.15)

        tasks = [_get_etf(e) for e in BTC_SPOT_ETFS]
        results = await asyncio.gather(*tasks)
        items = [r for r in results if r is not None]
        return items
