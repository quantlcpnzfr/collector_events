"""Stock market extractors: quotes, sectors, country indices, earnings.

Mirrors worldmonitor handlers:
  ListMarketQuotes       → market:stocks:v1
  GetSectorSummary       → market:sectors:v1
  GetCountryStockIndex   → market:country-index:v1
  ListEarningsCalendar   → market:earnings:v1

Upstream: Yahoo Finance chart API + Finnhub (optional).
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..reference import STOCK_SYMBOLS, SECTOR_ETFS, YAHOO_ONLY_SYMBOLS

logger = logging.getLogger(__name__)

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"
FINNHUB_QUOTE = "https://finnhub.io/api/v1/quote"


class StockQuoteExtractor(BaseExtractor):
    """Fetches stock quotes for major indices and equities.

    Dual source: Finnhub for US equities (real-time), Yahoo for indices.
    Mimics worldmonitor's seed-stock-quotes.mjs with 150ms stagger.
    """

    SOURCE = "stock_quotes"
    DOMAIN = "market"
    REDIS_KEY = "market:stocks:v1"
    TTL_SECONDS = 300  # 5min

    def __init__(self, finnhub_api_key: str = ""):
        super().__init__()
        self._finnhub_key = finnhub_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        sem = asyncio.Semaphore(5)
        items: list[IntelItem] = []

        async def _get_quote(sym_info: dict) -> IntelItem | None:
            async with sem:
                symbol = sym_info["symbol"]
                name = sym_info["name"]
                display = sym_info.get("display", symbol)

                # Finnhub for non-Yahoo-only symbols
                if self._finnhub_key and symbol not in YAHOO_ONLY_SYMBOLS and not symbol.startswith("^"):
                    try:
                        params = {"symbol": symbol, "token": self._finnhub_key}
                        async with session.get(FINNHUB_QUOTE, params=params) as resp:
                            if resp.status == 200:
                                data = await resp.json(content_type=None)
                                price = data.get("c", 0)
                                prev = data.get("pc", 0)
                                if price and prev:
                                    change = (price - prev) / prev * 100
                                    return IntelItem(
                                        id=f"stock:{symbol}",
                                        source="finnhub",
                                        domain=self.DOMAIN,
                                        title=f"{display}: ${price:.2f}",
                                        tags=["stock", "equity"],
                                        extra={
                                            "symbol": symbol, "name": name, "display": display,
                                            "price": price, "change": round(change, 2),
                                            "provider": "finnhub",
                                        },
                                    )
                    except Exception:
                        pass

                # Fallback to Yahoo Finance
                try:
                    url = f"{YAHOO_CHART}/{symbol}"
                    async with session.get(url, params={"interval": "1d", "range": "5d"}) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json(content_type=None)
                    result = data.get("chart", {}).get("result", [])
                    if not result:
                        return None
                    meta = result[0].get("meta", {})
                    price = meta.get("regularMarketPrice", 0)
                    prev = meta.get("previousClose", 0)
                    change = ((price - prev) / prev * 100) if prev else 0
                    # Build sparkline from last 5 closes
                    closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                    sparkline = [c for c in closes if c is not None][-5:]
                    return IntelItem(
                        id=f"stock:{symbol}",
                        source="yahoo_finance",
                        domain=self.DOMAIN,
                        title=f"{display}: ${price:.2f}" if price else f"{display}",
                        tags=["stock", "equity"],
                        extra={
                            "symbol": symbol, "name": name, "display": display,
                            "price": price, "change": round(change, 2),
                            "sparkline": sparkline, "provider": "yahoo",
                        },
                    )
                except Exception as exc:
                    logger.debug("Stock %s failed: %s", symbol, exc)
                    return None
                finally:
                    await asyncio.sleep(0.15)

        tasks = [_get_quote(s) for s in STOCK_SYMBOLS]
        results = await asyncio.gather(*tasks)
        items = [r for r in results if r is not None]
        return items


class SectorPerformanceExtractor(BaseExtractor):
    """Fetches SPDR sector ETF performance.

    Mirrors worldmonitor GetSectorSummary.
    """

    SOURCE = "sector_performance"
    DOMAIN = "market"
    REDIS_KEY = "market:sectors:v1"
    TTL_SECONDS = 3600  # 1h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for etf in SECTOR_ETFS:
            symbol = etf["symbol"]
            name = etf["name"]
            try:
                url = f"{YAHOO_CHART}/{symbol}"
                async with session.get(url, params={"interval": "1d", "range": "5d"}) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                result = data.get("chart", {}).get("result", [{}])[0]
                meta = result.get("meta", {})
                price = meta.get("regularMarketPrice", 0)
                prev = meta.get("previousClose", 0)
                change = ((price - prev) / prev * 100) if prev else 0
                items.append(IntelItem(
                    id=f"sector:{symbol}",
                    source=self.SOURCE,
                    domain=self.DOMAIN,
                    title=f"{name}: {change:+.2f}%",
                    tags=["sector", "etf"],
                    extra={
                        "symbol": symbol, "name": name,
                        "price": price, "change": round(change, 2),
                    },
                ))
                await asyncio.sleep(0.15)
            except Exception as exc:
                logger.debug("Sector %s failed: %s", symbol, exc)
        return items


class EarningsCalendarExtractor(BaseExtractor):
    """Fetches upcoming earnings from Finnhub.

    Mirrors worldmonitor ListEarningsCalendar.
    """

    SOURCE = "earnings_calendar"
    DOMAIN = "market"
    REDIS_KEY = "market:earnings:v1"
    TTL_SECONDS = 43200  # 12h

    def __init__(self, finnhub_api_key: str = ""):
        super().__init__()
        self._finnhub_key = finnhub_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._finnhub_key:
            logger.warning("Finnhub API key not set — skipping earnings")
            return []

        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        from_date = now.strftime("%Y-%m-%d")
        to_date = (now + timedelta(days=14)).strftime("%Y-%m-%d")

        url = "https://finnhub.io/api/v1/calendar/earnings"
        params = {"from": from_date, "to": to_date, "token": self._finnhub_key}
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        for entry in data.get("earningsCalendar", [])[:50]:
            symbol = entry.get("symbol", "")
            items.append(IntelItem(
                id=f"earnings:{symbol}:{entry.get('date', '')}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"Earnings: {symbol}",
                tags=["earnings", "calendar"],
                extra={
                    "symbol": symbol,
                    "date": entry.get("date"),
                    "hour": entry.get("hour"),
                    "eps_estimate": entry.get("epsEstimate"),
                    "eps_actual": entry.get("epsActual"),
                    "revenue_estimate": entry.get("revenueEstimate"),
                    "revenue_actual": entry.get("revenueActual"),
                },
            ))
        return items
