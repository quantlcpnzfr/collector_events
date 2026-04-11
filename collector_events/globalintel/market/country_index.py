"""Country stock index extractor — 47-country world market indices.

Mirrors worldmonitor GetCountryStockIndices / seed-country-indices.mjs:
    Source: Yahoo Finance chart API (1mo daily data)
    Calculates weekly change %, daily change %, and relative momentum vs S&P 500.

Each country maps to a Yahoo symbol (main benchmark index).
Items sorted by absolute weekly change (most volatile first).
"""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem

logger = get_logger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

# 47-country stock index map (worldmonitor shared/stocks.json + extra)
COUNTRY_INDICES: dict[str, dict] = {
    # Americas
    "United States": {"symbol": "^GSPC", "name": "S&P 500"},
    "Canada": {"symbol": "^GSPTSE", "name": "TSX Composite"},
    "Brazil": {"symbol": "^BVSP", "name": "Bovespa"},
    "Mexico": {"symbol": "^MXX", "name": "IPC"},
    "Argentina": {"symbol": "^MERV", "name": "Merval"},
    "Chile": {"symbol": "^IPSA", "name": "IPSA"},
    "Colombia": {"symbol": "^COLCAP", "name": "COLCAP"},
    "Peru": {"symbol": "^SPBLPGPT", "name": "S&P/BVL Peru"},
    # Europe
    "United Kingdom": {"symbol": "^FTSE", "name": "FTSE 100"},
    "Germany": {"symbol": "^GDAXI", "name": "DAX"},
    "France": {"symbol": "^FCHI", "name": "CAC 40"},
    "Italy": {"symbol": "FTSEMIB.MI", "name": "FTSE MIB"},
    "Spain": {"symbol": "^IBEX", "name": "IBEX 35"},
    "Netherlands": {"symbol": "^AEX", "name": "AEX"},
    "Switzerland": {"symbol": "^SSMI", "name": "SMI"},
    "Sweden": {"symbol": "^OMX", "name": "OMX Stockholm 30"},
    "Norway": {"symbol": "^OSEAX", "name": "Oslo All-Share"},
    "Denmark": {"symbol": "^OMXC25", "name": "OMX Copenhagen 25"},
    "Finland": {"symbol": "^OMXH25", "name": "OMX Helsinki 25"},
    "Belgium": {"symbol": "^BFX", "name": "BEL 20"},
    "Austria": {"symbol": "^ATX", "name": "ATX"},
    "Portugal": {"symbol": "^PSI20", "name": "PSI 20"},
    "Greece": {"symbol": "^GD.AT", "name": "Athens General"},
    "Poland": {"symbol": "^WIG20", "name": "WIG 20"},
    "Czech Republic": {"symbol": "^PX", "name": "PX"},
    "Hungary": {"symbol": "^BUX", "name": "BUX"},
    "Romania": {"symbol": "^BET", "name": "BET"},
    "Turkey": {"symbol": "XU100.IS", "name": "BIST 100"},
    "Russia": {"symbol": "IMOEX.ME", "name": "MOEX"},
    # Asia-Pacific
    "Japan": {"symbol": "^N225", "name": "Nikkei 225"},
    "China": {"symbol": "000001.SS", "name": "SSE Composite"},
    "Hong Kong": {"symbol": "^HSI", "name": "Hang Seng"},
    "South Korea": {"symbol": "^KS11", "name": "KOSPI"},
    "India": {"symbol": "^BSESN", "name": "BSE Sensex"},
    "Taiwan": {"symbol": "^TWII", "name": "TWSE"},
    "Australia": {"symbol": "^AXJO", "name": "ASX 200"},
    "New Zealand": {"symbol": "^NZ50", "name": "NZX 50"},
    "Singapore": {"symbol": "^STI", "name": "STI"},
    "Indonesia": {"symbol": "^JKSE", "name": "Jakarta Composite"},
    "Thailand": {"symbol": "^SET.BK", "name": "SET"},
    "Malaysia": {"symbol": "^KLSE", "name": "KLCI"},
    "Philippines": {"symbol": "PSEI.PS", "name": "PSEi"},
    "Vietnam": {"symbol": "^VNINDEX", "name": "VN-Index"},
    # Middle East & Africa
    "Saudi Arabia": {"symbol": "^TASI.SR", "name": "Tadawul"},
    "Israel": {"symbol": "^TA125.TA", "name": "TA-125"},
    "South Africa": {"symbol": "^J203.JO", "name": "JSE All Share"},
    "Egypt": {"symbol": "^EGX30", "name": "EGX 30"},
}


class CountryStockIndexExtractor(BaseExtractor):
    """Fetches daily data for 47 country stock indices from Yahoo Finance.

    Calculates:
      - Weekly change % (last 5 trading days)
      - Daily change % (last close vs previous)
      - Relative to S&P 500 (over/underperformance)

    Items sorted by absolute weekly change (largest moves first).
    """

    SOURCE = "country_indices"
    DOMAIN = "market"
    REDIS_KEY = "market:country-indices:v1"
    TTL_SECONDS = 43200  # 12h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        sem = asyncio.Semaphore(5)
        sp500_weekly_change: float | None = None

        async def _get_index(country: str, spec: dict) -> IntelItem | None:
            nonlocal sp500_weekly_change
            async with sem:
                try:
                    url = f"{YAHOO_CHART_URL}/{spec['symbol']}"
                    params = {"interval": "1d", "range": "1mo"}
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json(content_type=None)

                    result = data.get("chart", {}).get("result", [{}])[0]
                    meta = result.get("meta", {})
                    closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                    valid_closes = [c for c in closes if c is not None]

                    if len(valid_closes) < 2:
                        return None

                    price = meta.get("regularMarketPrice") or valid_closes[-1]
                    prev_close = valid_closes[-2] if len(valid_closes) >= 2 else price

                    # Daily change
                    daily_change = ((price - prev_close) / prev_close * 100) if prev_close else 0

                    # Weekly change (5 trading days back)
                    week_ago_idx = max(0, len(valid_closes) - 6)
                    week_ago_price = valid_closes[week_ago_idx]
                    weekly_change = ((price - week_ago_price) / week_ago_price * 100) if week_ago_price else 0

                    # Track S&P 500 for relative performance
                    if spec["symbol"] == "^GSPC":
                        sp500_weekly_change = weekly_change

                    severity = ""
                    if abs(weekly_change) > 5:
                        severity = "HIGH"
                    elif abs(weekly_change) > 2:
                        severity = "MEDIUM"

                    return IntelItem(
                        id=f"idx:{spec['symbol']}",
                        source="country_indices",
                        domain="market",
                        title=f"{country} ({spec['name']}): {weekly_change:+.1f}% weekly",
                        country=country,
                        severity=severity,
                        tags=["index", "equity", country.lower().replace(" ", "_")],
                        extra={
                            "symbol": spec["symbol"],
                            "name": spec["name"],
                            "country": country,
                            "price": round(price, 2),
                            "daily_change_pct": round(daily_change, 2),
                            "weekly_change_pct": round(weekly_change, 2),
                        },
                    )
                except Exception as exc:
                    logger.debug("Index %s (%s) failed: %s", country, spec["symbol"], exc)
                    return None
                finally:
                    await asyncio.sleep(0.2)  # 200ms stagger

        tasks = [_get_index(c, s) for c, s in COUNTRY_INDICES.items()]
        results = await asyncio.gather(*tasks)
        items = [r for r in results if r is not None]

        # Add relative performance vs S&P 500
        if sp500_weekly_change is not None:
            for item in items:
                wk = item.extra.get("weekly_change_pct", 0)
                item.extra["vs_sp500_pct"] = round(wk - sp500_weekly_change, 2)

        # Sort by absolute weekly change (most volatile first)
        items.sort(key=lambda x: -abs(x.extra.get("weekly_change_pct", 0)))
        return items
