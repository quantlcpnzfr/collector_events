"""FX rate extractor via Yahoo Finance."""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_logger
import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.config import _load

logger = get_logger(__name__)

_CFG = _load("market.json")
_YAHOO_CHART_URL = _CFG["fx"]["yahoo_chart_url"]
_FX_CURRENCIES: list[str] = _CFG["fx"]["currencies"]


class FXRateExtractor(BaseExtractor):
    """Fetches live FX rates from Yahoo Finance with 150ms stagger."""

    SOURCE = "yahoo_fx"
    DOMAIN = "market"
    REDIS_KEY = "shared:fx-rates:v1"
    TTL_SECONDS = 90000  # 25h

    def __init__(self, currencies: list[str] | None = None):
        super().__init__()
        self._currencies = currencies or _FX_CURRENCIES

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        sem = asyncio.Semaphore(5)

        async def _get_rate(cur: str) -> IntelItem | None:
            async with sem:
                symbol = f"{cur}USD=X" if cur != "USD" else "DX-Y.NYB"
                url = f"{_YAHOO_CHART_URL.rstrip('/')}/{symbol}"
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json(content_type=None)
                    result = data.get("chart", {}).get("result", [{}])[0]
                    meta = result.get("meta", {})
                    price = meta.get("regularMarketPrice", 0)
                    prev = meta.get("previousClose", 0)
                    change_pct = ((price - prev) / prev * 100) if prev else 0
                    return IntelItem(
                        id=f"fx:{cur}",
                        source="yahoo_fx",
                        domain="market",
                        title=f"{cur}/USD",
                        tags=["fx", "currency"],
                        extra={
                            "currency": cur,
                            "rate": price,
                            "prev_close": prev,
                            "change_pct": round(change_pct, 4),
                        },
                    )
                except Exception as exc:
                    logger.debug("FX %s failed: %s", cur, exc)
                    return None
                finally:
                    await asyncio.sleep(0.15)

        tasks = [_get_rate(c) for c in self._currencies]
        results = await asyncio.gather(*tasks)
        items = [r for r in results if r is not None]
        return items
