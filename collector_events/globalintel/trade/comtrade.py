"""UN Comtrade trade flow extractor."""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_CFG = _load("trade.json")
_COMTRADE_API_BASE = _CFG["comtrade_api_base"]
_REPORTERS: list[str] = _CFG["comtrade_reporters"]
_PARTNERS = ["0"]  # World


class ComtradeFlowExtractor(BaseExtractor):
    """Fetches bilateral trade flows from UN Comtrade."""

    SOURCE = "comtrade_flows"
    DOMAIN = "trade"
    REDIS_KEY = "trade:comtrade:flows:v1"
    TTL_SECONDS = 604800  # 7d

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        sem = asyncio.Semaphore(2)

        async def _get_reporter(reporter: str) -> list[IntelItem]:
            async with sem:
                try:
                    url = f"{_COMTRADE_API_BASE}/data/v1/get/C/A/HS"
                    params = {
                        "reporterCode": reporter,
                        "partnerCode": ",".join(_PARTNERS),
                        "flowCode": "M,X",
                        "cmdCode": "TOTAL",
                    }
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return []
                        data = await resp.json(content_type=None)
                except Exception as exc:
                    logger.debug("Comtrade %s failed: %s", reporter, exc)
                    return []

                items: list[IntelItem] = []
                for row in data.get("data", []):
                    flow = row.get("flowDesc", "")
                    value = row.get("primaryValue", 0)
                    period = row.get("period", "")
                    items.append(IntelItem(
                        id=f"comtrade:{reporter}:{flow}:{period}",
                        source="comtrade",
                        domain="trade",
                        country=[reporter] if reporter else [],
                        title=f"Comtrade: {reporter} {flow} ({period})",
                        tags=["comtrade", "trade_flow"],
                        extra={
                            "reporter": reporter,
                            "flow": flow,
                            "value_usd": value,
                            "period": period,
                        },
                    ))
                return items

        tasks = [_get_reporter(r) for r in _REPORTERS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items: list[IntelItem] = []
        for res in results:
            if isinstance(res, list):
                items.extend(res)
        return items
