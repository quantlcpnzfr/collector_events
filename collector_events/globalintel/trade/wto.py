"""WTO trade restriction extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_CFG = _load("trade.json")
_WTO_API_BASE = _CFG["wto_api_base"]


class WtoTradeRestrictionExtractor(BaseExtractor):
    """Fetches WTO trade policy measures (restrictions/barriers)."""

    SOURCE = "wto_restrictions"
    DOMAIN = "trade"
    REDIS_KEY = "trade:wto:restrictions:v1"
    TTL_SECONDS = 604800  # 7d

    def __init__(self, wto_api_key: str = ""):
        super().__init__()
        self._api_key = wto_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("WTO API key not configured")
            return []

        items: list[IntelItem] = []
        try:
            url = f"{_WTO_API_BASE}/timeseries/v1/data"
            params = {"i": "TP_TM_MEASURES", "lang": "1"}
            headers = {"Ocp-Apim-Subscription-Key": self._api_key}
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    logger.warning("WTO API returned %d", resp.status)
                    return []
                data = await resp.json(content_type=None)

            for entry in data.get("Dataset", []):
                reporter = entry.get("ReportingEconomyCode", "")
                measure = entry.get("ProductOrSector", "Trade policy measure")
                year = entry.get("Year", "")
                value = entry.get("Value", 0)

                items.append(IntelItem(
                    id=f"wto:restrictions:{reporter}:{year}",
                    source="wto",
                    domain="trade",
                    title=f"WTO: {reporter} - {measure} ({year})",
                    tags=["wto", "trade_restriction"],
                    extra={
                        "reporter": reporter,
                        "measure": measure,
                        "year": year,
                        "value": value,
                    },
                ))
        except Exception as exc:
            logger.warning("WTO trade restrictions failed: %s", exc)
        return items
