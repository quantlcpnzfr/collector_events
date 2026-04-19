"""WTO tariff trend extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_CFG = _load("trade.json")
_WTO_API_BASE = _CFG["wto_api_base"]
_TARIFF_ECONOMIES = _CFG["tariff_economies"]


class TariffTrendExtractor(BaseExtractor):
    """Fetches average applied tariff rates by economy from WTO."""

    SOURCE = "tariff_trends"
    DOMAIN = "trade"
    REDIS_KEY = "trade:tariff:trends:v1"
    TTL_SECONDS = 604800  # 7d

    def __init__(self, wto_api_key: str = ""):
        super().__init__()
        self._api_key = wto_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("WTO API key not configured for tariffs")
            return []

        items: list[IntelItem] = []
        try:
            url = f"{_WTO_API_BASE}/timeseries/v1/data"
            params = {
                "i": "TP_A_TAR_01",
                "r": _TARIFF_ECONOMIES,
                "lang": "1",
            }
            headers = {"Ocp-Apim-Subscription-Key": self._api_key}
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    logger.warning("WTO tariff API returned %d", resp.status)
                    return []
                data = await resp.json(content_type=None)

            for entry in data.get("Dataset", []):
                economy = entry.get("ReportingEconomyCode", "")
                year = entry.get("Year", "")
                value = entry.get("Value", 0)

                items.append(IntelItem(
                    id=f"wto:tariff:{economy}:{year}",
                    source="wto",
                    domain="trade",
                    country=[economy] if economy else [],
                    title=f"Tariff: {economy} avg applied rate ({year})",
                    tags=["wto", "tariff"],
                    extra={
                        "economy": economy,
                        "year": year,
                        "avg_tariff_pct": value,
                    },
                ))
        except Exception as exc:
            logger.warning("WTO tariff trends failed: %s", exc)
        return items
