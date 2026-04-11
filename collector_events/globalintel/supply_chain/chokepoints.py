"""Maritime chokepoint status extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_CHOKEPOINTS: list[dict] = _load("supply_chain.json")["chokepoints"]


class ChokepointStatusExtractor(BaseExtractor):
    """Reports status of critical maritime chokepoints."""

    SOURCE = "chokepoints"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:chokepoints:v1"
    TTL_SECONDS = 43200  # 12h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for cp in _CHOKEPOINTS:
            name = cp["name"]
            items.append(IntelItem(
                id=f"chokepoint:{name.lower().replace(' ', '_')}",
                source="chokepoints",
                domain="supply_chain",
                title=f"Chokepoint: {name}",
                lat=cp.get("lat"),
                lon=cp.get("lon"),
                severity="LOW",
                tags=["chokepoint", "maritime", "supply_chain"],
                extra={
                    "region": cp.get("region", ""),
                    "daily_barrels_m": cp.get("daily_barrels_m"),
                    "status": "OPEN",
                },
            ))
        return items
