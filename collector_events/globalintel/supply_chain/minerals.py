"""Critical minerals concentration risk extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_MINERALS: list[dict] = _load("supply_chain.json")["critical_minerals"]


class CriticalMineralsExtractor(BaseExtractor):
    """Reports critical mineral concentration risk by dominant producer."""

    SOURCE = "critical_minerals"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:minerals:v1"
    TTL_SECONDS = 604800  # 7d

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for mineral in _MINERALS:
            name = mineral["name"]
            producers = mineral.get("top_producers", [])
            use = mineral.get("use", "")

            # Concentration risk based on producer dominance
            # producers may be country code strings ["CN", "AU"] or dicts [{"country": "CN"}]
            def _country(p) -> str:
                return p.get("country", p) if isinstance(p, dict) else str(p)

            if len(producers) <= 2:
                severity = "HIGH"
            elif any(_country(p) == "CN" for p in producers):
                severity = "HIGH" if _country(producers[0]) == "CN" else "MEDIUM"
            else:
                severity = "LOW"

            # Extract country codes from producers list
            producer_codes = [_country(p).upper() for p in producers if _country(p)]

            items.append(IntelItem(
                id=f"mineral:{name.lower().replace(' ', '_')}",
                source="critical_minerals",
                domain="supply_chain",
                title=f"Critical Mineral: {name}",
                country=producer_codes,
                severity=severity,
                tags=["mineral", "supply_chain", "critical"],
                extra={
                    "use": use,
                    "top_producers": producers,
                    "producer_count": len(producers),
                },
            ))
        return items
