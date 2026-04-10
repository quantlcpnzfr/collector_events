"""Critical minerals concentration risk extractor."""

from __future__ import annotations

import logging

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = logging.getLogger(__name__)

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
            if len(producers) <= 2:
                severity = "HIGH"
            elif any(p.get("country") == "CN" for p in producers):
                severity = "HIGH" if producers[0].get("country") == "CN" else "MEDIUM"
            else:
                severity = "LOW"

            items.append(IntelItem(
                id=f"mineral:{name.lower().replace(' ', '_')}",
                source="critical_minerals",
                domain="supply_chain",
                title=f"Critical Mineral: {name}",
                severity=severity,
                tags=["mineral", "supply_chain", "critical"],
                extra={
                    "use": use,
                    "top_producers": producers,
                    "producer_count": len(producers),
                },
            ))
        return items
