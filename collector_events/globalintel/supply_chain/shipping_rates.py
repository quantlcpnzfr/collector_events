"""Shipping rate extractor (Freightos FBX routes — placeholder)."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_ROUTES: list[dict] = _load("supply_chain.json")["shipping_routes"]


class ShippingRateExtractor(BaseExtractor):
    """Tracks container shipping rates on major trade routes.

    Currently placeholder — requires Freightos API integration.
    """

    SOURCE = "shipping_rates"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:shipping:rates:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for route in _ROUTES:
            rid = route["id"]
            name = route["name"]
            items.append(IntelItem(
                id=f"shipping:{rid}",
                source="shipping_rates",
                domain="supply_chain",
                title=f"Shipping: {name}",
                tags=["shipping", "freight", "supply_chain"],
                extra={
                    "route_id": rid,
                    "route_name": name,
                    "status": "pending_integration",
                },
            ))
        return items
