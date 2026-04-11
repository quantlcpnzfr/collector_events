"""Shipping stress index extractor (placeholder)."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem

logger = get_logger(__name__)


class ShippingStressExtractor(BaseExtractor):
    """Composite shipping stress indicator.

    Placeholder — real version will aggregate port congestion,
    blank sailings, and rate volatility into a stress score.
    """

    SOURCE = "shipping_stress"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:shipping:stress:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        return [
            IntelItem(
                id="shipping_stress:global",
                source="shipping_stress",
                domain="supply_chain",
                title="Global Shipping Stress Index",
                severity="LOW",
                tags=["shipping", "stress", "supply_chain"],
                extra={"status": "pending_integration"},
            ),
        ]
