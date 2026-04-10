"""Supply chain intelligence extractors.

Mirrors worldmonitor handlers:
  GetShippingRates    → supply:shipping:rates:v1
  GetShippingStress   → supply:shipping:stress:v1
  GetChokepointStatus → supply:chokepoints:v1
  GetCriticalMinerals → supply:minerals:v1

Upstream:
  Freightos Baltic Index: https://fbx.freightos.com/ (API)
  UNCTAD/GISIS for chokepoints (or curated data)
  USGS for critical minerals
"""

from __future__ import annotations

import logging

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)


# ─── Key maritime chokepoints ────────────────────────────────────────

CHOKEPOINTS = [
    {"id": "suez", "name": "Suez Canal", "lat": 30.585, "lon": 32.265, "country": "EG",
     "daily_ships": 50, "pct_global_trade": 12},
    {"id": "hormuz", "name": "Strait of Hormuz", "lat": 26.6, "lon": 56.25, "country": "OM",
     "daily_ships": 20, "pct_global_trade": 21},  # % oil
    {"id": "malacca", "name": "Strait of Malacca", "lat": 2.5, "lon": 101.8, "country": "MY",
     "daily_ships": 80, "pct_global_trade": 25},
    {"id": "panama", "name": "Panama Canal", "lat": 9.08, "lon": -79.68, "country": "PA",
     "daily_ships": 35, "pct_global_trade": 6},
    {"id": "bosporus", "name": "Turkish Straits", "lat": 41.12, "lon": 29.08, "country": "TR",
     "daily_ships": 120, "pct_global_trade": 3},
    {"id": "gibraltar", "name": "Strait of Gibraltar", "lat": 35.96, "lon": -5.60, "country": "ES",
     "daily_ships": 300, "pct_global_trade": 5},
    {"id": "bab_el_mandeb", "name": "Bab el-Mandeb", "lat": 12.6, "lon": 43.3, "country": "DJ",
     "daily_ships": 40, "pct_global_trade": 10},
    {"id": "cape_good_hope", "name": "Cape of Good Hope", "lat": -34.35, "lon": 18.47, "country": "ZA",
     "daily_ships": 50, "pct_global_trade": 0},  # alternate
]

# ─── Critical minerals (from USGS and worldmonitor mining-sites.js) ──

CRITICAL_MINERALS = [
    {"id": "lithium", "name": "Lithium", "top_producers": ["AU", "CL", "CN"], "use": "EV batteries"},
    {"id": "cobalt", "name": "Cobalt", "top_producers": ["CD", "AU", "PH"], "use": "EV batteries"},
    {"id": "rare_earth", "name": "Rare Earth Elements", "top_producers": ["CN", "US", "MM"], "use": "Electronics, defense"},
    {"id": "nickel", "name": "Nickel", "top_producers": ["ID", "PH", "RU"], "use": "Stainless steel, EV batteries"},
    {"id": "copper", "name": "Copper", "top_producers": ["CL", "PE", "CD"], "use": "Electrical, construction"},
    {"id": "uranium", "name": "Uranium", "top_producers": ["KZ", "CA", "NA"], "use": "Nuclear energy"},
    {"id": "manganese", "name": "Manganese", "top_producers": ["ZA", "GA", "AU"], "use": "Steel production"},
    {"id": "graphite", "name": "Graphite", "top_producers": ["CN", "MZ", "BR"], "use": "Batteries, lubricants"},
    {"id": "tin", "name": "Tin", "top_producers": ["CN", "ID", "MM"], "use": "Electronics, solder"},
    {"id": "tungsten", "name": "Tungsten", "top_producers": ["CN", "VN", "RW"], "use": "Hard metals, defense"},
]


class ChokepointStatusExtractor(BaseExtractor):
    """Reports status of major maritime chokepoints.

    Combines curated static data with live shipping indicators.
    In production, would integrate AIS data feeds and news signals.
    """

    SOURCE = "chokepoints"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:chokepoints:v1"
    TTL_SECONDS = 43200  # 12h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for cp in CHOKEPOINTS:
            # Status is OPEN by default — override via news/AIS feed
            status = "OPEN"
            items.append(IntelItem(
                id=f"chokepoint:{cp['id']}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"{cp['name']}: {status}",
                lat=cp["lat"],
                lon=cp["lon"],
                country=cp["country"],
                tags=["chokepoint", "maritime", "shipping"],
                extra={
                    "chokepoint_id": cp["id"],
                    "name": cp["name"],
                    "status": status,
                    "daily_ships": cp["daily_ships"],
                    "pct_global_trade": cp["pct_global_trade"],
                },
            ))
        return items


class CriticalMineralsExtractor(BaseExtractor):
    """Reports critical mineral supply concentrations.

    Data source: USGS Mineral Commodity Summaries + static reference.
    In production, would fetch real-time pricing and supply disruption signals.
    """

    SOURCE = "critical_minerals"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:minerals:v1"
    TTL_SECONDS = 604800  # 7 days

    USGS_URL = "https://pubs.usgs.gov/periodicals/mcs2024/mcs2024.pdf"

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for mineral in CRITICAL_MINERALS:
            top = mineral["top_producers"]
            # Concentration risk: if top producer has >50% share
            concentration = "HIGH" if len(top) == 1 else "MODERATE" if top[0] == "CN" else "LOW"
            items.append(IntelItem(
                id=f"mineral:{mineral['id']}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"{mineral['name']}: {concentration} concentration risk",
                tags=["mineral", "critical", "supply_chain"],
                extra={
                    "mineral": mineral["name"],
                    "use_case": mineral["use"],
                    "top_producers": top,
                    "concentration_risk": concentration,
                },
            ))
        return items


class ShippingRateExtractor(BaseExtractor):
    """Fetches global container shipping rates.

    Upstream: Freightos Baltic Index (FBX) or DrewryWCI.
    Worldmonitor seed-shipping-rates.mjs sources FBX API.
    """

    SOURCE = "shipping_rates"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:shipping:rates:v1"
    TTL_SECONDS = 86400  # 24h

    # Key lanes
    ROUTES = [
        {"id": "fbx01", "name": "China/East Asia → North America West Coast"},
        {"id": "fbx03", "name": "China/East Asia → North Europe"},
        {"id": "fbx11", "name": "China/East Asia → Mediterranean"},
        {"id": "fbx13", "name": "North Europe → North America East Coast"},
    ]

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Freightos Baltic Index requires API partnership.
        # Placeholder: would integrate FBX API or DrewryWCI API.
        # For now, return reference route definitions.
        items: list[IntelItem] = []
        for route in self.ROUTES:
            items.append(IntelItem(
                id=f"shipping:{route['id']}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"Shipping: {route['name']}",
                tags=["shipping", "container", "rates"],
                extra={
                    "route_id": route["id"],
                    "route_name": route["name"],
                    "status": "pending_api_integration",
                },
            ))
        return items


class ShippingStressExtractor(BaseExtractor):
    """Computes composite shipping stress index.

    Components:
    - Rate deviation from 12-month avg
    - Chokepoint disruptions (from ChokepointStatus)
    - Port congestion indicators
    """

    SOURCE = "shipping_stress"
    DOMAIN = "supply_chain"
    REDIS_KEY = "supply:shipping:stress:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Composite stress score — would aggregate from ShippingRates + news
        logger.info("ShippingStressExtractor: requires ShippingRates and news data for full computation")
        return [IntelItem(
            id="supply:stress:composite",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title="Shipping Stress Index (pending full integration)",
            tags=["shipping", "stress", "composite"],
            extra={"status": "pending_integration"},
        )]
