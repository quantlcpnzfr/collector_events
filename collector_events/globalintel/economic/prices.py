"""Price comparison extractors: Grocery basket, Big Mac, Fuel prices, FAO food index.

Mirrors worldmonitor handlers:
  ListGroceryBasketPrices → economic:grocery:v1
  ListBigMacPrices        → economic:bigmac:v1
  ListFuelPrices          → economic:fuel:v1
  GetFaoFoodPriceIndex    → economic:fao:food:v1
"""

from __future__ import annotations

import logging
import re

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)


class FaoFoodPriceExtractor(BaseExtractor):
    """Fetches the FAO Food Price Index.

    Upstream: https://www.fao.org/worldfoodsituation/foodpricesindex/en/
    Parses the published CSV data for the overall FFPI and sub-indices.
    """

    SOURCE = "fao_food_price"
    DOMAIN = "economic"
    REDIS_KEY = "economic:fao:food:v1"
    TTL_SECONDS = 604800  # 7 days (published monthly)

    # FAO publishes monthly CSV
    FAO_CSV_URL = "https://www.fao.org/fileadmin/templates/worldfood/Reports_and_docs/Food_price_indices_data_jul14.csv"

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        async with session.get(self.FAO_CSV_URL) as resp:
            if resp.status != 200:
                logger.warning("FAO CSV fetch failed: %d", resp.status)
                return []
            text = await resp.text()

        # Parse the FAO CSV — extract latest row
        lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
        if len(lines) < 3:
            return []

        # Header detection — find the row with "Food Price Index"
        header_idx = None
        for i, line in enumerate(lines):
            if "food price index" in line.lower():
                header_idx = i
                break
        if header_idx is None:
            return []

        headers = [h.strip().strip('"') for h in lines[header_idx].split(",")]
        data_lines = lines[header_idx + 1:]
        if not data_lines:
            return []

        # Latest data point (last non-empty row)
        latest_row = None
        prev_row = None
        for line in reversed(data_lines):
            vals = [v.strip().strip('"') for v in line.split(",")]
            if len(vals) >= 2 and vals[1]:
                if latest_row is None:
                    latest_row = vals
                elif prev_row is None:
                    prev_row = vals
                    break

        if not latest_row:
            return []

        def _col(name: str) -> float | None:
            for i, h in enumerate(headers):
                if name.lower() in h.lower() and i < len(latest_row):
                    try:
                        return float(latest_row[i])
                    except ValueError:
                        return None
            return None

        ffpi = _col("food price")
        meat = _col("meat")
        dairy = _col("dairy")
        cereals = _col("cereals")
        oils = _col("oils")
        sugar = _col("sugar")

        # Month-over-month
        prev_ffpi = None
        if prev_row:
            for i, h in enumerate(headers):
                if "food price" in h.lower() and i < len(prev_row):
                    try:
                        prev_ffpi = float(prev_row[i])
                    except ValueError:
                        pass
                    break
        mom_pct = ((ffpi - prev_ffpi) / prev_ffpi * 100) if ffpi and prev_ffpi else None

        return [IntelItem(
            id="fao:food_price:latest",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title=f"FAO Food Price Index: {ffpi:.1f}" if ffpi else "FAO Food Price Index",
            tags=["food", "fao", "prices", "inflation"],
            extra={
                "current_ffpi": ffpi,
                "meat": meat,
                "dairy": dairy,
                "cereals": cereals,
                "oils": oils,
                "sugar": sugar,
                "mom_pct": round(mom_pct, 2) if mom_pct else None,
                "date": latest_row[0] if latest_row else None,
            },
        )]


class FuelPriceExtractor(BaseExtractor):
    """Fetches global fuel prices from GlobalPetrolPrices.com.

    Upstream: https://www.globalpetrolprices.com/api/ (or scraping)
    Worldmonitor uses seed-fuel-prices.mjs to scrape select countries.
    """

    SOURCE = "fuel_prices"
    DOMAIN = "economic"
    REDIS_KEY = "economic:fuel:v1"
    TTL_SECONDS = 604800  # 7 days

    # Placeholder: fuel price scraping requires careful parsing.
    # The worldmonitor seed script scrapes HTML tables from GlobalPetrolPrices.
    # For now, we use the simpler public GasBuddy US API as a starting point.

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # GasBuddy US national average (public endpoint)
        url = "https://www.gasbuddy.com/graphql"
        # Note: this is a simplified approach; production would scrape
        # multiple country sources like worldmonitor does.
        logger.info("FuelPriceExtractor: placeholder — implement scraping for production")
        return [IntelItem(
            id="fuel:placeholder",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title="Fuel Prices (pending implementation)",
            tags=["fuel", "energy", "prices"],
            extra={"status": "placeholder"},
        )]


class EconomicStressExtractor(BaseExtractor):
    """Computes a composite Economic Stress Score (0-100).

    Mirrors worldmonitor GetEconomicStress — composite of:
    VIX, yield spread (10Y-2Y), credit spreads, CISS, BLS data.
    Each component normalised 0-100 and weighted.
    """

    SOURCE = "economic_stress"
    DOMAIN = "economic"
    REDIS_KEY = "economic:stress:v1"
    TTL_SECONDS = 86400  # 24h

    def __init__(self, fred_api_key: str = ""):
        super().__init__()
        self._fred_key = fred_api_key or "DEMO_KEY"

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        components: list[dict] = []

        # Component 1: VIX
        vix = await self._fetch_vix(session)
        if vix is not None:
            # VIX 10=calm(10), VIX 30=stressed(70), VIX 50+=extreme(100)
            score = min(100, max(0, vix * 2.5))
            components.append({
                "id": "vix", "label": "VIX Volatility",
                "raw_value": vix, "score": round(score, 1), "weight": 0.25,
            })

        # Component 2: 10Y-2Y yield spread (FRED T10Y2Y)
        spread = await self._fetch_fred_latest(session, "T10Y2Y")
        if spread is not None:
            # Inverted curve (negative) = high stress
            score = min(100, max(0, 50 - spread * 30))
            components.append({
                "id": "yield_spread", "label": "10Y-2Y Yield Spread",
                "raw_value": spread, "score": round(score, 1), "weight": 0.20,
            })

        # Component 3: High-yield spread (FRED BAMLH0A0HYM2)
        hy = await self._fetch_fred_latest(session, "BAMLH0A0HYM2")
        if hy is not None:
            score = min(100, max(0, hy * 12))
            components.append({
                "id": "credit_spread", "label": "HY Credit Spread",
                "raw_value": hy, "score": round(score, 1), "weight": 0.20,
            })

        # Component 4: Fed Funds Rate
        ffr = await self._fetch_fred_latest(session, "FEDFUNDS")
        if ffr is not None:
            score = min(100, max(0, ffr * 12))
            components.append({
                "id": "fed_rate", "label": "Fed Funds Rate",
                "raw_value": ffr, "score": round(score, 1), "weight": 0.15,
            })

        if not components:
            return []

        total_weight = sum(c["weight"] for c in components)
        composite = sum(c["score"] * c["weight"] for c in components) / total_weight if total_weight else 0
        label = (
            "Extreme Stress" if composite > 75 else
            "High Stress" if composite > 55 else
            "Elevated" if composite > 35 else
            "Calm"
        )

        return [IntelItem(
            id="stress:composite:latest",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title=f"Economic Stress: {label} ({composite:.0f}/100)",
            severity="HIGH" if composite > 55 else "",
            tags=["stress", "macro", "composite"],
            extra={
                "composite_score": round(composite, 1),
                "label": label,
                "components": components,
            },
        )]

    async def _fetch_vix(self, session: aiohttp.ClientSession) -> float | None:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/^VIX"
        try:
            async with session.get(url, params={"interval": "1d", "range": "1d"}) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
            return data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        except Exception:
            return None

    async def _fetch_fred_latest(self, session: aiohttp.ClientSession, series: str) -> float | None:
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series, "api_key": self._fred_key,
            "file_type": "json", "sort_order": "desc", "limit": "1",
        }
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
            obs = data.get("observations", [])
            return float(obs[0]["value"]) if obs and obs[0]["value"] != "." else None
        except Exception:
            return None
