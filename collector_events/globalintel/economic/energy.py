"""Energy data extractors: EIA, GIE AGSI+, energy capacity/prices.

Mirrors worldmonitor handlers:
  GetCrudeInventories → economic:eia:crude:v1
  GetNatGasStorage    → economic:eia:natgas:v1
  GetEuGasStorage     → economic:gie:gas:v1
  GetEnergyCapacity   → economic:energy:capacity:v1
  GetEnergyPrices     → economic:energy:prices:v1

Upstream:
  EIA: https://api.eia.gov/v2/ (requires API key)
  GIE AGSI+: https://agsi.gie.eu/api (EU gas storage)
"""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem

logger = get_logger(__name__)


class EIACrudeInventoryExtractor(BaseExtractor):
    """Fetches U.S. crude oil inventories from EIA.

    Upstream: EIA API v2 — STEO dataset or Weekly Petroleum Status Report.
    """

    SOURCE = "eia_crude"
    DOMAIN = "economic"
    REDIS_KEY = "economic:eia:crude:v1"
    TTL_SECONDS = 604800  # 7 days (weekly data)

    def __init__(self, eia_api_key: str = ""):
        super().__init__()
        self._api_key = eia_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("EIA API key not set — skipping crude inventories")
            return []

        url = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
        params = {
            "api_key": self._api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": "EPC0",  # Crude oil
            "facets[duoarea][]": "NUS",   # National US
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": "12",
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        weeks = data.get("response", {}).get("data", [])
        for i, w in enumerate(weeks):
            stocks = _safe_float(w.get("value"))
            prev_stocks = _safe_float(weeks[i + 1].get("value")) if i + 1 < len(weeks) else None
            change = (stocks - prev_stocks) if stocks is not None and prev_stocks is not None else None
            items.append(IntelItem(
                id=f"eia:crude:{w.get('period', '')}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"US Crude Inventories: {stocks:.1f} mb" if stocks else "US Crude Inventories",
                country=["US"],
                tags=["oil", "crude", "eia", "energy"],
                extra={
                    "period": w.get("period"),
                    "stocks_mb": stocks,
                    "weekly_change_mb": round(change, 2) if change is not None else None,
                },
            ))
        return items


class EIANatGasStorageExtractor(BaseExtractor):
    """Fetches U.S. natural gas storage from EIA.

    Upstream: EIA API v2 — Natural Gas Weekly Storage Report.
    """

    SOURCE = "eia_natgas"
    DOMAIN = "economic"
    REDIS_KEY = "economic:eia:natgas:v1"
    TTL_SECONDS = 604800  # 7 days

    def __init__(self, eia_api_key: str = ""):
        super().__init__()
        self._api_key = eia_api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("EIA API key not set — skipping nat gas storage")
            return []

        url = "https://api.eia.gov/v2/natural-gas/stor/wkly/data/"
        params = {
            "api_key": self._api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[process][]": "SAY",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": "12",
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        weeks = data.get("response", {}).get("data", [])
        for i, w in enumerate(weeks):
            stor = _safe_float(w.get("value"))
            prev_stor = _safe_float(weeks[i + 1].get("value")) if i + 1 < len(weeks) else None
            change = (stor - prev_stor) if stor is not None and prev_stor is not None else None
            items.append(IntelItem(
                id=f"eia:natgas:{w.get('period', '')}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"US NatGas Storage: {stor:.0f} Bcf" if stor else "US NatGas Storage",
                country=["US"],
                tags=["natgas", "storage", "eia", "energy"],
                extra={
                    "period": w.get("period"),
                    "stor_bcf": stor,
                    "weekly_change_bcf": round(change, 1) if change is not None else None,
                },
            ))
        return items


class EuGasStorageExtractor(BaseExtractor):
    """Fetches EU gas storage levels from GIE AGSI+.

    Upstream: https://agsi.gie.eu/api (no auth required for aggregated data).
    """

    SOURCE = "gie_gas"
    DOMAIN = "economic"
    REDIS_KEY = "economic:gie:gas:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        url = "https://agsi.gie.eu/api/data/eu"
        params = {"size": "30"}  # last 30 days
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        entries = data.get("data", []) if isinstance(data, dict) else data
        if not entries:
            return []

        latest = entries[0] if entries else {}
        fill_pct = _safe_float(latest.get("full"))
        prev = entries[1] if len(entries) > 1 else {}
        prev_pct = _safe_float(prev.get("full"))
        change_1d = (fill_pct - prev_pct) if fill_pct is not None and prev_pct is not None else None

        # Determine trend
        if len(entries) >= 7:
            week_ago = _safe_float(entries[6].get("full"))
            trend = "FILLING" if fill_pct and week_ago and fill_pct > week_ago else "DRAWING"
        else:
            trend = "UNKNOWN"

        history = [
            {"date": e.get("gasDayStart"), "fill_pct": _safe_float(e.get("full"))}
            for e in entries[:30]
        ]

        return [IntelItem(
            id=f"gie:eu_gas:{latest.get('gasDayStart', '')}",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title=f"EU Gas Storage: {fill_pct:.1f}%" if fill_pct else "EU Gas Storage",
            country=["EU"],
            tags=["gas", "energy", "eu", "storage"],
            extra={
                "fill_pct": fill_pct,
                "fill_pct_change_1d": round(change_1d, 2) if change_1d is not None else None,
                "trend": trend,
                "date": latest.get("gasDayStart"),
                "history": history,
            },
        )]


def _safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
