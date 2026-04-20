"""BIS (Bank for International Settlements) data extractors.

Mirrors worldmonitor seed-bis-data.mjs → Redis → handlers:
  - GetBisPolicyRates   → economic:bis:policy:v1
  - GetBisExchangeRates → economic:bis:eer:v1
  - GetBisCredit        → economic:bis:credit:v1

Upstream: https://stats.bis.org/api/v1 (SDMX REST API, CSV format)
Auth: none
"""

from __future__ import annotations

import csv
import io
from forex_shared.logging.get_logger import get_logger
from collections import defaultdict

import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.reference import BIS_COUNTRY_CODES

logger = get_logger(__name__)

BIS_API = "https://stats.bis.org/api/v1/data"

# ─── Country code → name mapping (BIS uses 2-char) ──────────────────

BIS_COUNTRIES = {
    "US": "United States", "GB": "United Kingdom", "JP": "Japan",
    "XM": "Euro Area", "CH": "Switzerland", "SG": "Singapore",
    "IN": "India", "AU": "Australia", "CN": "China",
    "CA": "Canada", "KR": "South Korea", "BR": "Brazil",
}


class BisPolicyRateExtractor(BaseExtractor):
    """Fetches central bank policy rates from BIS.

    Upstream: WS_CBPOL dataset — monthly policy rates for 12 economies.
    """

    SOURCE = "bis_policy_rates"
    DOMAIN = "economic"
    REDIS_KEY = "economic:bis:policy:v1"
    TTL_SECONDS = 43200  # 12h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        url = f"{BIS_API}/WS_CBPOL/M.{BIS_COUNTRY_CODES}"
        params = {"startPeriod": _start_period(6), "detail": "dataonly", "format": "csv"}
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            text = await resp.text()

        items: list[IntelItem] = []
        grouped: dict[str, list[dict]] = defaultdict(list)
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            cc = row.get("REF_AREA", "")
            grouped[cc].append(row)

        for cc, rows in grouped.items():
            rows.sort(key=lambda r: r.get("TIME_PERIOD", ""), reverse=True)
            if not rows:
                continue
            current = _safe_float(rows[0].get("OBS_VALUE"))
            previous = _safe_float(rows[1].get("OBS_VALUE")) if len(rows) > 1 else None
            items.append(IntelItem(
                id=f"bis:policy:{cc}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"Policy rate: {BIS_COUNTRIES.get(cc, cc)}",
                country=[cc],
                tags=["central_bank", "policy_rate", "bis"],
                extra={
                    "country_code": cc,
                    "country_name": BIS_COUNTRIES.get(cc, cc),
                    "rate": current,
                    "previous_rate": previous,
                    "date": rows[0].get("TIME_PERIOD", ""),
                },
            ))
        return items


class BisExchangeRateExtractor(BaseExtractor):
    """Fetches real effective exchange rates (REER) from BIS.

    Upstream: WS_EER dataset — monthly REER broad basket.
    Mirrors worldmonitor seed-bis-data.mjs exchange rate section.
    """

    SOURCE = "bis_exchange_rates"
    DOMAIN = "economic"
    REDIS_KEY = "economic:bis:eer:v1"
    TTL_SECONDS = 43200  # 12h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        url = f"{BIS_API}/WS_EER/M.R.B.{BIS_COUNTRY_CODES}"
        params = {"startPeriod": _start_period(3), "detail": "dataonly", "format": "csv"}
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            text = await resp.text()

        items: list[IntelItem] = []
        grouped: dict[str, list[dict]] = defaultdict(list)
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            cc = row.get("REF_AREA", "")
            grouped[cc].append(row)

        for cc, rows in grouped.items():
            rows.sort(key=lambda r: r.get("TIME_PERIOD", ""), reverse=True)
            if not rows:
                continue
            real_eer = _safe_float(rows[0].get("OBS_VALUE"))
            prev_eer = _safe_float(rows[1].get("OBS_VALUE")) if len(rows) > 1 else None
            real_change = ((real_eer - prev_eer) / prev_eer * 100) if real_eer and prev_eer else None
            items.append(IntelItem(
                id=f"bis:eer:{cc}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"REER: {BIS_COUNTRIES.get(cc, cc)}",
                country=[cc],
                tags=["fx", "reer", "bis"],
                extra={
                    "country_code": cc,
                    "country_name": BIS_COUNTRIES.get(cc, cc),
                    "real_eer": real_eer,
                    "real_change": round(real_change, 2) if real_change else None,
                    "date": rows[0].get("TIME_PERIOD", ""),
                },
            ))
        return items


class BisCreditExtractor(BaseExtractor):
    """Fetches credit-to-GDP ratios from BIS.

    Upstream: WS_CREDIT_GAP dataset — quarterly credit-to-GDP.
    """

    SOURCE = "bis_credit"
    DOMAIN = "economic"
    REDIS_KEY = "economic:bis:credit:v1"
    TTL_SECONDS = 43200  # 12h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # WS_TC (Total Credit) with credit-to-GDP ratio key structure
        # Key: Q.{countries}.C.A.M.770.A → quarterly, all sectors, market value, credit/GDP %
        url = f"{BIS_API}/WS_TC/Q.{BIS_COUNTRY_CODES}.C.A.M.770.A"
        # Quarterly data has ~6-month publication lag; use 2-year lookback like worldmonitor
        params = {"startPeriod": _start_period_quarterly(), "detail": "dataonly", "format": "csv"}
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            text = await resp.text()

        items: list[IntelItem] = []
        grouped: dict[str, list[dict]] = defaultdict(list)
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            cc = row.get("REF_AREA", "")
            grouped[cc].append(row)

        for cc, rows in grouped.items():
            rows.sort(key=lambda r: r.get("TIME_PERIOD", ""), reverse=True)
            if not rows:
                continue
            ratio = _safe_float(rows[0].get("OBS_VALUE"))
            prev = _safe_float(rows[1].get("OBS_VALUE")) if len(rows) > 1 else None
            items.append(IntelItem(
                id=f"bis:credit:{cc}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"Credit-to-GDP: {BIS_COUNTRIES.get(cc, cc)}",
                country=[cc],
                tags=["credit", "gdp", "bis"],
                extra={
                    "country_code": cc,
                    "country_name": BIS_COUNTRIES.get(cc, cc),
                    "credit_gdp_ratio": ratio,
                    "previous_ratio": prev,
                    "date": rows[0].get("TIME_PERIOD", ""),
                },
            ))
        return items


# ─── helpers ─────────────────────────────────────────────────────────

def _start_period(months_ago: int) -> str:
    from datetime import datetime, timezone, timedelta
    dt = datetime.now(timezone.utc) - timedelta(days=months_ago * 30)
    return dt.strftime("%Y-%m")


def _start_period_quarterly() -> str:
    """Return a quarterly start period 2 years ago (e.g. '2024-Q1').

    BIS quarterly datasets have ~6-month publication lag, so a 2-year
    lookback ensures data availability (matches worldmonitor pattern).
    """
    from datetime import datetime, timezone
    dt = datetime.now(timezone.utc)
    year = dt.year - 2
    return f"{year}-Q1"


def _safe_float(v: str | None) -> float | None:
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None
