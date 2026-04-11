"""ECB Data Portal extractors.

Data sources:
  - ECB Exchange Rates  → data-api.ecb.europa.eu  (SDMX REST API)
  - ECB Yield Curve     → data-api.ecb.europa.eu  (YC dataset)
  - ECB Financial Stress Index (CISS) → data-api.ecb.europa.eu

Mirrors worldmonitor handlers:
  GetEcbFxRates  → economic:ecb:fx:v1
  GetEuYieldCurve → economic:ecb:yield:v1
  GetEuFsi        → economic:ecb:fsi:v1
"""

from __future__ import annotations

import csv
import io
from forex_shared.logging.get_logger import get_loggerfrom collections import defaultdict
from datetime import datetime, timedelta, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = get_logger(__name__)

ECB_API = "https://data-api.ecb.europa.eu/service/data"

# ECB exchange rate currencies against EUR
ECB_FX_PAIRS = [
    "USD", "GBP", "JPY", "CHF", "AUD", "CAD", "SEK", "NOK", "DKK",
    "PLN", "CZK", "HUF", "TRY", "ZAR", "BRL", "KRW", "CNY", "INR",
    "MXN", "NZD", "SGD", "HKD", "THB", "IDR", "MYR", "PHP", "RUB",
]


class EcbFxRateExtractor(BaseExtractor):
    """Fetches ECB reference exchange rates (EUR-based).

    Upstream: EXR dataset — daily reference rates for ~30 currencies.
    """

    SOURCE = "ecb_fx_rates"
    DOMAIN = "economic"
    REDIS_KEY = "economic:ecb:fx:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        keys = "+".join(ECB_FX_PAIRS)
        url = f"{ECB_API}/EXR/D.{keys}.EUR.SP00.A"
        start = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
        params = {"startPeriod": start, "format": "csvdata"}
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            text = await resp.text()

        grouped: dict[str, list[dict]] = defaultdict(list)
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            cur = row.get("CURRENCY", "")
            grouped[cur].append(row)

        items: list[IntelItem] = []
        for cur, rows in grouped.items():
            rows.sort(key=lambda r: r.get("TIME_PERIOD", ""), reverse=True)
            if not rows:
                continue
            rate = _safe_float(rows[0].get("OBS_VALUE"))
            prev = _safe_float(rows[1].get("OBS_VALUE")) if len(rows) > 1 else None
            change_1d = ((rate - prev) / prev * 100) if rate and prev else None
            items.append(IntelItem(
                id=f"ecb:fx:EUR{cur}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"EUR/{cur}",
                tags=["fx", "ecb", "reference_rate"],
                extra={
                    "pair": f"EUR{cur}",
                    "rate": rate,
                    "change_1d": round(change_1d, 4) if change_1d else None,
                    "date": rows[0].get("TIME_PERIOD", ""),
                },
            ))
        return items


class EcbYieldCurveExtractor(BaseExtractor):
    """Fetches the ECB Euro Area Yield Curve.

    Upstream: YC dataset — AAA-rated government bond yield curve.
    Tenors: 1Y, 2Y, 5Y, 10Y, 20Y, 30Y.
    """

    SOURCE = "ecb_yield_curve"
    DOMAIN = "economic"
    REDIS_KEY = "economic:ecb:yield:v1"
    TTL_SECONDS = 86400  # 24h

    TENORS = ["1Y", "2Y", "5Y", "10Y", "20Y", "30Y"]
    TENOR_CODES = {
        "1Y": "Y1", "2Y": "Y2", "5Y": "Y5",
        "10Y": "Y10", "20Y": "Y20", "30Y": "Y30",
    }

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        start = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")
        rates: dict[str, float | None] = {}
        date_str = ""

        for tenor_label, tenor_code in self.TENOR_CODES.items():
            url = f"{ECB_API}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_{tenor_code}"
            params = {"startPeriod": start, "format": "csvdata"}
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        rates[tenor_label] = None
                        continue
                    text = await resp.text()
                reader = csv.DictReader(io.StringIO(text))
                rows = sorted(reader, key=lambda r: r.get("TIME_PERIOD", ""), reverse=True)
                if rows:
                    rates[tenor_label] = _safe_float(rows[0].get("OBS_VALUE"))
                    date_str = date_str or rows[0].get("TIME_PERIOD", "")
            except Exception as exc:
                logger.debug("ECB YC %s failed: %s", tenor_label, exc)
                rates[tenor_label] = None

        return [IntelItem(
            id=f"ecb:yield_curve:{date_str}",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title="Euro Area Yield Curve (AAA)",
            tags=["yield_curve", "ecb", "bonds"],
            extra={"rates": rates, "date": date_str},
        )]


class EcbStressIndexExtractor(BaseExtractor):
    """Fetches ECB Composite Indicator of Systemic Stress (CISS).

    CISS measures financial stress in the Euro Area on a 0–1 scale.
    """

    SOURCE = "ecb_fsi"
    DOMAIN = "economic"
    REDIS_KEY = "economic:ecb:fsi:v1"
    TTL_SECONDS = 86400  # 24h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        url = f"{ECB_API}/CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX"
        start = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        params = {"startPeriod": start, "format": "csvdata"}
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            text = await resp.text()

        reader = csv.DictReader(io.StringIO(text))
        rows = sorted(reader, key=lambda r: r.get("TIME_PERIOD", ""), reverse=True)
        if not rows:
            return []

        latest = _safe_float(rows[0].get("OBS_VALUE"))
        date_str = rows[0].get("TIME_PERIOD", "")
        label = (
            "Very High" if latest and latest > 0.5 else
            "High" if latest and latest > 0.3 else
            "Elevated" if latest and latest > 0.15 else
            "Low"
        )
        history = [
            {"date": r.get("TIME_PERIOD", ""), "value": _safe_float(r.get("OBS_VALUE"))}
            for r in rows[:30]
        ]

        return [IntelItem(
            id=f"ecb:ciss:{date_str}",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title=f"ECB CISS: {label} ({latest:.3f})" if latest else "ECB CISS",
            severity="HIGH" if latest and latest > 0.3 else "",
            tags=["stress", "ecb", "systemic_risk"],
            extra={
                "latest_value": latest,
                "latest_date": date_str,
                "label": label,
                "history": history,
            },
        )]


def _safe_float(v: str | None) -> float | None:
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None
