"""CFTC Commitments of Traders (COT) positioning extractor.

Mirrors worldmonitor seed-cot.mjs (two Socrata resources):

1. **TFF Combined** (Traders in Financial Futures):
   Resource: ``yw9f-hn96``
   Instruments matched by regex on ``market_and_exchange_names``:
     ES (E-MINI S&P), NQ (E-MINI NASDAQ), ZN (10-YEAR NOTE),
     ZT (2-YEAR NOTE), EC (EURO FX), JY (JAPANESE YEN)
   Fields: ``asset_mgr_positions_{long,short}_all``,
           ``lev_money_positions_{long,short}_all``,
           ``dealer_positions_{long,short}_all``

2. **Disaggregated** (physical/commercial commodities):
   Resource: ``rxbv-e226``
   Instruments matched by ``cftc_contract_market_code``:
     Gold (088691), WTI Crude (067651)
   Fields: ``m_money_positions_{long,short}_all``,
           ``swap_positions_{long,short}_all``

Net positioning:
    ``net_pct = ((long - short) / max(long + short, 1)) * 100``
"""

from __future__ import annotations

import logging
import re

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

CFTC_BASE = "https://publicreporting.cftc.gov/resource"
TFF_RESOURCE = "yw9f-hn96"      # Traders in Financial Futures
DISAGG_RESOURCE = "rxbv-e226"   # Disaggregated

# ─── TFF contracts (financial futures) ───────────────────────────────
# key = label, regex = match on market_and_exchange_names (case-insensitive)
TFF_CONTRACTS: dict[str, dict] = {
    "ES": {"regex": r"E-MINI S&P 500", "symbol": "ES", "label": "S&P 500 E-mini"},
    "NQ": {"regex": r"E-MINI NASDAQ", "symbol": "NQ", "label": "Nasdaq E-mini"},
    "ZN": {"regex": r"10(-| )YEAR.*NOTE", "symbol": "ZN", "label": "10Y Treasury"},
    "ZT": {"regex": r"2(-| )YEAR.*NOTE", "symbol": "ZT", "label": "2Y Treasury"},
    "EC": {"regex": r"EURO FX", "symbol": "EC", "label": "Euro FX"},
    "JY": {"regex": r"JAPANESE YEN", "symbol": "JY", "label": "Japanese Yen"},
}

# ─── Disaggregated contracts (commodities) ───────────────────────────
# key = label, cftc_code = match on cftc_contract_market_code
DISAGG_CONTRACTS: dict[str, dict] = {
    "Gold":  {"cftc_code": "088691", "symbol": "GC", "label": "Gold"},
    "WTI":   {"cftc_code": "067651", "symbol": "CL", "label": "WTI Crude"},
}


def _net_pct(long_val: int, short_val: int) -> float:
    """Net positioning as percentage: ((L - S) / max(L + S, 1)) * 100."""
    total = max(long_val + short_val, 1)
    return round((long_val - short_val) / total * 100, 2)


def _safe_int(v: object) -> int:
    """Parse to int, default 0."""
    try:
        return int(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


class CotPositioningExtractor(BaseExtractor):
    """Fetches CFTC COT positioning from TFF + Disaggregated reports.

    Uses two Socrata endpoints:
      - TFF Combined (yw9f-hn96):  asset_mgr / lev_money / dealer positions
      - Disaggregated (rxbv-e226): m_money / swap positions

    Returns per-instrument net positioning percentages and week-over-week
    changes for asset managers, leveraged money, and dealer categories.
    """

    SOURCE = "cot_positioning"
    DOMAIN = "market"
    REDIS_KEY = "market:cot:v1"
    TTL_SECONDS = 604800  # 7 days (published weekly on Friday)

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        tff_items = await self._fetch_tff(session)
        disagg_items = await self._fetch_disagg(session)
        return tff_items + disagg_items

    async def _fetch_tff(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        """Fetch Traders in Financial Futures Combined report."""
        url = f"{CFTC_BASE}/{TFF_RESOURCE}.json"
        params = {
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$limit": "500",
        }
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("COT TFF fetch failed: %s", exc)
            return []

        # Group rows by contract
        by_contract: dict[str, list[dict]] = {}
        for row in data:
            name = (row.get("market_and_exchange_names") or "").upper()
            for key, spec in TFF_CONTRACTS.items():
                if re.search(spec["regex"], name, re.IGNORECASE):
                    by_contract.setdefault(key, []).append(row)
                    break

        items: list[IntelItem] = []
        for key, spec in TFF_CONTRACTS.items():
            rows = by_contract.get(key, [])
            rows.sort(key=lambda r: r.get("report_date_as_yyyy_mm_dd", ""), reverse=True)
            if not rows:
                continue

            latest = rows[0]
            prev = rows[1] if len(rows) > 1 else {}

            # Asset Manager positions
            am_long = _safe_int(latest.get("asset_mgr_positions_long_all"))
            am_short = _safe_int(latest.get("asset_mgr_positions_short_all"))
            am_net_pct = _net_pct(am_long, am_short)

            # Leveraged Money positions
            lm_long = _safe_int(latest.get("lev_money_positions_long_all"))
            lm_short = _safe_int(latest.get("lev_money_positions_short_all"))
            lm_net_pct = _net_pct(lm_long, lm_short)

            # Dealer positions
            dl_long = _safe_int(latest.get("dealer_positions_long_all"))
            dl_short = _safe_int(latest.get("dealer_positions_short_all"))
            dl_net_pct = _net_pct(dl_long, dl_short)

            # Previous week for change calculation
            prev_am_long = _safe_int(prev.get("asset_mgr_positions_long_all"))
            prev_am_short = _safe_int(prev.get("asset_mgr_positions_short_all"))
            prev_am_net_pct = _net_pct(prev_am_long, prev_am_short) if prev else None

            am_change = round(am_net_pct - prev_am_net_pct, 2) if prev_am_net_pct is not None else None

            # Overall sentiment based on asset manager positioning
            sentiment = "BULLISH" if am_net_pct > 10 else "BEARISH" if am_net_pct < -10 else "NEUTRAL"

            items.append(IntelItem(
                id=f"cot:tff:{spec['symbol']}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"COT {spec['label']}: {sentiment} (AM net {am_net_pct:+.1f}%)",
                tags=["cot", "positioning", "cftc", "tff"],
                extra={
                    "report": "TFF",
                    "contract": key,
                    "symbol": spec["symbol"],
                    "label": spec["label"],
                    "date": latest.get("report_date_as_yyyy_mm_dd"),
                    "asset_mgr": {"long": am_long, "short": am_short, "net_pct": am_net_pct, "change_1w": am_change},
                    "lev_money": {"long": lm_long, "short": lm_short, "net_pct": lm_net_pct},
                    "dealer": {"long": dl_long, "short": dl_short, "net_pct": dl_net_pct},
                    "sentiment": sentiment,
                },
            ))
        return items

    async def _fetch_disagg(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        """Fetch Disaggregated report for commodities."""
        url = f"{CFTC_BASE}/{DISAGG_RESOURCE}.json"
        params = {
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$limit": "200",
        }
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("COT Disagg fetch failed: %s", exc)
            return []

        # Group by CFTC contract market code
        by_code: dict[str, list[dict]] = {}
        for row in data:
            code = (row.get("cftc_contract_market_code") or "").strip()
            for key, spec in DISAGG_CONTRACTS.items():
                if code == spec["cftc_code"]:
                    by_code.setdefault(key, []).append(row)
                    break

        items: list[IntelItem] = []
        for key, spec in DISAGG_CONTRACTS.items():
            rows = by_code.get(key, [])
            rows.sort(key=lambda r: r.get("report_date_as_yyyy_mm_dd", ""), reverse=True)
            if not rows:
                continue

            latest = rows[0]
            prev = rows[1] if len(rows) > 1 else {}

            # Managed Money positions
            mm_long = _safe_int(latest.get("m_money_positions_long_all"))
            mm_short = _safe_int(latest.get("m_money_positions_short_all"))
            mm_net_pct = _net_pct(mm_long, mm_short)

            # Swap positions
            sw_long = _safe_int(latest.get("swap_positions_long_all"))
            sw_short = _safe_int(latest.get("swap_positions_short_all"))
            sw_net_pct = _net_pct(sw_long, sw_short)

            # Previous week change
            prev_mm_long = _safe_int(prev.get("m_money_positions_long_all"))
            prev_mm_short = _safe_int(prev.get("m_money_positions_short_all"))
            prev_mm_net_pct = _net_pct(prev_mm_long, prev_mm_short) if prev else None
            mm_change = round(mm_net_pct - prev_mm_net_pct, 2) if prev_mm_net_pct is not None else None

            sentiment = "BULLISH" if mm_net_pct > 10 else "BEARISH" if mm_net_pct < -10 else "NEUTRAL"

            items.append(IntelItem(
                id=f"cot:disagg:{spec['symbol']}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"COT {spec['label']}: {sentiment} (MM net {mm_net_pct:+.1f}%)",
                tags=["cot", "positioning", "cftc", "disaggregated"],
                extra={
                    "report": "Disaggregated",
                    "contract": key,
                    "symbol": spec["symbol"],
                    "label": spec["label"],
                    "date": latest.get("report_date_as_yyyy_mm_dd"),
                    "managed_money": {"long": mm_long, "short": mm_short, "net_pct": mm_net_pct, "change_1w": mm_change},
                    "swap": {"long": sw_long, "short": sw_short, "net_pct": sw_net_pct},
                    "sentiment": sentiment,
                },
            ))
        return items
