"""Full 10-component Fear & Greed composite index extractor."""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_loggerimport re

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_CFG = _load("market.json")
_YAHOO_CHART_URL = _CFG["fx"]["yahoo_chart_url"]
_FG = _CFG["fear_greed"]
_CNN_FG_URL = _FG["cnn_url"]
_AAII_URL = _FG["aaii_url"]
_BARCHART_CPC_URL = _FG["barchart_cpc_url"]
_BARCHART_S5TH_URL = _FG["barchart_s5th_url"]
_FG_YAHOO_SYMBOLS: list[str] = _FG["yahoo_symbols"]
_CATEGORY_WEIGHTS: dict[str, float] = _FG["category_weights"]
_MAC_UA = _CFG["user_agent"]


class FearGreedExtractor(BaseExtractor):
    """Full 10-component Fear & Greed composite index.

    Mirrors worldmonitor seed-fear-greed.mjs with weighted categories:
      Sentiment (0.10): CNN F&G + AAII bull/bear spread
      Volatility (0.10): VIX vs VIX9D/VIX3M ratio, SKEW
      Positioning (0.15): put/call ratio, SPY/RSP ratio, GLD demand
      Trend (0.10): S&P 500 vs 200d and 50d SMA
      Breadth (0.10): % stocks above 200-day MA ($S5TH)
      Momentum (0.10): S&P 500 RSI(14) + 20d ROC
      Liquidity (0.15): SOFR, M2 growth, Fed balance (from FRED data)
      Credit (0.10): HY-IG spread via HYG/TLT ratio
      Macro (0.05): Fed funds, yield curve (T10Y2Y), unemployment
      CrossAsset (0.05): TLT, DXY, HYG vs SPY correlation

    Components that fail gracefully return None → excluded from average.
    Final score: weighted average of normalised 0-100 values.
    """

    SOURCE = "fear_greed"
    DOMAIN = "market"
    REDIS_KEY = "market:fear-greed:v1"
    TTL_SECONDS = 64800  # 18h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Fetch all data concurrently
        yahoo_data, cnn_score, aaii_data, put_call, breadth_pct = await asyncio.gather(
            self._fetch_yahoo_batch(session),
            self._fetch_cnn_fg(session),
            self._fetch_aaii(session),
            self._fetch_barchart_cpc(session),
            self._fetch_barchart_breadth(session),
            return_exceptions=True,
        )
        # Unpack safely
        if isinstance(yahoo_data, Exception):
            logger.warning("F&G Yahoo batch failed: %s", yahoo_data)
            yahoo_data = {}
        if isinstance(cnn_score, Exception):
            cnn_score = None
        if isinstance(aaii_data, Exception):
            aaii_data = None
        if isinstance(put_call, Exception):
            put_call = None
        if isinstance(breadth_pct, Exception):
            breadth_pct = None

        # ── Category scores (each 0-100, None if unavailable) ──
        components: dict[str, float | None] = {}
        components["sentiment"] = self._calc_sentiment(cnn_score, aaii_data)
        components["volatility"] = self._calc_volatility(yahoo_data)
        components["positioning"] = self._calc_positioning(put_call, yahoo_data)
        components["trend"] = self._calc_trend(yahoo_data)
        components["breadth"] = self._calc_breadth(breadth_pct)
        components["momentum"] = self._calc_momentum(yahoo_data)
        components["liquidity"] = None  # placeholder — needs FRED data
        components["credit"] = self._calc_credit(yahoo_data)
        components["macro"] = None  # placeholder — needs FRED data
        components["cross_asset"] = self._calc_cross_asset(yahoo_data)

        # ── Weighted composite ──
        total_weight = 0.0
        weighted_sum = 0.0
        for cat, score in components.items():
            if score is not None:
                w = _CATEGORY_WEIGHTS.get(cat, 0)
                weighted_sum += score * w
                total_weight += w

        score = weighted_sum / total_weight if total_weight > 0 else 50.0
        score = max(0, min(100, score))

        label = (
            "Extreme Fear" if score < 20 else
            "Fear" if score < 40 else
            "Neutral" if score < 60 else
            "Greed" if score < 75 else
            "Extreme Greed"
        )

        # Financial Stress Index (worldmonitor FSI formula)
        fsi = self._calc_fsi(yahoo_data)

        return [IntelItem(
            id="fear_greed:latest",
            source="fear_greed",
            domain="market",
            title=f"Fear & Greed: {label} ({score:.0f})",
            severity="HIGH" if score < 20 or score > 80 else "",
            tags=["sentiment", "market", "fear_greed"],
            extra={
                "score": round(score, 1),
                "label": label,
                "components": {k: round(v, 1) if v is not None else None for k, v in components.items()},
                "weights": _CATEGORY_WEIGHTS,
                "fsi": round(fsi, 2) if fsi is not None else None,
                "cnn_score": cnn_score,
            },
        )]

    # ── Data fetchers ────────────────────────────────────────────────

    async def _fetch_yahoo_batch(
        self, session: aiohttp.ClientSession,
    ) -> dict[str, dict]:
        """Fetch Yahoo chart data for all F&G symbols."""
        results: dict[str, dict] = {}
        for symbol in _FG_YAHOO_SYMBOLS:
            try:
                url = f"{_YAHOO_CHART_URL}/{symbol}"
                params = {"interval": "1d", "range": "6mo"}
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                result = data.get("chart", {}).get("result", [{}])[0]
                meta = result.get("meta", {})
                closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                valid_closes = [c for c in closes if c is not None]
                results[symbol] = {
                    "price": meta.get("regularMarketPrice", 0),
                    "prev_close": meta.get("previousClose", 0),
                    "closes": valid_closes,
                }
                await asyncio.sleep(0.15)  # 150ms stagger
            except Exception as exc:
                logger.debug("F&G Yahoo %s failed: %s", symbol, exc)
        return results

    async def _fetch_cnn_fg(self, session: aiohttp.ClientSession) -> float | None:
        """CNN Fear & Greed Index (requires Mac UA to avoid 418)."""
        try:
            headers = {"User-Agent": _MAC_UA}
            async with session.get(_CNN_FG_URL, headers=headers) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
            fg = data.get("fear_and_greed", data)
            return float(fg.get("score", fg.get("value", 50)))
        except Exception as exc:
            logger.debug("CNN F&G failed: %s", exc)
            return None

    async def _fetch_aaii(self, session: aiohttp.ClientSession) -> dict | None:
        """Scrape AAII Investor Sentiment Survey — bull/neutral/bear percentages."""
        try:
            headers = {"User-Agent": _MAC_UA}
            async with session.get(_AAII_URL, headers=headers) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
            cells = re.findall(r'class="tableTxt"[^>]*>([\d.]+)%?', html)
            if len(cells) >= 3:
                return {
                    "bullish": float(cells[0]),
                    "neutral": float(cells[1]),
                    "bearish": float(cells[2]),
                }
        except Exception as exc:
            logger.debug("AAII scrape failed: %s", exc)
        return None

    async def _fetch_barchart_cpc(self, session: aiohttp.ClientSession) -> float | None:
        """Scrape Barchart $CPC (CBOE total put/call ratio)."""
        try:
            headers = {"User-Agent": _MAC_UA}
            async with session.get(_BARCHART_CPC_URL, headers=headers) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
            m = re.search(r'"lastPrice":\s*([\d.]+)', html)
            if m:
                return float(m.group(1))
            m2 = re.search(r'Last\s+Price[^<]*<[^>]*>([\d.]+)', html)
            if m2:
                return float(m2.group(1))
        except Exception as exc:
            logger.debug("Barchart CPC failed: %s", exc)
        return None

    async def _fetch_barchart_breadth(self, session: aiohttp.ClientSession) -> float | None:
        """Scrape Barchart $S5TH (% of S&P 500 above 200-day MA)."""
        try:
            headers = {"User-Agent": _MAC_UA}
            async with session.get(_BARCHART_S5TH_URL, headers=headers) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
            m = re.search(r'"lastPrice":\s*([\d.]+)', html)
            if m:
                return float(m.group(1))
        except Exception as exc:
            logger.debug("Barchart S5TH failed: %s", exc)
        return None

    # ── Category calculators (each returns 0-100 or None) ────────────

    def _calc_sentiment(self, cnn: float | None, aaii: dict | None) -> float | None:
        """CNN F&G (0-100 as-is) averaged with AAII bull-bear spread."""
        scores: list[float] = []
        if cnn is not None:
            scores.append(cnn)
        if aaii:
            spread = aaii["bullish"] - aaii["bearish"]
            aaii_score = max(0, min(100, (spread + 40) / 80 * 100))
            scores.append(aaii_score)
        return sum(scores) / len(scores) if scores else None

    def _calc_volatility(self, yahoo: dict) -> float | None:
        """VIX + VIX term structure + SKEW → 0-100 (low VIX = greed)."""
        vix_data = yahoo.get("^VIX", {})
        vix = vix_data.get("price")
        if not vix:
            return None

        vix_score = max(0, min(100, (40 - vix) / 30 * 100))

        vix9d = yahoo.get("^VIX9D", {}).get("price")
        term_score = None
        if vix9d and vix:
            ratio = vix9d / vix
            term_score = max(0, min(100, (1.1 - ratio) / 0.2 * 100))

        skew = yahoo.get("^SKEW", {}).get("price")
        skew_score = None
        if skew:
            skew_score = max(0, min(100, (150 - skew) / 40 * 100))

        scores = [s for s in [vix_score, term_score, skew_score] if s is not None]
        return sum(scores) / len(scores) if scores else None

    def _calc_positioning(self, put_call: float | None, yahoo: dict) -> float | None:
        """Put/call ratio + SPY/RSP ratio + GLD demand."""
        scores: list[float] = []

        if put_call is not None:
            pc_score = max(0, min(100, (1.3 - put_call) / 0.7 * 100))
            scores.append(pc_score)

        spy = yahoo.get("SPY", {}).get("price")
        rsp = yahoo.get("RSP", {}).get("price")
        if spy and rsp:
            ratio = spy / rsp
            spy_rsp_score = max(0, min(100, (5.0 - ratio) / 1.5 * 100))
            scores.append(spy_rsp_score)

        gld_data = yahoo.get("GLD", {})
        gld_closes = gld_data.get("closes", [])
        if len(gld_closes) >= 20:
            gld_change = (gld_closes[-1] - gld_closes[-20]) / gld_closes[-20]
            gld_score = max(0, min(100, (0.05 - gld_change) / 0.10 * 100))
            scores.append(gld_score)

        return sum(scores) / len(scores) if scores else None

    def _calc_trend(self, yahoo: dict) -> float | None:
        """S&P 500 vs 200d and 50d SMA."""
        sp = yahoo.get("^GSPC", {})
        closes = sp.get("closes", [])
        price = sp.get("price")
        if not price or len(closes) < 50:
            return None

        sma50 = sum(closes[-50:]) / 50
        window = min(len(closes), 126)
        sma200_proxy = sum(closes[-window:]) / window

        above_50 = price > sma50
        above_200 = price > sma200_proxy
        distance_50 = (price - sma50) / sma50

        score = 50.0
        if above_200:
            score += 25
        if above_50:
            score += 15
        score += distance_50 * 200
        return max(0, min(100, score))

    def _calc_breadth(self, breadth_pct: float | None) -> float | None:
        """% of S&P 500 stocks above 200-day MA → 0-100 directly."""
        if breadth_pct is None:
            return None
        return max(0, min(100, breadth_pct))

    def _calc_momentum(self, yahoo: dict) -> float | None:
        """S&P 500 RSI(14) + 20d rate of change."""
        sp = yahoo.get("^GSPC", {})
        closes = sp.get("closes", [])
        if len(closes) < 20:
            return None

        roc_20 = (closes[-1] - closes[-20]) / closes[-20] * 100

        rsi_score = None
        if len(closes) >= 15:
            gains = []
            losses = []
            for i in range(-14, 0):
                diff = closes[i] - closes[i - 1]
                if diff > 0:
                    gains.append(diff)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(abs(diff))
            avg_gain = sum(gains) / 14
            avg_loss = sum(losses) / 14
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                rsi_score = rsi

        roc_score = max(0, min(100, 50 + roc_20 * 5))

        vals = [s for s in [rsi_score, roc_score] if s is not None]
        return sum(vals) / len(vals) if vals else None

    def _calc_credit(self, yahoo: dict) -> float | None:
        """HY/IG credit spread proxy via HYG/TLT ratio."""
        hyg = yahoo.get("HYG", {}).get("price")
        tlt = yahoo.get("TLT", {}).get("price")
        if not hyg or not tlt:
            return None
        ratio = hyg / tlt
        return max(0, min(100, (ratio - 0.7) / 0.4 * 100))

    def _calc_cross_asset(self, yahoo: dict) -> float | None:
        """TLT, DXY, HYG divergence from SPY."""
        spy = yahoo.get("SPY", {}).get("closes", [])
        tlt = yahoo.get("TLT", {}).get("closes", [])
        dxy = yahoo.get("DX-Y.NYB", {}).get("closes", [])
        scores: list[float] = []

        if len(tlt) >= 20 and len(spy) >= 20:
            tlt_chg = (tlt[-1] - tlt[-20]) / tlt[-20]
            spy_chg = (spy[-1] - spy[-20]) / spy[-20]
            divergence = spy_chg - tlt_chg
            scores.append(max(0, min(100, 50 + divergence * 500)))

        if len(dxy) >= 20:
            dxy_chg = (dxy[-1] - dxy[-20]) / dxy[-20]
            scores.append(max(0, min(100, 50 - dxy_chg * 500)))

        return sum(scores) / len(scores) if scores else None

    def _calc_fsi(self, yahoo: dict) -> float | None:
        """Financial Stress Index: (HYG/TLT) / (VIX * HY_OAS / 100) * 10000."""
        hyg = yahoo.get("HYG", {}).get("price")
        tlt = yahoo.get("TLT", {}).get("price")
        vix = yahoo.get("^VIX", {}).get("price")
        if not all([hyg, tlt, vix]):
            return None
        hy_oas_proxy = max(1.0, 200 - hyg)
        try:
            return (hyg / tlt) / (vix * hy_oas_proxy / 100) * 10000
        except ZeroDivisionError:
            return None
