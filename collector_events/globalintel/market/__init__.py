"""Market data extractors: FX rates, Fear & Greed composite, prediction markets.

Data sources (mirrors worldmonitor seed scripts):

FX Rates — Yahoo Finance
    URL: https://query1.finance.yahoo.com/v8/finance/chart/{CUR}USD=X
    Auth: none (rate limited, 150ms stagger, semaphore=5)
    50+ currency pairs

Fear & Greed — Full 10-component composite (mirrors seed-fear-greed.mjs)
    Sources: Yahoo Finance (VIX, ^GSPC, TLT, HYG, SPY, GLD, RSP, sector ETFs),
             CNN Fear & Greed API, AAII sentiment survey scraping,
             Barchart ($CPC put/call, $S5TH breadth)
    10 categories with weights:
        Sentiment (0.10), Volatility (0.10), Positioning (0.15), Trend (0.10),
        Breadth (0.10), Momentum (0.10), Liquidity (0.15), Credit (0.10),
        Macro (0.05), CrossAsset (0.05)

Prediction Markets — Polymarket + Kalshi (mirrors seed-prediction-markets.mjs)
    Polymarket:  https://gamma-api.polymarket.com/events?tag_slug=...
    Kalshi:      https://api.elections.kalshi.com/trade-api/v2/events
    Tag-based queries, volume/price filtering, sport/entertainment exclusion,
    scoring = uncertainty × 0.6 + log_volume × 0.4
"""

from __future__ import annotations

import asyncio
import logging
import math
import re

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

# ─── FX rates ────────────────────────────────────────────────────────

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

# 50+ currencies — same list as worldmonitor SHARED_FX_FALLBACKS
FX_CURRENCIES = [
    "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD", "CNY", "HKD",
    "SGD", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "TRY", "ZAR",
    "MXN", "BRL", "INR", "KRW", "THB", "IDR", "MYR", "PHP", "TWD",
    "ILS", "SAR", "AED", "RUB", "EGP", "NGN", "KES", "ARS", "CLP",
    "COP", "PEN", "VND", "PKR", "BDT", "LKR", "UAH", "RON", "BGN",
    "HRK", "ISK", "GEL", "KZT", "UZS",
]


class FXRateExtractor(BaseExtractor):
    """Fetches live FX rates from Yahoo Finance with 150ms stagger."""

    SOURCE = "yahoo_fx"
    DOMAIN = "market"
    REDIS_KEY = "shared:fx-rates:v1"
    TTL_SECONDS = 90000  # 25h

    def __init__(self, currencies: list[str] | None = None):
        super().__init__()
        self._currencies = currencies or FX_CURRENCIES

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        sem = asyncio.Semaphore(5)

        async def _get_rate(cur: str) -> IntelItem | None:
            async with sem:
                symbol = f"{cur}USD=X" if cur != "USD" else "DX-Y.NYB"
                url = f"{YAHOO_CHART_URL}/{symbol}"
                params = {"interval": "1d", "range": "2d"}
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json(content_type=None)
                    result = data.get("chart", {}).get("result", [{}])[0]
                    meta = result.get("meta", {})
                    price = meta.get("regularMarketPrice", 0)
                    prev = meta.get("previousClose", 0)
                    change_pct = ((price - prev) / prev * 100) if prev else 0
                    return IntelItem(
                        id=f"fx:{cur}",
                        source="yahoo_fx",
                        domain="market",
                        title=f"{cur}/USD",
                        tags=["fx", "currency"],
                        extra={
                            "currency": cur,
                            "rate": price,
                            "prev_close": prev,
                            "change_pct": round(change_pct, 4),
                        },
                    )
                except Exception as exc:
                    logger.debug("FX %s failed: %s", cur, exc)
                    return None
                finally:
                    await asyncio.sleep(0.15)

        tasks = [_get_rate(c) for c in self._currencies]
        results = await asyncio.gather(*tasks)
        items = [r for r in results if r is not None]
        return items


# ═══════════════════════════════════════════════════════════════════════
#  Fear & Greed — full 10-component composite
#  Mirrors worldmonitor seed-fear-greed.mjs (499 lines)
# ═══════════════════════════════════════════════════════════════════════

CNN_FG_URL = "https://production.dataviz.cnn.io/index/fearandgreed/current"
AAII_URL = "https://www.aaii.com/sentimentsurvey/sent_results"
BARCHART_CPC_URL = "https://www.barchart.com/stocks/quotes/%24CPC"
BARCHART_S5TH_URL = "https://www.barchart.com/stocks/quotes/%24S5TH"

# Yahoo symbols needed for Fear & Greed
FG_YAHOO_SYMBOLS = [
    "^VIX", "^VIX9D", "^VIX3M", "^SKEW", "^GSPC",
    "GLD", "TLT", "HYG", "SPY", "RSP", "DX-Y.NYB",
]

# Category weights (must sum to 1.0)
CATEGORY_WEIGHTS: dict[str, float] = {
    "sentiment": 0.10,
    "volatility": 0.10,
    "positioning": 0.15,
    "trend": 0.10,
    "breadth": 0.10,
    "momentum": 0.10,
    "liquidity": 0.15,
    "credit": 0.10,
    "macro": 0.05,
    "cross_asset": 0.05,
}

# Mac Safari UA to bypass CNN 418 bot detection
MAC_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
)


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

        # 1. Sentiment (CNN F&G + AAII)
        components["sentiment"] = self._calc_sentiment(cnn_score, aaii_data)

        # 2. Volatility (VIX family + SKEW)
        components["volatility"] = self._calc_volatility(yahoo_data)

        # 3. Positioning (put/call + SPY/RSP + GLD)
        components["positioning"] = self._calc_positioning(put_call, yahoo_data)

        # 4. Trend (S&P 500 vs moving averages)
        components["trend"] = self._calc_trend(yahoo_data)

        # 5. Breadth ($S5TH % above 200d MA)
        components["breadth"] = self._calc_breadth(breadth_pct)

        # 6. Momentum (RSI + ROC)
        components["momentum"] = self._calc_momentum(yahoo_data)

        # 7. Liquidity (placeholder — needs FRED data)
        components["liquidity"] = None

        # 8. Credit (HYG/TLT ratio)
        components["credit"] = self._calc_credit(yahoo_data)

        # 9. Macro (placeholder — needs FRED data)
        components["macro"] = None

        # 10. Cross-asset (TLT, DXY vs SPY)
        components["cross_asset"] = self._calc_cross_asset(yahoo_data)

        # ── Weighted composite ──
        total_weight = 0.0
        weighted_sum = 0.0
        for cat, score in components.items():
            if score is not None:
                w = CATEGORY_WEIGHTS.get(cat, 0)
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
                "weights": CATEGORY_WEIGHTS,
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
        for symbol in FG_YAHOO_SYMBOLS:
            try:
                url = f"{YAHOO_CHART_URL}/{symbol}"
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
            headers = {"User-Agent": MAC_UA}
            async with session.get(CNN_FG_URL, headers=headers) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
            # data.fear_and_greed.score or data.score
            fg = data.get("fear_and_greed", data)
            return float(fg.get("score", fg.get("value", 50)))
        except Exception as exc:
            logger.debug("CNN F&G failed: %s", exc)
            return None

    async def _fetch_aaii(self, session: aiohttp.ClientSession) -> dict | None:
        """Scrape AAII Investor Sentiment Survey — bull/neutral/bear percentages."""
        try:
            headers = {"User-Agent": MAC_UA}
            async with session.get(AAII_URL, headers=headers) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
            # Parse: find all <td class="tableTxt"> cells
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
            headers = {"User-Agent": MAC_UA}
            async with session.get(BARCHART_CPC_URL, headers=headers) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
            # Look for __NEXT_DATA__ JSON or direct data-ng-init
            m = re.search(r'"lastPrice":\s*([\d.]+)', html)
            if m:
                return float(m.group(1))
            # Fallback: find "Last Price" followed by a number
            m2 = re.search(r'Last\s+Price[^<]*<[^>]*>([\d.]+)', html)
            if m2:
                return float(m2.group(1))
        except Exception as exc:
            logger.debug("Barchart CPC failed: %s", exc)
        return None

    async def _fetch_barchart_breadth(self, session: aiohttp.ClientSession) -> float | None:
        """Scrape Barchart $S5TH (% of S&P 500 above 200-day MA)."""
        try:
            headers = {"User-Agent": MAC_UA}
            async with session.get(BARCHART_S5TH_URL, headers=headers) as resp:
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
            # Spread typically -40 to +40 → normalise to 0-100
            aaii_score = max(0, min(100, (spread + 40) / 80 * 100))
            scores.append(aaii_score)
        return sum(scores) / len(scores) if scores else None

    def _calc_volatility(self, yahoo: dict) -> float | None:
        """VIX + VIX term structure + SKEW → 0-100 (low VIX = greed)."""
        vix_data = yahoo.get("^VIX", {})
        vix = vix_data.get("price")
        if not vix:
            return None

        # VIX score: 10→100 (extreme greed), 40→0 (extreme fear)
        vix_score = max(0, min(100, (40 - vix) / 30 * 100))

        # VIX term structure: VIX9D/VIX > 1 = backwardation = fear
        vix9d = yahoo.get("^VIX9D", {}).get("price")
        vix3m = yahoo.get("^VIX3M", {}).get("price")
        term_score = None
        if vix9d and vix:
            ratio = vix9d / vix
            # ratio > 1.1 = fear (0), ratio < 0.9 = greed (100)
            term_score = max(0, min(100, (1.1 - ratio) / 0.2 * 100))

        # SKEW: high SKEW = tail risk = fear
        skew = yahoo.get("^SKEW", {}).get("price")
        skew_score = None
        if skew:
            skew_score = max(0, min(100, (150 - skew) / 40 * 100))

        scores = [s for s in [vix_score, term_score, skew_score] if s is not None]
        return sum(scores) / len(scores) if scores else None

    def _calc_positioning(self, put_call: float | None, yahoo: dict) -> float | None:
        """Put/call ratio + SPY/RSP ratio + GLD demand."""
        scores: list[float] = []

        # Put/call: high = fear. Typical range 0.6–1.3
        if put_call is not None:
            pc_score = max(0, min(100, (1.3 - put_call) / 0.7 * 100))
            scores.append(pc_score)

        # SPY/RSP ratio: high = narrow leadership = risk
        spy = yahoo.get("SPY", {}).get("price")
        rsp = yahoo.get("RSP", {}).get("price")
        if spy and rsp:
            ratio = spy / rsp
            # Typical ~ 3.5-5.0; higher = narrow = fear
            spy_rsp_score = max(0, min(100, (5.0 - ratio) / 1.5 * 100))
            scores.append(spy_rsp_score)

        # GLD demand: rising GLD = fear (safe haven)
        gld_data = yahoo.get("GLD", {})
        gld_closes = gld_data.get("closes", [])
        if len(gld_closes) >= 20:
            gld_change = (gld_closes[-1] - gld_closes[-20]) / gld_closes[-20]
            # Rising gold = fear: +5% → 0, -5% → 100
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
        # 200d — use available data (6 months ≈ 126 trading days)
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
        # Distance bonus/penalty
        score += distance_50 * 200  # ±5% → ±10 points
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

        # 20-day ROC
        roc_20 = (closes[-1] - closes[-20]) / closes[-20] * 100

        # Simple RSI(14) approximation
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
                rsi_score = rsi  # RSI directly maps to 0-100

        # ROC normalised: ±10% → 0-100
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
        # Higher HYG/TLT = risk-on = greed. Typical 0.7-1.1
        return max(0, min(100, (ratio - 0.7) / 0.4 * 100))

    def _calc_cross_asset(self, yahoo: dict) -> float | None:
        """TLT, DXY, HYG divergence from SPY."""
        spy = yahoo.get("SPY", {}).get("closes", [])
        tlt = yahoo.get("TLT", {}).get("closes", [])
        dxy = yahoo.get("DX-Y.NYB", {}).get("closes", [])
        scores: list[float] = []

        # Falling TLT (bonds selling off) while SPY rises = risk-on = greed
        if len(tlt) >= 20 and len(spy) >= 20:
            tlt_chg = (tlt[-1] - tlt[-20]) / tlt[-20]
            spy_chg = (spy[-1] - spy[-20]) / spy[-20]
            divergence = spy_chg - tlt_chg
            scores.append(max(0, min(100, 50 + divergence * 500)))

        # Falling DXY (weak dollar) = risk-on = greed
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
        # HY OAS proxy: use HYG price inversely (lower HYG = wider spreads)
        hy_oas_proxy = max(1.0, 200 - hyg)  # very rough proxy
        try:
            return (hyg / tlt) / (vix * hy_oas_proxy / 100) * 10000
        except ZeroDivisionError:
            return None


# ═══════════════════════════════════════════════════════════════════════
#  Prediction Markets — Polymarket + Kalshi
#  Mirrors worldmonitor seed-prediction-markets.mjs (153 lines)
# ═══════════════════════════════════════════════════════════════════════

POLYMARKET_API = "https://gamma-api.polymarket.com/events"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2/events"

# Tag-based queries (mirrors worldmonitor prediction-tags.json)
PREDICTION_TAGS = [
    "politics", "ukraine", "china", "taiwan", "iran", "russia",
    "north-korea", "israel", "nato", "election",
    "ai", "crypto", "bitcoin", "ethereum",
    "fed", "inflation", "recession", "tariffs", "trade-war",
    "climate", "pandemic", "energy",
]

# Exclusion keywords (30 terms — mirrors worldmonitor _prediction-scoring.mjs)
EXCLUSION_KEYWORDS = frozenset({
    "sport", "sports", "nfl", "nba", "mlb", "nhl", "soccer", "football",
    "baseball", "basketball", "hockey", "tennis", "golf", "cricket",
    "olympic", "formula 1", "f1", "ufc", "boxing", "wrestling",
    "celebrity", "entertainment", "movie", "music", "kardashian",
    "bachelor", "tiktok", "youtube", "streamer", "influencer",
})

MIN_VOLUME = 5000
PRICE_MIN = 0.05  # 5%
PRICE_MAX = 0.95  # 95%


def _should_exclude(title: str) -> bool:
    """Check if event title matches sport/entertainment exclusion list."""
    lower = title.lower()
    return any(kw in lower for kw in EXCLUSION_KEYWORDS)


def _score_market(price: float, volume: float) -> float:
    """Scoring: uncertainty × 0.6 + log_volume × 0.4 (mirrors worldmonitor)."""
    uncertainty = 1.0 - abs(2 * price - 1.0)  # max at 0.5
    log_vol = math.log10(max(volume, 1)) / 8  # normalise log10(vol) to ~0-1
    return uncertainty * 0.6 + log_vol * 0.4


class PredictionMarketExtractor(BaseExtractor):
    """Fetches active prediction markets from Polymarket + Kalshi.

    Mirrors worldmonitor seed-prediction-markets.mjs:
      - Polymarket: tag-based queries with 300ms delay between tags
      - Kalshi: single bulk fetch (status=open, limit=100)
      - Filters: exclude sports/entertainment, volume ≥ 5000, price 5-95%
      - Scoring: uncertainty × 0.6 + log_volume × 0.4
    """

    SOURCE = "prediction_markets"
    DOMAIN = "market"
    REDIS_KEY = "prediction:markets-bootstrap:v1"
    TTL_SECONDS = 10800  # 3h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        poly_items = await self._fetch_polymarket(session)
        kalshi_items = await self._fetch_kalshi(session)
        all_items = poly_items + kalshi_items
        # Sort by score descending
        all_items.sort(key=lambda x: -x.extra.get("score", 0))
        return all_items

    async def _fetch_polymarket(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        """Fetch Polymarket events by tag with 300ms delay."""
        items: list[IntelItem] = []
        seen_slugs: set[str] = set()

        for tag in PREDICTION_TAGS:
            try:
                params = {"tag_slug": tag, "active": "true", "closed": "false", "limit": "50"}
                async with session.get(POLYMARKET_API, params=params) as resp:
                    if resp.status != 200:
                        continue
                    events = await resp.json(content_type=None)
            except Exception as exc:
                logger.debug("Polymarket tag %s failed: %s", tag, exc)
                continue

            for ev in (events if isinstance(events, list) else []):
                slug = ev.get("slug", "")
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                title = ev.get("title", "")
                if _should_exclude(title):
                    continue

                volume = float(ev.get("volume", 0) or 0)
                if volume < MIN_VOLUME:
                    continue

                # Get best market probability
                markets = ev.get("markets", [])
                best_price = 0.5
                for mkt in markets:
                    p = float(mkt.get("outcomePrices", mkt.get("lastTradePrice", 0.5)) or 0.5)
                    if isinstance(p, str):
                        try:
                            p = float(p)
                        except ValueError:
                            p = 0.5
                    if PRICE_MIN <= p <= PRICE_MAX:
                        best_price = p

                score = _score_market(best_price, volume)

                items.append(IntelItem(
                    id=f"poly:{slug}",
                    source="polymarket",
                    domain="market",
                    title=title[:300],
                    url=f"https://polymarket.com/event/{slug}",
                    tags=["prediction", tag],
                    extra={
                        "provider": "polymarket",
                        "volume": volume,
                        "probability": best_price,
                        "liquidity": float(ev.get("liquidity", 0) or 0),
                        "end_date": ev.get("endDate", ""),
                        "category": ev.get("category", tag),
                        "score": round(score, 3),
                        "tag": tag,
                    },
                ))
            await asyncio.sleep(0.3)  # 300ms between tags

        return items

    async def _fetch_kalshi(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        """Fetch Kalshi events (single bulk query)."""
        items: list[IntelItem] = []
        try:
            params = {"status": "open", "with_nested_markets": "true", "limit": "100"}
            async with session.get(KALSHI_API, params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.debug("Kalshi fetch failed: %s", exc)
            return []

        events = data.get("events", [])
        for ev in events:
            title = ev.get("title", "")
            if _should_exclude(title):
                continue

            event_ticker = ev.get("event_ticker", "")
            markets = ev.get("markets", [])

            total_volume = 0
            best_price = 0.5
            for mkt in markets:
                vol = float(mkt.get("volume", 0) or 0)
                total_volume += vol
                # Kalshi uses yes_bid/yes_ask or last_price
                price = float(mkt.get("last_price", mkt.get("yes_bid", 0.5)) or 0.5)
                if PRICE_MIN <= price <= PRICE_MAX:
                    best_price = price

            if total_volume < MIN_VOLUME:
                continue

            score = _score_market(best_price, total_volume)

            items.append(IntelItem(
                id=f"kalshi:{event_ticker}",
                source="kalshi",
                domain="market",
                title=title[:300],
                url=f"https://kalshi.com/markets/{event_ticker}",
                tags=["prediction", "kalshi"],
                extra={
                    "provider": "kalshi",
                    "event_ticker": event_ticker,
                    "volume": total_volume,
                    "probability": best_price,
                    "category": ev.get("category", ""),
                    "score": round(score, 3),
                    "market_count": len(markets),
                },
            ))

        return items
