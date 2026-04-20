"""Prediction markets extractor (Polymarket + Kalshi)."""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_logger
import math

import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.config import _load

logger = get_logger(__name__)

_CFG = _load("market.json")["prediction_markets"]
_POLYMARKET_API = _CFG["polymarket_api"]
_KALSHI_API = _CFG["kalshi_api"]
_PREDICTION_TAGS: list[str] = _CFG["tags"]
_EXCLUSION_KEYWORDS: frozenset[str] = frozenset(_CFG["exclusion_keywords"])
_MIN_VOLUME: int = _CFG["min_volume"]
_PRICE_MIN: float = _CFG["price_range"][0]
_PRICE_MAX: float = _CFG["price_range"][1]


def _should_exclude(title: str) -> bool:
    """Check if event title matches sport/entertainment exclusion list."""
    lower = title.lower()
    return any(kw in lower for kw in _EXCLUSION_KEYWORDS)


def _score_market(price: float, volume: float) -> float:
    """Scoring: uncertainty * 0.6 + log_volume * 0.4 (mirrors worldmonitor)."""
    uncertainty = 1.0 - abs(2 * price - 1.0)  # max at 0.5
    log_vol = math.log10(max(volume, 1)) / 8  # normalise log10(vol) to ~0-1
    return uncertainty * 0.6 + log_vol * 0.4


class PredictionMarketExtractor(BaseExtractor):
    """Fetches active prediction markets from Polymarket + Kalshi.

    Mirrors worldmonitor seed-prediction-markets.mjs:
      - Polymarket: tag-based queries with 300ms delay between tags
      - Kalshi: single bulk fetch (status=open, limit=100)
      - Filters: exclude sports/entertainment, volume >= 5000, price 5-95%
      - Scoring: uncertainty * 0.6 + log_volume * 0.4
    """

    SOURCE = "prediction_markets"
    DOMAIN = "market"
    REDIS_KEY = "prediction:markets-bootstrap:v1"
    TTL_SECONDS = 10800  # 3h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        poly_items = await self._fetch_polymarket(session)
        kalshi_items = await self._fetch_kalshi(session)
        all_items = poly_items + kalshi_items
        all_items.sort(key=lambda x: -x.extra.get("score", 0))
        return all_items

    async def _fetch_polymarket(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        """Fetch Polymarket events by tag with 300ms delay."""
        items: list[IntelItem] = []
        seen_slugs: set[str] = set()

        for tag in _PREDICTION_TAGS:
            try:
                params = {"tag_slug": tag, "active": "true", "closed": "false", "limit": "50"}
                async with session.get(_POLYMARKET_API, params=params) as resp:
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
                if volume < _MIN_VOLUME:
                    continue

                markets = ev.get("markets", [])
                best_price = 0.5
                for mkt in markets:
                    raw_price = mkt.get("outcomePrices", mkt.get("lastTradePrice", 0.5))
                    # outcomePrices can be a JSON string like '["0.95","0.05"]'
                    if isinstance(raw_price, str):
                        try:
                            import json
                            parsed = json.loads(raw_price)
                            if isinstance(parsed, list) and parsed:
                                p = float(parsed[0])
                            else:
                                p = float(raw_price)
                        except (json.JSONDecodeError, ValueError, IndexError):
                            p = 0.5
                    elif raw_price is None:
                        p = 0.5
                    else:
                        p = float(raw_price)
                    if _PRICE_MIN <= p <= _PRICE_MAX:
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
            async with session.get(_KALSHI_API, params=params) as resp:
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
                price = float(mkt.get("last_price", mkt.get("yes_bid", 0.5)) or 0.5)
                if _PRICE_MIN <= price <= _PRICE_MAX:
                    best_price = price

            if total_volume < _MIN_VOLUME:
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
