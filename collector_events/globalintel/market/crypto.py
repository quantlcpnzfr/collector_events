"""Cryptocurrency extractors: quotes, sectors, DeFi/AI/Other tokens, stablecoins.

Mirrors worldmonitor handlers:
  ListCryptoQuotes     → market:crypto:quotes:v1
  ListCryptoSectors    → market:crypto:sectors:v1
  ListStablecoinMarkets → market:crypto:stablecoins:v1
  ListDefiTokens       → market:crypto:defi:v1
  ListAiTokens         → market:crypto:ai:v1
  ListOtherTokens      → market:crypto:other:v1

Upstream:
  CoinGecko: https://api.coingecko.com/api/v3/
  CoinPaprika: https://api.coinpaprika.com/v1/
"""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_logger
import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.reference import (
    CRYPTO_IDS, CRYPTO_META, CRYPTO_COINPAPRIKA,
    DEFI_IDS, DEFI_META,
    AI_TOKEN_IDS, AI_TOKEN_META,
    OTHER_TOKEN_IDS, OTHER_TOKEN_META,
    STABLECOIN_IDS, STABLECOIN_COINPAPRIKA,
    CRYPTO_SECTORS,
)

logger = get_logger(__name__)

COINGECKO_API = "https://api.coingecko.com/api/v3"
COINPAPRIKA_API = "https://api.coinpaprika.com/v1"


class CryptoQuoteExtractor(BaseExtractor):
    """Fetches top 10 cryptocurrency market data from CoinGecko.

    Fallback: CoinPaprika when CoinGecko rate-limits.
    """

    SOURCE = "crypto_quotes"
    DOMAIN = "market"
    REDIS_KEY = "market:crypto:quotes:v1"
    TTL_SECONDS = 300  # 5min

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Primary: CoinGecko /coins/markets
        items = await self._try_coingecko(session, CRYPTO_IDS, CRYPTO_META, CRYPTO_COINPAPRIKA)
        return items

    async def _try_coingecko(
        self, session: aiohttp.ClientSession,
        ids: list[str], meta: dict, paprika_map: dict,
    ) -> list[IntelItem]:
        ids_str = ",".join(ids)
        url = f"{COINGECKO_API}/coins/markets"
        params = {
            "vs_currency": "usd", "ids": ids_str,
            "order": "market_cap_desc",
            "per_page": "50", "sparkline": "true",
            "price_change_percentage": "1h,24h,7d",
        }
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return self._parse_coingecko(data, meta)
        except Exception as exc:
            logger.debug("CoinGecko failed: %s — falling back to CoinPaprika", exc)

        # Fallback: CoinPaprika
        return await self._try_coinpaprika(session, paprika_map, meta)

    async def _try_coinpaprika(
        self, session: aiohttp.ClientSession,
        paprika_map: dict, meta: dict,
    ) -> list[IntelItem]:
        items: list[IntelItem] = []
        for gecko_id, paprika_id in paprika_map.items():
            try:
                url = f"{COINPAPRIKA_API}/tickers/{paprika_id}"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                qd = data.get("quotes", {}).get("USD", {})
                items.append(IntelItem(
                    id=f"crypto:{gecko_id}",
                    source="coinpaprika",
                    domain=self.DOMAIN,
                    title=f"{data.get('name', gecko_id)}: ${qd.get('price', 0):,.2f}",
                    tags=["crypto", "coinpaprika"],
                    extra={
                        "id": gecko_id,
                        "symbol": meta.get(gecko_id, {}).get("symbol", ""),
                        "price": qd.get("price"),
                        "change_24h": qd.get("percent_change_24h"),
                        "market_cap": qd.get("market_cap"),
                        "volume_24h": qd.get("volume_24h"),
                        "provider": "coinpaprika",
                    },
                ))
                await asyncio.sleep(0.1)
            except Exception as exc:
                logger.debug("CoinPaprika %s failed: %s", paprika_id, exc)
        return items

    def _parse_coingecko(self, data: list, meta: dict) -> list[IntelItem]:
        items: list[IntelItem] = []
        for coin in data:
            cid = coin.get("id", "")
            price = coin.get("current_price", 0)
            items.append(IntelItem(
                id=f"crypto:{cid}",
                source="coingecko",
                domain=self.DOMAIN,
                title=f"{coin.get('name', cid)}: ${price:,.2f}" if price else coin.get("name", cid),
                tags=["crypto", "coingecko"],
                extra={
                    "id": cid,
                    "symbol": coin.get("symbol", ""),
                    "price": price,
                    "change_1h": coin.get("price_change_percentage_1h_in_currency"),
                    "change_24h": coin.get("price_change_percentage_24h_in_currency"),
                    "change_7d": coin.get("price_change_percentage_7d_in_currency"),
                    "market_cap": coin.get("market_cap"),
                    "volume_24h": coin.get("total_volume"),
                    "sparkline": coin.get("sparkline_in_7d", {}).get("price", []),
                    "provider": "coingecko",
                },
            ))
        return items


class DefiTokenExtractor(CryptoQuoteExtractor):
    """Fetches DeFi token prices: AAVE, UNI, JUP, PENDLE, MORPHO, etc."""

    SOURCE = "defi_tokens"
    REDIS_KEY = "market:crypto:defi:v1"
    TTL_SECONDS = 600  # 10min

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        return await self._try_coingecko(session, DEFI_IDS, DEFI_META, {})


class AiTokenExtractor(CryptoQuoteExtractor):
    """Fetches AI token prices: TAO, RENDER, FET, AKT, etc."""

    SOURCE = "ai_tokens"
    REDIS_KEY = "market:crypto:ai:v1"
    TTL_SECONDS = 600

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        return await self._try_coingecko(session, AI_TOKEN_IDS, AI_TOKEN_META, {})


class OtherTokenExtractor(CryptoQuoteExtractor):
    """Fetches other notable tokens: APT, SUI, SEI, INJ, JTO, etc."""

    SOURCE = "other_tokens"
    REDIS_KEY = "market:crypto:other:v1"
    TTL_SECONDS = 600

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        return await self._try_coingecko(session, OTHER_TOKEN_IDS, OTHER_TOKEN_META, {})


class StablecoinExtractor(BaseExtractor):
    """Fetches stablecoin market data and peg deviation.

    Monitors USDT, USDC, DAI, FDUSD, USDe peg health.
    """

    SOURCE = "stablecoins"
    DOMAIN = "market"
    REDIS_KEY = "market:crypto:stablecoins:v1"
    TTL_SECONDS = 600

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        ids_str = ",".join(STABLECOIN_IDS)
        url = f"{COINGECKO_API}/coins/markets"
        params = {"vs_currency": "usd", "ids": ids_str, "order": "market_cap_desc"}

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return await self._fallback_paprika(session)
                data = await resp.json(content_type=None)
        except Exception:
            return await self._fallback_paprika(session)

        items: list[IntelItem] = []
        for coin in data:
            price = coin.get("current_price", 1.0)
            deviation = abs(price - 1.0) * 100 if price else 0
            items.append(IntelItem(
                id=f"stablecoin:{coin.get('id', '')}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"{coin.get('name', '')}: ${price:.4f}",
                severity="HIGH" if deviation > 0.5 else "",
                tags=["stablecoin", "peg"],
                extra={
                    "id": coin.get("id"),
                    "symbol": coin.get("symbol"),
                    "price": price,
                    "peg_deviation_pct": round(deviation, 4),
                    "market_cap": coin.get("market_cap"),
                    "volume_24h": coin.get("total_volume"),
                },
            ))
        return items

    async def _fallback_paprika(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        for gecko_id, paprika_id in STABLECOIN_COINPAPRIKA.items():
            try:
                url = f"{COINPAPRIKA_API}/tickers/{paprika_id}"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                price = data.get("quotes", {}).get("USD", {}).get("price", 1.0)
                deviation = abs(price - 1.0) * 100
                items.append(IntelItem(
                    id=f"stablecoin:{gecko_id}",
                    source="coinpaprika",
                    domain=self.DOMAIN,
                    title=f"{data.get('name', '')}: ${price:.4f}",
                    tags=["stablecoin", "peg"],
                    extra={
                        "id": gecko_id, "price": price,
                        "peg_deviation_pct": round(deviation, 4),
                    },
                ))
                await asyncio.sleep(0.1)
            except Exception:
                pass
        return items


class CryptoSectorExtractor(BaseExtractor):
    """Fetches aggregated crypto sector data from CoinGecko.

    Maps 8 sectors (Layer 1, DeFi, L2, AI, Memes, Gaming, Privacy, Infra)
    with their constituent tokens.
    """

    SOURCE = "crypto_sectors"
    DOMAIN = "market"
    REDIS_KEY = "market:crypto:sectors:v1"
    TTL_SECONDS = 3600  # 1h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Flatten all token IDs and batch-fetch
        all_ids: set[str] = set()
        for sec in CRYPTO_SECTORS:
            all_ids.update(sec.get("tokens", []))
        if not all_ids:
            return []

        ids_str = ",".join(all_ids)
        url = f"{COINGECKO_API}/coins/markets"
        params = {
            "vs_currency": "usd", "ids": ids_str,
            "order": "market_cap_desc", "per_page": "250",
            "price_change_percentage": "24h",
        }
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
        except Exception:
            return []

        price_map = {c["id"]: c for c in data}

        items: list[IntelItem] = []
        for sec in CRYPTO_SECTORS:
            tokens_data = []
            total_cap = 0
            changes = []
            for tid in sec.get("tokens", []):
                if tid in price_map:
                    c = price_map[tid]
                    tokens_data.append({
                        "id": tid, "symbol": c.get("symbol"),
                        "price": c.get("current_price"),
                        "change_24h": c.get("price_change_percentage_24h"),
                        "market_cap": c.get("market_cap", 0),
                    })
                    total_cap += c.get("market_cap", 0) or 0
                    ch = c.get("price_change_percentage_24h")
                    if ch is not None:
                        changes.append(ch)

            avg_change = sum(changes) / len(changes) if changes else 0
            items.append(IntelItem(
                id=f"crypto_sector:{sec['name']}",
                source=self.SOURCE,
                domain=self.DOMAIN,
                title=f"{sec['name']}: {avg_change:+.1f}%",
                tags=["crypto", "sector"],
                extra={
                    "sector": sec["name"],
                    "total_market_cap": total_cap,
                    "avg_change_24h": round(avg_change, 2),
                    "token_count": len(tokens_data),
                    "tokens": tokens_data[:10],
                },
            ))
        return items
