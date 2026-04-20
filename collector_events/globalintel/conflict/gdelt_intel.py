"""GDELT intelligence article extractor."""

from __future__ import annotations

import asyncio
from forex_shared.logging.get_logger import get_logger
import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.config import _load

logger = get_logger(__name__)

_CFG = _load("conflict.json")["gdelt"]
_DOC_API = _CFG["doc_api"]
_TOPICS: dict[str, str] = _CFG["topics"]


class GDELTIntelExtractor(BaseExtractor):
    """Fetches intelligence articles from GDELT Doc API.

    Mirrors worldmonitor seed-gdelt-intel.mjs:
      - 6 thematic topics with boolean queries
      - 75 articles per topic, 14-day window
      - Retry with exponential backoff (60s → 120s → 240s) on failures
      - 20s delay between topics to respect GDELT rate limits
    """

    SOURCE = "gdelt"
    DOMAIN = "conflict"
    REDIS_KEY = "intelligence:gdelt-intel:v1"
    TTL_SECONDS = 86400

    GDELT_MAX_RETRIES = 3
    GDELT_BACKOFF_BASE_S = 60

    def __init__(self, topics: dict[str, str] | list[str] | None = None, rate_limit_s: float = 20):
        super().__init__()
        self._topics = topics or _TOPICS
        self._rate_limit_s = rate_limit_s

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        all_items: list[IntelItem] = []
        # Config may be a list (from JSON) or dict — normalise iteration
        if isinstance(self._topics, dict):
            topic_iter = self._topics.items()
        else:
            topic_iter = ((f"topic_{i}", q) for i, q in enumerate(self._topics))
        for topic_key, query in topic_iter:
            articles = await self._fetch_topic(session, topic_key, query)
            all_items.extend(articles)
            if self._rate_limit_s > 0:
                await asyncio.sleep(self._rate_limit_s)
        return all_items

    async def _fetch_topic(
        self,
        session: aiohttp.ClientSession,
        topic_key: str,
        query: str,
    ) -> list[IntelItem]:
        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": "75",
            "format": "json",
            "sort": "datedesc",
            "timespan": "14d",
        }
        last_exc: Exception | None = None
        for attempt in range(self.GDELT_MAX_RETRIES):
            try:
                async with session.get(_DOC_API, params=params) as resp:
                    if resp.status == 429:
                        wait = self.GDELT_BACKOFF_BASE_S * (2 ** attempt)
                        logger.warning("GDELT 429 on %s — backoff %ds", topic_key, wait)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                return self._parse_articles(data, topic_key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                wait = self.GDELT_BACKOFF_BASE_S * (2 ** attempt)
                logger.warning("GDELT %s attempt %d failed: %s — retry in %ds", topic_key, attempt + 1, exc, wait)
                await asyncio.sleep(wait)

        logger.error("GDELT topic %s all retries exhausted: %s", topic_key, last_exc)
        return []

    def _parse_articles(self, data: dict, topic_key: str) -> list[IntelItem]:
        items: list[IntelItem] = []
        for art in data.get("articles", []):
            url = art.get("url", "")
            items.append(IntelItem(
                id=f"gdelt:{topic_key}:{url[:80]}",
                source="gdelt",
                domain="conflict",
                title=art.get("title", "")[:300],
                url=url,
                ts=art.get("seendate", ""),
                tags=[topic_key],
                extra={
                    "topic": topic_key,
                    "tone": art.get("tone", 0),
                    "source_domain": art.get("domain", ""),
                    "language": art.get("language", ""),
                    "image": art.get("socialimage", ""),
                },
            ))
        return items
