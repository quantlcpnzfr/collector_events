"""Reddit velocity extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import math
import time

import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.config import _load

logger = get_logger(__name__)

_CFG = _load("social.json")
_REDDIT_BASE = _CFG["reddit"]["base_url"]
_REDDIT_SUBREDDITS: list[str] = _CFG["reddit"]["subreddits"]


class RedditVelocityExtractor(BaseExtractor):
    """Fetches trending posts from geopolitical subreddits.

    Scores by velocity: log1p(score) * ratio * recency * 100
    where recency = exp(-age_seconds / 21600) (6h half-life).
    """

    SOURCE = "reddit"
    DOMAIN = "social"
    REDIS_KEY = "intelligence:social:reddit:v1"
    TTL_SECONDS = 1800

    def __init__(self, subreddits: list[str] | None = None, limit: int = 25):
        super().__init__()
        self._subreddits = subreddits or _REDDIT_SUBREDDITS
        self._limit = limit

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        now = time.time()

        for sub in self._subreddits:
            url = f"{_REDDIT_BASE}/r/{sub}/hot.json"
            params = {"limit": str(self._limit)}
            headers = {"User-Agent": "globalintel/1.0"}
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
            except Exception as exc:
                logger.debug("Reddit r/%s failed: %s", sub, exc)
                continue

            for child in data.get("data", {}).get("children", []):
                post = child.get("data", {})
                score = post.get("score", 0)
                ratio = post.get("upvote_ratio", 0.5)
                created = post.get("created_utc", now)
                age = max(1, now - created)
                recency = math.exp(-age / 21600)
                velocity = math.log1p(score) * ratio * recency * 100

                items.append(IntelItem(
                    id=f"reddit:{post.get('id', '')}",
                    source="reddit",
                    domain="social",
                    title=post.get("title", "")[:300],
                    url=f"https://reddit.com{post.get('permalink', '')}",
                    ts=str(int(created)),
                    tags=["reddit", sub],
                    extra={
                        "subreddit": sub,
                        "score": score,
                        "upvote_ratio": ratio,
                        "num_comments": post.get("num_comments", 0),
                        "velocity": round(velocity, 2),
                        "velocity_score": round(velocity, 2),
                        "author": post.get("author", ""),
                    },
                ))

        items.sort(key=lambda x: -x.extra.get("velocity", 0))
        return items[:30]
