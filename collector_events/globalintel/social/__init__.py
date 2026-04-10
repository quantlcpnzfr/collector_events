"""Social intelligence extractors: Reddit + Telegram relay.

Data sources (mirrors worldmonitor):

Reddit — public JSON API (no auth)
    URL: https://www.reddit.com/r/{subreddit}/hot.json?limit=25&raw_json=1
    Subreddits: worldnews, geopolitics
    Velocity scoring: log1p(score) × upvoteRatio × recencyFactor × 100
    Recency: exp(-ageSec / 21600) — 6h half-life

Telegram — OSINT relay (worldmonitor uses GramJS MTProto User API on Railway)
    Relay URL: {RELAY_BASE}/telegram/feed?limit=N&topic=T&channel=C
    Auth: shared secret header
    ~30 curated OSINT channels across 3 tiers
    Topics: conflict, breaking, geopolitics, middleeast, osint, cyber
    In-memory 200-item rolling buffer on relay, 60s poll cycle

NOTE: worldmonitor does NOT use Twitter/X, Instagram, Facebook, or Bluesky.
      Telegram is the only social media platform with direct extraction.
      Reddit is used as a secondary social velocity signal.
"""

from __future__ import annotations

import logging
import math
import time

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

# ─── Reddit ──────────────────────────────────────────────────────────

REDDIT_BASE = "https://www.reddit.com"
REDDIT_SUBREDDITS = ["worldnews", "geopolitics"]


class RedditVelocityExtractor(BaseExtractor):
    """Fetches trending posts from geopolitics-related subreddits.

    Velocity score = log1p(score) × upvoteRatio × recencyFactor × 100
    recencyFactor = exp(-age_seconds / 21600)  # 6h half-life

    Post-collection structure per item:
        id="reddit:{post_id}", source="reddit", domain="social",
        title, url, extra={subreddit, score, upvote_ratio, num_comments, velocity_score}
    """

    SOURCE = "reddit"
    DOMAIN = "social"
    REDIS_KEY = "intelligence:social:reddit:v1"
    TTL_SECONDS = 1800  # 30min

    def __init__(self, subreddits: list[str] | None = None, limit: int = 25):
        super().__init__()
        self._subreddits = subreddits or REDDIT_SUBREDDITS
        self._limit = limit

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        now = time.time()
        for sub in self._subreddits:
            url = f"{REDDIT_BASE}/r/{sub}/hot.json"
            params = {"limit": str(self._limit), "raw_json": "1"}
            headers = {"User-Agent": "globalintel/1.0 (forex_system)"}
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status != 200:
                        logger.debug("Reddit r/%s returned %d", sub, resp.status)
                        continue
                    data = await resp.json(content_type=None)
            except Exception as exc:
                logger.warning("Reddit r/%s failed: %s", sub, exc)
                continue

            for child in data.get("data", {}).get("children", []):
                post = child.get("data", {})
                score = post.get("score", 0)
                ratio = post.get("upvote_ratio", 0.5)
                created = post.get("created_utc", now)
                age_sec = max(0, now - created)
                recency = math.exp(-age_sec / 21600)
                velocity = math.log1p(score) * ratio * recency * 100

                items.append(IntelItem(
                    id=f"reddit:{post.get('id', '')}",
                    source="reddit",
                    domain="social",
                    title=post.get("title", "")[:300],
                    url=f"https://reddit.com{post.get('permalink', '')}",
                    ts=str(int(created)),
                    tags=[sub, "social_velocity"],
                    extra={
                        "subreddit": sub,
                        "score": score,
                        "upvote_ratio": ratio,
                        "num_comments": post.get("num_comments", 0),
                        "velocity_score": round(velocity, 2),
                    },
                ))
        # Sort by velocity descending, keep top 30
        items.sort(key=lambda x: x.extra.get("velocity_score", 0), reverse=True)
        return items[:30]


# ─── Telegram OSINT relay ────────────────────────────────────────────

# Channel definitions (mirrors worldmonitor data/telegram-channels.json)
TELEGRAM_CHANNELS = [
    # Tier 1
    {"handle": "VahsidOnline", "tier": 1, "topic": "geopolitics", "region": "iran"},
    # Tier 2
    {"handle": "AbuAliExpress", "tier": 2, "topic": "middleeast", "region": "middleeast"},
    {"handle": "AuroraIntel", "tier": 2, "topic": "osint", "region": "global"},
    {"handle": "BNaborNews", "tier": 2, "topic": "breaking", "region": "global"},
    {"handle": "Flash43", "tier": 2, "topic": "conflict", "region": "ukraine"},
    {"handle": "DeepStateUA", "tier": 2, "topic": "conflict", "region": "ukraine"},
    {"handle": "IranIntl_En", "tier": 2, "topic": "middleeast", "region": "iran"},
    {"handle": "liveuamap", "tier": 2, "topic": "conflict", "region": "ukraine"},
    {"handle": "OSINTdefender", "tier": 2, "topic": "osint", "region": "global"},
    {"handle": "ryaboronline", "tier": 2, "topic": "conflict", "region": "ukraine"},
    {"handle": "ShadowBreak", "tier": 2, "topic": "conflict", "region": "global"},
    {"handle": "WarMonitor_eng", "tier": 2, "topic": "conflict", "region": "ukraine"},
    # Tier 3
    {"handle": "belaborat", "tier": 3, "topic": "osint", "region": "global"},
    {"handle": "CyberDetective", "tier": 3, "topic": "cyber", "region": "global"},
    {"handle": "nexaborews", "tier": 3, "topic": "breaking", "region": "europe"},
    {"handle": "spectatorindex", "tier": 3, "topic": "geopolitics", "region": "global"},
    {"handle": "DDGeopolitics", "tier": 3, "topic": "geopolitics", "region": "global"},
    {"handle": "TheCradleMedia", "tier": 3, "topic": "middleeast", "region": "middleeast"},
]


class TelegramRelayExtractor(BaseExtractor):
    """Fetches OSINT messages from a Telegram relay (self-hosted).

    The relay must expose: GET {base_url}/telegram/feed?limit=N&topic=T
    with shared secret auth.

    Post-collection structure per item:
        id="telegram:{handle}:{msg_id}", source="telegram", domain="social",
        title=<message text>, url=<t.me link>,
        extra={channel, channel_title, topic, tags, tier, early_signal}
    """

    SOURCE = "telegram"
    DOMAIN = "social"
    REDIS_KEY = "social:telegram:v1"
    TTL_SECONDS = 300  # 5min — fast-moving data

    def __init__(self, relay_url: str = "", shared_secret: str = "", limit: int = 100):
        super().__init__()
        self._relay_url = relay_url
        self._shared_secret = shared_secret
        self._limit = limit

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._relay_url:
            logger.info("Telegram relay URL not configured — skipping")
            return []

        url = f"{self._relay_url}/telegram/feed"
        params = {"limit": str(self._limit)}
        headers = {
            "x-relay-key": self._shared_secret,
            "Authorization": f"Bearer {self._shared_secret}",
        }
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("Telegram relay fetch failed: %s", exc)
            return []

        messages = data if isinstance(data, list) else data.get("messages", [])
        items: list[IntelItem] = []
        for msg in messages:
            items.append(IntelItem(
                id=msg.get("id", ""),
                source="telegram",
                domain="social",
                title=msg.get("text", "")[:500],
                url=msg.get("url", ""),
                ts=msg.get("ts", ""),
                tags=msg.get("tags", []),
                extra={
                    "channel": msg.get("channel", ""),
                    "channel_title": msg.get("channelTitle", ""),
                    "topic": msg.get("topic", ""),
                    "early_signal": msg.get("earlySignal", False),
                },
            ))
        return items
