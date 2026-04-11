"""Redis-backed cache for extracted intelligence data.

Mirrors worldmonitor's seed→Redis→API pattern:
each extractor writes its result to a Redis key with a domain-scoped TTL.
Uses ``RedisProvider`` from shared_lib for non-blocking I/O compatible with asyncio event loops.
"""

from __future__ import annotations

import json
from typing import Any

from forex_shared.logging.get_logger import get_logger
from forex_shared.providers.cache.redis_provider import RedisProvider
from .base import ExtractionResult

logger = get_logger(__name__)


class IntelCache:
    """Async wrapper around ``RedisProvider`` for intelligence storage.

    Key naming convention (matches worldmonitor):
        ``{domain}:{source}:{version}``
        e.g. ``conflict:acled:v1``, ``cyber:threats:v2``

    Each key stores a JSON blob of the ``ExtractionResult.to_dict()`` output
    with a configurable TTL.

    Additionally writes a health-check meta key:
        ``seed-meta:{domain}:{source}``
    """

    META_TTL = 7 * 24 * 3600  # 7 days — same as worldmonitor

    def __init__(self, redis_provider: RedisProvider):
        self._r = redis_provider

    # ── write ────────────────────────────────────────────────────

    async def store(self, result: ExtractionResult, key: str, ttl: int) -> None:
        """Persist an extraction result + metadata."""
        payload = result.to_dict()
        await self._r.set_json_raw(key, payload, ttl=ttl)
        meta_key = f"seed-meta:{key.replace(':', '-')}"
        meta = {
            "key": key,
            "count": len(result.items),
            "elapsed_ms": result.elapsed_ms,
            "fetched_at": result.fetched_at,
            "error": result.error,
        }
        await self._r.set_json_raw(meta_key, meta, ttl=self.META_TTL)
        logger.info(
            "Cached %d items → %s (TTL %ds)", len(result.items), key, ttl,
        )

    # ── read ─────────────────────────────────────────────────────

    async def load(self, key: str) -> dict[str, Any] | None:
        return await self._r.get_json_raw(key)

    async def health(self, key: str) -> dict[str, Any] | None:
        meta_key = f"seed-meta:{key.replace(':', '-')}"
        return await self._r.get_json_raw(meta_key)
