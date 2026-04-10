"""Redis-backed cache for extracted intelligence data.

Mirrors worldmonitor's seed→Redis→API pattern:
each extractor writes its result to a Redis key with a domain-scoped TTL.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import redis

from .base import ExtractionResult

logger = logging.getLogger(__name__)


class IntelCache:
    """Thin wrapper around a ``redis.Redis`` client for intelligence storage.

    Key naming convention (matches worldmonitor):
        ``{domain}:{source}:{version}``
        e.g. ``conflict:acled:v1``, ``cyber:threats:v2``

    Each key stores a JSON blob of the ``ExtractionResult.to_dict()`` output
    with a configurable TTL.

    Additionally writes a health-check meta key:
        ``seed-meta:{domain}:{source}``
    """

    META_TTL = 7 * 24 * 3600  # 7 days — same as worldmonitor

    def __init__(self, redis_client: redis.Redis):
        self._r = redis_client

    # ── write ────────────────────────────────────────────────────

    def store(self, result: ExtractionResult, key: str, ttl: int) -> None:
        """Persist an extraction result + metadata."""
        payload = json.dumps(result.to_dict(), default=str)
        self._r.setex(key, ttl, payload)
        meta_key = f"seed-meta:{key.replace(':', '-')}"
        meta = json.dumps({
            "key": key,
            "count": len(result.items),
            "elapsed_ms": result.elapsed_ms,
            "fetched_at": result.fetched_at,
            "error": result.error,
        })
        self._r.setex(meta_key, self.META_TTL, meta)
        logger.info(
            "Cached %d items → %s (TTL %ds)", len(result.items), key, ttl,
        )

    # ── read ─────────────────────────────────────────────────────

    def load(self, key: str) -> dict[str, Any] | None:
        raw = self._r.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def health(self, key: str) -> dict[str, Any] | None:
        meta_key = f"seed-meta:{key.replace(':', '-')}"
        raw = self._r.get(meta_key)
        if raw is None:
            return None
        return json.loads(raw)
