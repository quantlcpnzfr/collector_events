"""Base extractor contract and shared utilities for global intelligence collection."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# ─── Shared constants ───────────────────────────────────────────────

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30)


# ─── Data models ────────────────────────────────────────────────────

@dataclass
class IntelItem:
    """Normalised intelligence item — the universal output of every extractor."""

    id: str
    source: str           # extractor source name (e.g. "acled", "gdelt", "feodo")
    domain: str           # top-level domain (e.g. "conflict", "cyber", "market")
    title: str
    url: str = ""
    body: str = ""
    ts: str = ""          # ISO-8601 timestamp of the upstream event
    fetched_at: str = ""  # ISO-8601 timestamp when we fetched it
    lat: float | None = None
    lon: float | None = None
    country: str = ""
    severity: str = ""    # "HIGH" | "MEDIUM" | "LOW" | ""
    tags: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = {
            "id": self.id,
            "source": self.source,
            "domain": self.domain,
            "title": self.title,
            "url": self.url,
            "body": self.body,
            "ts": self.ts,
            "fetched_at": self.fetched_at,
            "country": self.country,
            "severity": self.severity,
            "tags": self.tags,
        }
        if self.lat is not None:
            d["lat"] = self.lat
        if self.lon is not None:
            d["lon"] = self.lon
        if self.extra:
            d["extra"] = self.extra
        return d


@dataclass
class ExtractionResult:
    """Wrapper returned by every extractor run."""

    source: str
    domain: str
    items: list[IntelItem] = field(default_factory=list)
    error: str | None = None
    elapsed_ms: float = 0.0
    fetched_at: str = ""

    @property
    def ok(self) -> bool:
        return self.error is None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "domain": self.domain,
            "count": len(self.items),
            "error": self.error,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "fetched_at": self.fetched_at,
            "items": [item.to_dict() for item in self.items],
        }


# ─── Base extractor ─────────────────────────────────────────────────

class BaseExtractor(ABC):
    """Abstract base class for all global intelligence extractors.

    Subclasses implement ``_fetch(session)`` which returns a list of
    ``IntelItem`` instances.  The base class provides retry logic,
    timing, and a shared ``aiohttp.ClientSession``.
    """

    # Override in subclass
    SOURCE: str = ""
    DOMAIN: str = ""
    REDIS_KEY: str = ""
    TTL_SECONDS: int = 3600

    def __init__(self, max_retries: int = 2, backoff_base: float = 2.0):
        self._max_retries = max_retries
        self._backoff_base = backoff_base

    @abstractmethod
    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        """Fetch and normalise data from the upstream source."""

    async def run(self, session: aiohttp.ClientSession | None = None) -> ExtractionResult:
        """Execute the extractor with retry and timing."""
        own_session = session is None
        if own_session:
            session = aiohttp.ClientSession(
                timeout=DEFAULT_TIMEOUT,
                headers={"User-Agent": CHROME_UA},
            )
        t0 = time.monotonic()
        now = datetime.now(timezone.utc).isoformat()
        try:
            items = await self._retry_fetch(session)
            for item in items:
                if not item.fetched_at:
                    item.fetched_at = now
            return ExtractionResult(
                source=self.SOURCE,
                domain=self.DOMAIN,
                items=items,
                elapsed_ms=(time.monotonic() - t0) * 1000,
                fetched_at=now,
            )
        except Exception as exc:
            logger.exception("Extractor %s failed: %s", self.SOURCE, exc)
            return ExtractionResult(
                source=self.SOURCE,
                domain=self.DOMAIN,
                error=str(exc),
                elapsed_ms=(time.monotonic() - t0) * 1000,
                fetched_at=now,
            )
        finally:
            if own_session:
                await session.close()

    async def _retry_fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                return await self._fetch(session)
            except Exception as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    wait = self._backoff_base ** attempt
                    logger.warning(
                        "%s attempt %d failed (%s), retrying in %.1fs",
                        self.SOURCE, attempt + 1, exc, wait,
                    )
                    await asyncio.sleep(wait)
        raise last_exc  # type: ignore[misc]
