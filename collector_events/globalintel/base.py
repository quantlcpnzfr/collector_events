"""Base extractor contract and shared utilities for global intelligence collection."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import aiohttp

# ─── Cross-service contracts delegados ao shared_lib ───────────────
# IntelItem, ExtractionResult e ExtractionResult são definidos em
# forex_shared.domain.intel para permitir consumo por qualquer serviço
# sem depender de collector_events.
from forex_shared.domain.intel import ExtractionResult, IntelItem

# Re-exportados para backward compatibility: qualquer módulo que
# importava "from .base import IntelItem" continua funcionando.
__all__ = ["IntelItem", "ExtractionResult", "BaseExtractor", "CHROME_UA", "DEFAULT_TIMEOUT"]

logger = logging.getLogger(__name__)

# ─── Shared constants ───────────────────────────────────────────────

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30)


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
