"""Global intelligence orchestrator.

Runs all extractors on a schedule, stores results via IntelCache (Redis),
and provides a unified interface for the forex_system pipeline.

Usage:
    from collector_events.globalintel.orchestrator import IntelOrchestrator

    orch = IntelOrchestrator()                # uses GlobalIntelConfig from env
    await orch.run_all()                      # one-shot all extractors (shared session)
    await orch.run_domain("cyber")            # single domain (shared session)
    await orch.start_scheduler()              # background APScheduler loop
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from collector_events.globalintel.base import BaseExtractor, ExtractionResult, CHROME_UA, DEFAULT_TIMEOUT
from collector_events.globalintel.cache import IntelCache
from collector_events.globalintel.config_env import GlobalIntelConfig
from collector_events.globalintel.extractor_factory import (
    ExtractorFactory,
    OrchestratorConfig,
    ScheduleEntry,
)
from collector_events.globalintel.global_tag_manager import GlobalTagManager

from forex_shared.logging.get_logger import get_logger
from forex_shared.logging.loggable import Loggable
from forex_shared.providers.cache.redis_provider import RedisProvider
from forex_shared.providers.mq.mq_factory import MQFactory
from forex_shared.providers.mq.mq_provider_async import MQProviderAsync
from forex_shared.providers.mq.topics import IntelTopics

logger = get_logger(__name__)


class IntelOrchestrator(Loggable):
    """Central orchestrator for all global intelligence extractors.

    Instantiates all extractors via ``ExtractorFactory``, runs them on a
    schedule via APScheduler, and stores results to Redis via IntelCache.

    Uses ``GlobalIntelConfig`` (hot-reloadable via ``EnvConfigManager``) by
    default.  Accepts a legacy ``OrchestratorConfig`` for backward compatibility.
    """

    def __init__(
        self,
        config: GlobalIntelConfig | OrchestratorConfig | None = None,
        mq: MQProviderAsync | None = None,
        sources_filter: set[str] | None = None,
        on_result: Callable[[ExtractionResult], Awaitable[None]] | None = None,
    ) -> None:
        if config is None:
            config = GlobalIntelConfig()
        self._config = config
        self._mq: MQProviderAsync | None = mq
        self._redis: RedisProvider | None = None
        self._cache: IntelCache | None = None
        self._tag_manager: GlobalTagManager | None = None
        self._scheduler: AsyncIOScheduler = AsyncIOScheduler()
        self._sources_filter = sources_filter
        self._on_result = on_result

        factory = ExtractorFactory(config)
        self._schedule: list[ScheduleEntry] = factory.build_schedule(
            sources=sources_filter,
        )

    # ── Config helpers ───────────────────────────────────────────────

    def _get_redis_url(self) -> str:
        if isinstance(self._config, GlobalIntelConfig):
            return self._config.REDIS_URL
        return self._config.redis_url

    # ── MQ helpers ───────────────────────────────────────────────────

    async def connect_mq(self, mq: MQProviderAsync | None = None) -> None:
        """Connect MQ provider.  If ``mq`` is provided, uses it directly.

        Call this before ``start_scheduler()`` to enable MQ publishing.
        If not called, the orchestrator runs in cache-only mode (no MQ).
        """
        if mq is not None:
            self._mq = mq
        if self._mq is None:
            self._mq = MQFactory.create_async_from_env()
        await self._mq.connect()
        redis_provider = await self._get_redis_provider()
        self._tag_manager = GlobalTagManager(redis_provider=redis_provider, mq=self._mq)
        self.log.info("IntelOrchestrator MQ connected; GlobalTagManager ready")

    async def disconnect_mq(self) -> None:
        """Gracefully disconnect MQ provider."""
        if self._mq is not None:
            await self._mq.disconnect()
            self._mq = None
            self._tag_manager = None
            self.log.info("IntelOrchestrator MQ disconnected")

    async def disconnect_redis(self) -> None:
        """Gracefully disconnect Redis provider."""
        if self._redis is not None:
            await self._redis.disconnect()
            self._redis = None
            self._cache = None
            self.log.info("IntelOrchestrator Redis disconnected")

    async def _get_redis_provider(self) -> RedisProvider:
        if self._redis is None:
            self._redis = RedisProvider.from_env()
            await self._redis.connect()
        return self._redis

    async def _get_cache(self) -> IntelCache:
        if self._cache is None:
            redis_provider = await self._get_redis_provider()
            self._cache = IntelCache(redis_provider)
        return self._cache

    # ── One-shot methods ─────────────────────────────────────────────

    async def run_all(self) -> list[ExtractionResult]:
        """Run all extractors once with a shared aiohttp session."""
        results: list[ExtractionResult] = []
        async with aiohttp.ClientSession(
            timeout=DEFAULT_TIMEOUT,
            headers={"User-Agent": CHROME_UA},
        ) as session:
            for entry in self._schedule:
                result = await self._run_entry(entry, session=session)
                results.append(result)
        return results

    async def run_domain(self, domain: str) -> list[ExtractionResult]:
        """Run extractors for a single domain with a shared session."""
        results: list[ExtractionResult] = []
        async with aiohttp.ClientSession(
            timeout=DEFAULT_TIMEOUT,
            headers={"User-Agent": CHROME_UA},
        ) as session:
            for entry in self._schedule:
                if entry.extractor.DOMAIN == domain:
                    result = await self._run_entry(entry, session=session)
                    results.append(result)
        return results

    async def run_source(self, source: str) -> ExtractionResult | None:
        """Run a single extractor by SOURCE name."""
        for entry in self._schedule:
            if entry.extractor.SOURCE == source:
                return await self._run_entry(entry)
        return None

    async def _run_entry(
        self,
        entry: ScheduleEntry,
        session: aiohttp.ClientSession | None = None,
    ) -> ExtractionResult:
        ext = entry.extractor
        self.log.info("Running %s/%s ...", ext.DOMAIN, ext.SOURCE)
        result = await ext.run(session=session)
        if result.ok:
            cache = await self._get_cache()
            await cache.store(result, ext.REDIS_KEY, ext.TTL_SECONDS)
            self.log.info(
                "  ✓ %s: %d items in %.0fms → Redis %s (TTL %ds)",
                ext.SOURCE, len(result.items), result.elapsed_ms,
                ext.REDIS_KEY, ext.TTL_SECONDS,
            )
        else:
            self.log.warning("  ✗ %s: %s", ext.SOURCE, result.error)
        entry.last_run = datetime.now(timezone.utc).timestamp()

        # Publish items to MQ and process GlobalTags
        if result.ok and self._mq is not None:
            for item in result.items:
                await self._mq.publish(IntelTopics.events(item.domain), item.to_mq_payload())
            if self._tag_manager is not None:
                await self._tag_manager.process_result(result)

        # on_result callback (e.g. MongoDB persistence)
        if self._on_result is not None:
            asyncio.create_task(self._on_result(result))

        return result

    # ── Scheduler loop ───────────────────────────────────────────────

    async def start_scheduler(self) -> None:
        """Start the APScheduler background loop — one job per extractor."""
        for entry in self._schedule:
            self._scheduler.add_job(
                self._run_entry,
                trigger="interval",
                seconds=entry.interval_seconds,
                args=[entry],
                id=f"{entry.extractor.DOMAIN}_{entry.extractor.SOURCE}",
                next_run_time=datetime.now(timezone.utc),
                misfire_grace_time=60,
                coalesce=True,
            )
        self._scheduler.start()
        self.log.info(
            "IntelOrchestrator scheduler started with %d extractors",
            len(self._schedule),
        )

    def stop_scheduler(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    # ── Status / health ──────────────────────────────────────────────

    async def health(self) -> dict[str, Any]:
        """Return health status of all extractors (async — reads Redis)."""
        cache = await self._get_cache()
        status: dict[str, Any] = {}
        for entry in self._schedule:
            ext = entry.extractor
            key = f"{ext.DOMAIN}/{ext.SOURCE}"
            meta = await cache.health(ext.REDIS_KEY)
            status[key] = {
                "redis_key": ext.REDIS_KEY,
                "ttl_seconds": ext.TTL_SECONDS,
                "interval_seconds": entry.interval_seconds,
                "last_run": entry.last_run,
                "cache_meta": meta,
            }
        return status

    @property
    def extractors(self) -> list[BaseExtractor]:
        return [e.extractor for e in self._schedule]

    @property
    def domains(self) -> set[str]:
        return {e.extractor.DOMAIN for e in self._schedule}
