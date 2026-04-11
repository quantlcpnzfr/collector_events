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
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiohttp
import redis.asyncio as aioredis
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .base import BaseExtractor, ExtractionResult, CHROME_UA, DEFAULT_TIMEOUT
from .cache import IntelCache
from .config_env import GlobalIntelConfig
from .global_tag_manager import GlobalTagManager
from forex_shared.domain.intel import IntelDomain
from forex_shared.logging.get_logger import get_logger
from forex_shared.logging.loggable import Loggable
from forex_shared.providers.mq.mq_factory import MQFactory
from forex_shared.providers.mq.mq_provider_async import MQProviderAsync
from forex_shared.providers.mq.topics import IntelTopics

# Domain imports
from .feeds.rss_extractor import RSSFeedExtractor
from .conflict import ACLEDExtractor, GDELTIntelExtractor, GDELTUnrestExtractor
from .conflict.ucdp import UCDPExtractor
from .conflict.unrest import UnrestMergeExtractor
from .conflict.displacement import DisplacementExtractor, GPSJammingExtractor
from .cyber import CyberThreatExtractor
from .economic import EconomicCalendarExtractor
from .economic.bis import BisPolicyRateExtractor, BisExchangeRateExtractor, BisCreditExtractor
from .economic.ecb import EcbFxRateExtractor, EcbYieldCurveExtractor, EcbStressIndexExtractor
from .economic.bls import BlsSeriesExtractor
from .economic.world_bank import WorldBankExtractor
from .economic.energy import EIACrudeInventoryExtractor, EIANatGasStorageExtractor, EuGasStorageExtractor
from .economic.prices import FaoFoodPriceExtractor, EconomicStressExtractor
from .market import FXRateExtractor, FearGreedExtractor, PredictionMarketExtractor
from .market.stocks import StockQuoteExtractor, SectorPerformanceExtractor, EarningsCalendarExtractor
from .market.crypto import (
    CryptoQuoteExtractor, DefiTokenExtractor, AiTokenExtractor,
    OtherTokenExtractor, StablecoinExtractor, CryptoSectorExtractor,
)
from .market.commodities import CommodityQuoteExtractor
from .market.etf import BtcEtfFlowExtractor
from .market.gulf import GulfQuoteExtractor
from .market.cot import CotPositioningExtractor
from .market.country_index import CountryStockIndexExtractor
from .environment import (
    USGSEarthquakeExtractor, NASAFireExtractor, GDACSExtractor, NASAEONETExtractor,
)
from .sanctions import OFACSanctionsExtractor
from .social import RedditVelocityExtractor, TelegramRelayExtractor
from .advisories import AdvisoryExtractor
from .trade import WtoTradeRestrictionExtractor, ComtradeFlowExtractor, TariffTrendExtractor
from .supply_chain import (
    ChokepointStatusExtractor, CriticalMineralsExtractor,
    ShippingRateExtractor, ShippingStressExtractor,
)

logger = get_logger(__name__)


@dataclass
class ScheduleEntry:
    """Maps an extractor to its run interval."""
    extractor: BaseExtractor
    interval_seconds: int
    last_run: float = 0.0


@dataclass
class OrchestratorConfig:
    """Configuration assembled from environment variables."""
    redis_url: str = ""
    acled_email: str = ""
    acled_key: str = ""
    fred_api_key: str = ""
    otx_key: str = ""
    abuseipdb_key: str = ""
    nasa_firms_key: str = ""
    telegram_relay_url: str = ""
    telegram_secret: str = ""
    # Finance-variant keys
    finnhub_api_key: str = ""
    bls_api_key: str = ""
    eia_api_key: str = ""
    wto_api_key: str = ""
    # Conflict-variant keys
    acled_access_token: str = ""
    acled_password: str = ""
    wingbits_api_key: str = ""

    @classmethod
    def from_env(cls) -> OrchestratorConfig:
        return cls(
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            acled_email=os.getenv("ACLED_EMAIL", ""),
            acled_key=os.getenv("ACLED_API_KEY", ""),
            acled_access_token=os.getenv("ACLED_ACCESS_TOKEN", ""),
            acled_password=os.getenv("ACLED_PASSWORD", ""),
            fred_api_key=os.getenv("FRED_API_KEY", ""),
            otx_key=os.getenv("OTX_API_KEY", ""),
            abuseipdb_key=os.getenv("ABUSEIPDB_API_KEY", ""),
            nasa_firms_key=os.getenv("NASA_FIRMS_KEY", ""),
            telegram_relay_url=os.getenv("TELEGRAM_RELAY_URL", ""),
            telegram_secret=os.getenv("TELEGRAM_RELAY_SECRET", ""),
            finnhub_api_key=os.getenv("FINNHUB_API_KEY", ""),
            bls_api_key=os.getenv("BLS_API_KEY", ""),
            eia_api_key=os.getenv("EIA_API_KEY", ""),
            wto_api_key=os.getenv("WTO_API_KEY", ""),
            wingbits_api_key=os.getenv("WINGBITS_API_KEY", ""),
        )


class IntelOrchestrator(Loggable):
    """Central orchestrator for all global intelligence extractors.

    Instantiates all extractors, runs them on a schedule via APScheduler,
    and stores results to Redis via IntelCache.

    Uses ``GlobalIntelConfig`` (hot-reloadable via ``EnvConfigManager``) by
    default.  Accepts a legacy ``OrchestratorConfig`` for backward compatibility.
    """

    def __init__(
        self,
        config: GlobalIntelConfig | OrchestratorConfig | None = None,
        mq: MQProviderAsync | None = None,
    ) -> None:
        if config is None:
            config = GlobalIntelConfig()
        self._config = config
        self._mq: MQProviderAsync | None = mq
        self._cache: IntelCache | None = None
        self._tag_manager: GlobalTagManager | None = None
        self._schedule: list[ScheduleEntry] = []
        self._scheduler: AsyncIOScheduler = AsyncIOScheduler()
        self._build_schedule()

    # ── Config helpers ───────────────────────────────────────────────

    def _attr(self, new_name: str, old_name: str, default: str = "") -> str:
        """Read an attribute from either config type by name."""
        cfg = self._config
        if isinstance(cfg, GlobalIntelConfig):
            return getattr(cfg, new_name, default)
        return getattr(cfg, old_name, default)

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
        redis_url = self._get_redis_url()
        redis_client = aioredis.Redis.from_url(redis_url, decode_responses=True)
        self._tag_manager = GlobalTagManager(redis_client=redis_client, mq=self._mq)
        self.log.info("IntelOrchestrator MQ connected; GlobalTagManager ready")

    async def disconnect_mq(self) -> None:
        """Gracefully disconnect MQ provider."""
        if self._mq is not None:
            await self._mq.disconnect()
            self._mq = None
            self._tag_manager = None
            self.log.info("IntelOrchestrator MQ disconnected")

    def _build_schedule(self) -> None:
        # Normalise attribute access across GlobalIntelConfig and legacy OrchestratorConfig
        acled_email        = self._attr("ACLED_EMAIL",           "acled_email")
        acled_key          = self._attr("ACLED_API_KEY",         "acled_key")
        acled_access_token = self._attr("ACLED_ACCESS_TOKEN",    "acled_access_token")
        acled_password     = self._attr("ACLED_PASSWORD",        "acled_password")
        wingbits_api_key   = self._attr("WINGBITS_API_KEY",      "wingbits_api_key")
        otx_key            = self._attr("OTX_API_KEY",           "otx_key")
        abuseipdb_key      = self._attr("ABUSEIPDB_API_KEY",     "abuseipdb_key")
        fred_api_key       = self._attr("FRED_API_KEY",          "fred_api_key")
        bls_api_key        = self._attr("BLS_API_KEY",           "bls_api_key")
        eia_api_key        = self._attr("EIA_API_KEY",           "eia_api_key")
        wto_api_key        = self._attr("WTO_API_KEY",           "wto_api_key")
        nasa_firms_key     = self._attr("NASA_FIRMS_KEY",        "nasa_firms_key")
        finnhub_api_key    = self._attr("FINNHUB_API_KEY",       "finnhub_api_key")
        telegram_relay_url = self._attr("TELEGRAM_RELAY_URL",    "telegram_relay_url")
        telegram_secret    = self._attr("TELEGRAM_RELAY_SECRET", "telegram_secret")

        self._schedule = [
            # ── Feeds ──
            ScheduleEntry(RSSFeedExtractor(), interval_seconds=900),

            # ── Conflict/Geopolitics ──
            ScheduleEntry(ACLEDExtractor(
                email=acled_email, api_key=acled_key,
                access_token=acled_access_token, password=acled_password,
            ), interval_seconds=900),
            ScheduleEntry(GDELTIntelExtractor(), interval_seconds=86400),
            ScheduleEntry(GDELTUnrestExtractor(), interval_seconds=16200),
            ScheduleEntry(UCDPExtractor(), interval_seconds=86400),
            ScheduleEntry(UnrestMergeExtractor(
                acled_email=acled_email, acled_api_key=acled_key,
                acled_access_token=acled_access_token,
            ), interval_seconds=16200),
            ScheduleEntry(DisplacementExtractor(), interval_seconds=86400),
            ScheduleEntry(GPSJammingExtractor(api_key=wingbits_api_key), interval_seconds=172800),

            # ── Cyber ──
            ScheduleEntry(CyberThreatExtractor(otx_key=otx_key, abuseipdb_key=abuseipdb_key), interval_seconds=10800),

            # ── Economic (calendar) ──
            ScheduleEntry(EconomicCalendarExtractor(fred_api_key=fred_api_key), interval_seconds=129600),

            # ── Economic (BIS) ──
            ScheduleEntry(BisPolicyRateExtractor(), interval_seconds=604800),    # weekly
            ScheduleEntry(BisExchangeRateExtractor(), interval_seconds=604800),
            ScheduleEntry(BisCreditExtractor(), interval_seconds=604800),

            # ── Economic (ECB) ──
            ScheduleEntry(EcbFxRateExtractor(), interval_seconds=86400),         # daily
            ScheduleEntry(EcbYieldCurveExtractor(), interval_seconds=86400),
            ScheduleEntry(EcbStressIndexExtractor(), interval_seconds=86400),

            # ── Economic (BLS) ──
            ScheduleEntry(BlsSeriesExtractor(api_key=bls_api_key), interval_seconds=604800),

            # ── Economic (World Bank) ──
            ScheduleEntry(WorldBankExtractor(), interval_seconds=604800),

            # ── Economic (Energy) ──
            ScheduleEntry(EIACrudeInventoryExtractor(eia_api_key=eia_api_key), interval_seconds=604800),
            ScheduleEntry(EIANatGasStorageExtractor(eia_api_key=eia_api_key), interval_seconds=604800),
            ScheduleEntry(EuGasStorageExtractor(), interval_seconds=86400),

            # ── Economic (Prices & Stress) ──
            ScheduleEntry(FaoFoodPriceExtractor(), interval_seconds=604800),
            ScheduleEntry(EconomicStressExtractor(fred_api_key=fred_api_key), interval_seconds=21600),

            # ── Market (FX, Fear/Greed, Prediction) ──
            ScheduleEntry(FXRateExtractor(), interval_seconds=90000),
            ScheduleEntry(FearGreedExtractor(), interval_seconds=64800),
            ScheduleEntry(PredictionMarketExtractor(), interval_seconds=10800),

            # ── Market (Stocks) ──
            ScheduleEntry(StockQuoteExtractor(finnhub_api_key=finnhub_api_key), interval_seconds=3600),
            ScheduleEntry(SectorPerformanceExtractor(), interval_seconds=3600),
            ScheduleEntry(EarningsCalendarExtractor(finnhub_api_key=finnhub_api_key), interval_seconds=86400),

            # ── Market (Crypto) ──
            ScheduleEntry(CryptoQuoteExtractor(), interval_seconds=3600),
            ScheduleEntry(DefiTokenExtractor(), interval_seconds=3600),
            ScheduleEntry(AiTokenExtractor(), interval_seconds=3600),
            ScheduleEntry(OtherTokenExtractor(), interval_seconds=3600),
            ScheduleEntry(StablecoinExtractor(), interval_seconds=3600),
            ScheduleEntry(CryptoSectorExtractor(), interval_seconds=7200),

            # ── Market (Commodities, ETFs, Gulf, COT) ──
            ScheduleEntry(CommodityQuoteExtractor(), interval_seconds=3600),
            ScheduleEntry(BtcEtfFlowExtractor(), interval_seconds=3600),
            ScheduleEntry(GulfQuoteExtractor(), interval_seconds=3600),
            ScheduleEntry(CotPositioningExtractor(), interval_seconds=604800),   # weekly
            ScheduleEntry(CountryStockIndexExtractor(), interval_seconds=43200),  # 12h

            # ── Environment ──
            ScheduleEntry(USGSEarthquakeExtractor(), interval_seconds=3600),
            ScheduleEntry(NASAFireExtractor(api_key=nasa_firms_key), interval_seconds=7200),
            ScheduleEntry(GDACSExtractor(), interval_seconds=7200),
            ScheduleEntry(NASAEONETExtractor(), interval_seconds=14400),

            # ── Sanctions ──
            ScheduleEntry(OFACSanctionsExtractor(), interval_seconds=54000),

            # ── Social ──
            ScheduleEntry(RedditVelocityExtractor(), interval_seconds=600),
            ScheduleEntry(TelegramRelayExtractor(
                relay_url=telegram_relay_url,
                shared_secret=telegram_secret,
            ), interval_seconds=60),

            # ── Advisories ──
            ScheduleEntry(AdvisoryExtractor(), interval_seconds=10800),

            # ── Trade ──
            ScheduleEntry(WtoTradeRestrictionExtractor(wto_api_key=wto_api_key), interval_seconds=604800),
            ScheduleEntry(TariffTrendExtractor(wto_api_key=wto_api_key), interval_seconds=604800),
            ScheduleEntry(ComtradeFlowExtractor(), interval_seconds=604800),

            # ── Supply Chain ──
            ScheduleEntry(ChokepointStatusExtractor(), interval_seconds=43200),
            ScheduleEntry(CriticalMineralsExtractor(), interval_seconds=604800),
            ScheduleEntry(ShippingRateExtractor(), interval_seconds=86400),
            ScheduleEntry(ShippingStressExtractor(), interval_seconds=86400),
        ]

    def _get_cache(self) -> IntelCache:
        if self._cache is None:
            client = aioredis.Redis.from_url(self._get_redis_url(), decode_responses=True)
            self._cache = IntelCache(client)
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
            cache = self._get_cache()
            await cache.store(result, ext.REDIS_KEY, ext.TTL_SECONDS)
            self.log.info(
                "  ✓ %s: %d items in %.0fms → Redis %s (TTL %ds)",
                ext.SOURCE, len(result.items), result.elapsed_ms,
                ext.REDIS_KEY, ext.TTL_SECONDS,
            )
        else:
            self.log.warning("  ✗ %s: %s", ext.SOURCE, result.error)
        entry.last_run = datetime.now(timezone.utc).timestamp()

        # Publish items to MQ and process GlobalTags (F3.2 + F3.4)
        if result.ok and self._mq is not None:
            for item in result.items:
                await self._mq.publish(IntelTopics.events(item.domain), item.to_mq_payload())
            if self._tag_manager is not None:
                await self._tag_manager.process_result(result)

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
                next_run_time=datetime.now(timezone.utc),  # run immediately on start
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
        cache = self._get_cache()
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
