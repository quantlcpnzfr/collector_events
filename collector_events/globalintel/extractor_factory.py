# globalintel/extractor_factory.py
"""
ExtractorFactory — lazy instantiation of all global intelligence extractors.

Isolates 40+ extractor imports from the orchestrator so that a single broken
extractor (missing dependency, bad import) does not prevent the rest of the
service from starting.

Usage::

    factory = ExtractorFactory(config)
    schedule = factory.build_schedule()          # list[ScheduleEntry]
    schedule = factory.build_schedule(sources={"rss_feed", "acled"})  # filtered
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from forex_shared.logging.get_logger import get_logger

if TYPE_CHECKING:
    from collector_events.globalintel.base import BaseExtractor
    from collector_events.globalintel.config_env import GlobalIntelConfig

logger = get_logger(__name__)


@dataclass
class ScheduleEntry:
    """Maps an extractor to its run interval."""
    extractor: BaseExtractor
    interval_seconds: int
    last_run: float = 0.0


# ── OrchestratorConfig (legacy compat) ───────────────────────────────────

@dataclass
class OrchestratorConfig:
    """Legacy configuration — prefer GlobalIntelConfig for new code."""
    redis_url: str = ""
    acled_email: str = ""
    acled_key: str = ""
    fred_api_key: str = ""
    otx_key: str = ""
    abuseipdb_key: str = ""
    nasa_firms_key: str = ""
    telegram_relay_url: str = ""
    telegram_secret: str = ""
    finnhub_api_key: str = ""
    bls_api_key: str = ""
    eia_api_key: str = ""
    wto_api_key: str = ""
    acled_access_token: str = ""
    acled_password: str = ""
    wingbits_api_key: str = ""

    @classmethod
    def from_env(cls) -> OrchestratorConfig:
        from forex_shared.config import IntelConfig, StorageConfig
        return cls(
            redis_url=StorageConfig.REDIS_URL,
            acled_email=IntelConfig.ACLED_EMAIL,
            acled_key=IntelConfig.ACLED_API_KEY,
            acled_access_token=IntelConfig.ACLED_ACCESS_TOKEN,
            acled_password=IntelConfig.ACLED_PASSWORD,
            fred_api_key=IntelConfig.FRED_API_KEY,
            otx_key=IntelConfig.OTX_API_KEY,
            abuseipdb_key=IntelConfig.ABUSEIPDB_API_KEY,
            nasa_firms_key=IntelConfig.NASA_FIRMS_KEY,
            telegram_relay_url=IntelConfig.TELEGRAM_RELAY_URL,
            telegram_secret=IntelConfig.TELEGRAM_RELAY_SECRET,
            finnhub_api_key=IntelConfig.FINNHUB_API_KEY,
            bls_api_key=IntelConfig.BLS_API_KEY,
            eia_api_key=IntelConfig.EIA_API_KEY,
            wto_api_key=IntelConfig.WTO_API_KEY,
            wingbits_api_key=IntelConfig.WINGBITS_API_KEY,
        )


# ── Safe import helper ───────────────────────────────────────────────────

def _safe_import(module_path: str, class_name: str):
    """Import a single extractor class, returning None on failure."""
    try:
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)
    except Exception as exc:
        logger.warning(
            "ExtractorFactory: failed to import %s.%s — %s (skipped)",
            module_path, class_name, exc,
        )
        return None


# ── Extractor registry ───────────────────────────────────────────────────

# Each tuple: (module_path, class_name)
_EXTRACTOR_CLASSES: list[tuple[str, str]] = [
    # Feeds
    ("collector_events.globalintel.feeds.rss_extractor", "RSSFeedExtractor"),
    # Conflict
    ("collector_events.globalintel.conflict", "ACLEDExtractor"),
    ("collector_events.globalintel.conflict", "GDELTIntelExtractor"),
    ("collector_events.globalintel.conflict", "GDELTUnrestExtractor"),
    ("collector_events.globalintel.conflict.ucdp", "UCDPExtractor"),
    ("collector_events.globalintel.conflict.unrest", "UnrestMergeExtractor"),
    ("collector_events.globalintel.conflict.displacement", "DisplacementExtractor"),
    ("collector_events.globalintel.conflict.displacement", "GPSJammingExtractor"),
    # Cyber
    ("collector_events.globalintel.cyber", "CyberThreatExtractor"),
    # Economic
    ("collector_events.globalintel.economic", "EconomicCalendarExtractor"),
    ("collector_events.globalintel.economic.bis", "BisPolicyRateExtractor"),
    ("collector_events.globalintel.economic.bis", "BisExchangeRateExtractor"),
    ("collector_events.globalintel.economic.bis", "BisCreditExtractor"),
    ("collector_events.globalintel.economic.ecb", "EcbFxRateExtractor"),
    ("collector_events.globalintel.economic.ecb", "EcbYieldCurveExtractor"),
    ("collector_events.globalintel.economic.ecb", "EcbStressIndexExtractor"),
    ("collector_events.globalintel.economic.bls", "BlsSeriesExtractor"),
    ("collector_events.globalintel.economic.world_bank", "WorldBankExtractor"),
    ("collector_events.globalintel.economic.energy", "EIACrudeInventoryExtractor"),
    ("collector_events.globalintel.economic.energy", "EIANatGasStorageExtractor"),
    ("collector_events.globalintel.economic.energy", "EuGasStorageExtractor"),
    ("collector_events.globalintel.economic.prices", "FaoFoodPriceExtractor"),
    ("collector_events.globalintel.economic.prices", "EconomicStressExtractor"),
    # Market
    ("collector_events.globalintel.market", "FXRateExtractor"),
    ("collector_events.globalintel.market", "FearGreedExtractor"),
    ("collector_events.globalintel.market", "PredictionMarketExtractor"),
    ("collector_events.globalintel.market.stocks", "StockQuoteExtractor"),
    ("collector_events.globalintel.market.stocks", "SectorPerformanceExtractor"),
    ("collector_events.globalintel.market.stocks", "EarningsCalendarExtractor"),
    ("collector_events.globalintel.market.crypto", "CryptoQuoteExtractor"),
    ("collector_events.globalintel.market.crypto", "DefiTokenExtractor"),
    ("collector_events.globalintel.market.crypto", "AiTokenExtractor"),
    ("collector_events.globalintel.market.crypto", "OtherTokenExtractor"),
    ("collector_events.globalintel.market.crypto", "StablecoinExtractor"),
    ("collector_events.globalintel.market.crypto", "CryptoSectorExtractor"),
    ("collector_events.globalintel.market.commodities", "CommodityQuoteExtractor"),
    ("collector_events.globalintel.market.etf", "BtcEtfFlowExtractor"),
    ("collector_events.globalintel.market.gulf", "GulfQuoteExtractor"),
    ("collector_events.globalintel.market.cot", "CotPositioningExtractor"),
    ("collector_events.globalintel.market.country_index", "CountryStockIndexExtractor"),
    # Environment
    ("collector_events.globalintel.environment", "USGSEarthquakeExtractor"),
    ("collector_events.globalintel.environment", "NASAFireExtractor"),
    ("collector_events.globalintel.environment", "GDACSExtractor"),
    ("collector_events.globalintel.environment", "NASAEONETExtractor"),
    # Sanctions
    ("collector_events.globalintel.sanctions", "OFACSanctionsExtractor"),
    # Social
    ("collector_events.globalintel.social", "RedditVelocityExtractor"),
    ("collector_events.globalintel.social", "TelegramRelayExtractor"),
    # Advisories
    ("collector_events.globalintel.advisories", "AdvisoryExtractor"),
    # Trade
    ("collector_events.globalintel.trade", "WtoTradeRestrictionExtractor"),
    ("collector_events.globalintel.trade", "TariffTrendExtractor"),
    ("collector_events.globalintel.trade", "ComtradeFlowExtractor"),
    # Supply Chain
    ("collector_events.globalintel.supply_chain", "ChokepointStatusExtractor"),
    ("collector_events.globalintel.supply_chain", "CriticalMineralsExtractor"),
    ("collector_events.globalintel.supply_chain", "ShippingRateExtractor"),
    ("collector_events.globalintel.supply_chain", "ShippingStressExtractor"),
]


class ExtractorFactory:
    """Lazily imports and instantiates extractors with per-class error isolation.

    A single broken extractor (missing dependency, bad import) is logged and
    skipped — the rest of the service continues normally.
    """

    def __init__(self, config: GlobalIntelConfig | OrchestratorConfig | None = None) -> None:
        if config is None:
            from collector_events.globalintel.config_env import GlobalIntelConfig as _GIC
            config = _GIC()
        self._config = config

    # ── Config helpers ───────────────────────────────────────────────

    def _attr(self, new_name: str, old_name: str, default: str = "") -> str:
        from collector_events.globalintel.config_env import GlobalIntelConfig
        cfg = self._config
        if isinstance(cfg, GlobalIntelConfig):
            return getattr(cfg, new_name, default)
        return getattr(cfg, old_name, default)

    def _read_keys(self) -> dict[str, str]:
        return {
            "acled_email":        self._attr("ACLED_EMAIL",           "acled_email"),
            "acled_key":          self._attr("ACLED_API_KEY",         "acled_key"),
            "acled_access_token": self._attr("ACLED_ACCESS_TOKEN",    "acled_access_token"),
            "acled_password":     self._attr("ACLED_PASSWORD",        "acled_password"),
            "wingbits_api_key":   self._attr("WINGBITS_API_KEY",      "wingbits_api_key"),
            "otx_key":            self._attr("OTX_API_KEY",           "otx_key"),
            "abuseipdb_key":      self._attr("ABUSEIPDB_API_KEY",     "abuseipdb_key"),
            "fred_api_key":       self._attr("FRED_API_KEY",          "fred_api_key"),
            "bls_api_key":        self._attr("BLS_API_KEY",           "bls_api_key"),
            "eia_api_key":        self._attr("EIA_API_KEY",           "eia_api_key"),
            "wto_api_key":        self._attr("WTO_API_KEY",           "wto_api_key"),
            "nasa_firms_key":     self._attr("NASA_FIRMS_KEY",        "nasa_firms_key"),
            "finnhub_api_key":    self._attr("FINNHUB_API_KEY",       "finnhub_api_key"),
            "telegram_relay_url": self._attr("TELEGRAM_RELAY_URL",    "telegram_relay_url"),
            "telegram_secret":    self._attr("TELEGRAM_RELAY_SECRET", "telegram_secret"),
        }

    # ── Instantiation ────────────────────────────────────────────────

    def _instantiate(self, class_name: str, cls, keys: dict[str, str]) -> BaseExtractor | None:
        """Instantiate a single extractor class with the correct constructor args."""
        try:
            # Map class names to their required constructor kwargs
            ctor_args = _CTOR_ARGS.get(class_name)
            if ctor_args is not None:
                kwargs = {k: keys[v] for k, v in ctor_args.items()}
                return cls(**kwargs)
            return cls()
        except Exception as exc:
            logger.warning(
                "ExtractorFactory: failed to instantiate %s — %s (skipped)",
                class_name, exc,
            )
            return None

    # ── Public API ───────────────────────────────────────────────────

    def build_schedule(
        self,
        sources: set[str] | None = None,
    ) -> list[ScheduleEntry]:
        """Build the full extractor schedule.

        Each extractor is imported and instantiated individually — failures
        are logged and skipped.

        Args:
            sources: if provided, only include extractors whose SOURCE matches.
        """
        keys = self._read_keys()
        schedule: list[ScheduleEntry] = []
        loaded = 0
        skipped = 0

        for module_path, class_name in _EXTRACTOR_CLASSES:
            cls = _safe_import(module_path, class_name)
            if cls is None:
                skipped += 1
                continue

            ext = self._instantiate(class_name, cls, keys)
            if ext is None:
                skipped += 1
                continue

            interval = _INTERVALS.get(class_name, 3600)
            schedule.append(ScheduleEntry(extractor=ext, interval_seconds=interval))
            loaded += 1

        if sources is not None:
            schedule = [e for e in schedule if e.extractor.SOURCE in sources]

        logger.info(
            "ExtractorFactory: loaded %d extractors, skipped %d",
            loaded, skipped,
        )
        return schedule


# ── Constructor argument mapping ─────────────────────────────────────────
# class_name → {ctor_param: keys_dict_key}
# Classes not listed here are instantiated with no arguments.

_CTOR_ARGS: dict[str, dict[str, str]] = {
    "ACLEDExtractor": {
        "email": "acled_email", "api_key": "acled_key",
        "access_token": "acled_access_token", "password": "acled_password",
    },
    "UnrestMergeExtractor": {
        "acled_email": "acled_email", "acled_api_key": "acled_key",
        "acled_access_token": "acled_access_token",
    },
    "GPSJammingExtractor": {"api_key": "wingbits_api_key"},
    "CyberThreatExtractor": {"otx_key": "otx_key", "abuseipdb_key": "abuseipdb_key"},
    "EconomicCalendarExtractor": {"fred_api_key": "fred_api_key"},
    "BlsSeriesExtractor": {"api_key": "bls_api_key"},
    "EIACrudeInventoryExtractor": {"eia_api_key": "eia_api_key"},
    "EIANatGasStorageExtractor": {"eia_api_key": "eia_api_key"},
    "EconomicStressExtractor": {"fred_api_key": "fred_api_key"},
    "StockQuoteExtractor": {"finnhub_api_key": "finnhub_api_key"},
    "EarningsCalendarExtractor": {"finnhub_api_key": "finnhub_api_key"},
    "NASAFireExtractor": {"api_key": "nasa_firms_key"},
    "TelegramRelayExtractor": {
        "relay_url": "telegram_relay_url", "shared_secret": "telegram_secret",
    },
    "WtoTradeRestrictionExtractor": {"wto_api_key": "wto_api_key"},
    "TariffTrendExtractor": {"wto_api_key": "wto_api_key"},
}


# ── Interval mapping (seconds) ──────────────────────────────────────────
# class_name → interval in seconds.  Default: 3600 (1h).

_INTERVALS: dict[str, int] = {
    # Feeds
    "RSSFeedExtractor": 900,
    # Conflict
    "ACLEDExtractor": 900,
    "GDELTIntelExtractor": 86400,
    "GDELTUnrestExtractor": 16200,
    "UCDPExtractor": 86400,
    "UnrestMergeExtractor": 16200,
    "DisplacementExtractor": 86400,
    "GPSJammingExtractor": 172800,
    # Cyber
    "CyberThreatExtractor": 10800,
    # Economic
    "EconomicCalendarExtractor": 129600,
    "BisPolicyRateExtractor": 604800,
    "BisExchangeRateExtractor": 604800,
    "BisCreditExtractor": 604800,
    "EcbFxRateExtractor": 86400,
    "EcbYieldCurveExtractor": 86400,
    "EcbStressIndexExtractor": 86400,
    "BlsSeriesExtractor": 604800,
    "WorldBankExtractor": 604800,
    "EIACrudeInventoryExtractor": 604800,
    "EIANatGasStorageExtractor": 604800,
    "EuGasStorageExtractor": 86400,
    "FaoFoodPriceExtractor": 604800,
    "EconomicStressExtractor": 21600,
    # Market
    "FXRateExtractor": 90000,
    "FearGreedExtractor": 64800,
    "PredictionMarketExtractor": 10800,
    "StockQuoteExtractor": 3600,
    "SectorPerformanceExtractor": 3600,
    "EarningsCalendarExtractor": 86400,
    "CryptoQuoteExtractor": 3600,
    "DefiTokenExtractor": 3600,
    "AiTokenExtractor": 3600,
    "OtherTokenExtractor": 3600,
    "StablecoinExtractor": 3600,
    "CryptoSectorExtractor": 7200,
    "CommodityQuoteExtractor": 3600,
    "BtcEtfFlowExtractor": 3600,
    "GulfQuoteExtractor": 3600,
    "CotPositioningExtractor": 604800,
    "CountryStockIndexExtractor": 43200,
    # Environment
    "USGSEarthquakeExtractor": 3600,
    "NASAFireExtractor": 7200,
    "GDACSExtractor": 7200,
    "NASAEONETExtractor": 14400,
    # Sanctions
    "OFACSanctionsExtractor": 54000,
    # Social
    "RedditVelocityExtractor": 600,
    "TelegramRelayExtractor": 60,
    # Advisories
    "AdvisoryExtractor": 10800,
    # Trade
    "WtoTradeRestrictionExtractor": 604800,
    "TariffTrendExtractor": 604800,
    "ComtradeFlowExtractor": 604800,
    # Supply Chain
    "ChokepointStatusExtractor": 43200,
    "CriticalMineralsExtractor": 604800,
    "ShippingRateExtractor": 86400,
    "ShippingStressExtractor": 86400,
}
