# globalintel — Real-time global intelligence collection
#
# Python equivalent of worldmonitor's data collection layer,
# adapted for the forex_system architecture (Redis cache, MQ publication).
#
# Domains:
#   feeds/        — RSS/Atom news aggregation (100+ feeds)
#   conflict/     — ACLED, UCDP, GDELT conflict & unrest events
#   cyber/        — Threat intelligence (Feodo, URLhaus, OTX, AbuseIPDB)
#   economic/     — Economic calendar, BIS, ECB, BLS, World Bank, energy, prices
#   market/       — FX, stocks, crypto, commodities, ETFs, Gulf, COT, fear & greed
#   environment/  — Earthquakes (USGS), fires (NASA FIRMS), disasters (GDACS)
#   sanctions/    — OFAC SDN & Consolidated Lists
#   social/       — Reddit social velocity, Telegram OSINT relay
#   advisories/   — Government travel & health advisories
#   trade/        — WTO restrictions, tariff trends, Comtrade flows
#   supply_chain/ — Shipping rates, chokepoints, critical minerals
#   reference/    — Shared config data (symbols, tickers, countries)

from collector_events.globalintel.base import BaseExtractor
from collector_events.globalintel.cache import IntelCache
from collector_events.globalintel.global_tag_manager import GlobalTagManager
from collector_events.globalintel.intel_store import IntelMongoStore
from collector_events.globalintel.orchestrator import (
    IntelOrchestrator,
    OrchestratorConfig,
    ScheduleEntry,
)

__all__ = [
    "BaseExtractor",
    "GlobalTagManager",
    "IntelCache",
    "IntelMongoStore",
    "IntelOrchestrator",
    "OrchestratorConfig",
    "ScheduleEntry",
]
