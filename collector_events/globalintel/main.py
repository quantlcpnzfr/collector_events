# collector_events/globalintel/main.py
"""
GlobalIntel daemon — continuous extraction + MongoDB persistence.

Verified-OK extractors (29 free, no paid API keys required) are grouped
by domain. On startup, an initial full sweep runs immediately. The
APScheduler then drives each extractor on its own interval.

Usage:
    python -m collector_events.globalintel.main
    python -m collector_events.globalintel.main --no-persist
    python -m collector_events.globalintel.main --only market
    python -m collector_events.globalintel.main --only environment --no-persist
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from forex_shared.logging.get_logger import get_logger, setup_logging
setup_logging()

import logging
for _noisy in ("aiohttp", "urllib3", "asyncio", "charset_normalizer", "apscheduler"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = get_logger(__name__)

from forex_shared.env_config_manager import EnvConfigManager
try:
    EnvConfigManager.startup()
except Exception:
    pass  # works without MongoDB config sync

from collector_events.globalintel.orchestrator import IntelOrchestrator
from collector_events.globalintel.intel_store import IntelMongoStore

# ─────────────────────────────────────────────────────────────────────────────
# Canonical list of 29 verified-OK extractors (no paid API keys needed).
# This is the single source of truth — imported by the integration test.
# ─────────────────────────────────────────────────────────────────────────────

OK_EXTRACTORS: dict[str, list[str]] = {
    "economic": [
        "bis_policy_rates",
        "bis_exchange_rates",
        "bis_credit",
        "ecb_fx_rates",
        "ecb_yield_curve",
        "ecb_fsi",
        "gie_gas",
        "bls",
        "economic_stress",
    ],
    "market": [
        "yahoo_fx",
        "fear_greed",
        "prediction_markets",
        "commodity_quotes",
        "btc_etf_flows",
        "gulf_quotes",
        "cot_positioning",
        "country_indices",
        "sector_performance",
        "crypto_quotes",
        "defi_tokens",
        "ai_tokens",
        "stablecoins",
        "stock_quotes",
    ],
    "environment": [
        "usgs",
        "eonet",
        "gdacs",
    ],
    "social": [
        "reddit",
    ],
    "advisories": [
        "advisories",
    ],
    "supply_chain": [
        "chokepoints",
        "critical_minerals",
        "shipping_rates",
    ],
    "feeds": [
        "rss_feeds",
    ],
}

ALL_OK_SOURCES: set[str] = {
    source for sources in OK_EXTRACTORS.values() for source in sources
}


# ─────────────────────────────────────────────────────────────────────────────
# Daemon
# ─────────────────────────────────────────────────────────────────────────────

async def main(persist: bool = True, only_domain: str = "") -> int:
    """Production daemon: initial sweep + continuous APScheduler loop."""
    sources_filter: set[str] | None = None
    if only_domain:
        sources_filter = set(OK_EXTRACTORS.get(only_domain, []))
        if not sources_filter:
            logger.error("Unknown domain '%s'. Available: %s", only_domain,
                         sorted(OK_EXTRACTORS))
            return 1
        logger.info("Domain filter active: %s (%d sources)",
                    only_domain, len(sources_filter))

    store: IntelMongoStore | None = None
    on_result = None
    if persist:
        store = IntelMongoStore()
        await store.ensure_indexes()
        on_result = store.store_result
        logger.info("MongoDB persistence enabled (intel_items / intel_runs)")
    else:
        logger.info("Persistence disabled (--no-persist)")

    orch = IntelOrchestrator(
        sources_filter=sources_filter,
        on_result=on_result,
    )

    logger.info("GlobalIntel daemon starting — %d extractors scheduled",
                len(orch._schedule))

    # ── Graceful shutdown ─────────────────────────────────────────────
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows does not support add_signal_handler for all signals
            pass

    # ── Initial full sweep ────────────────────────────────────────────
    logger.info("Running initial extraction sweep …")
    try:
        await orch.run_all()
    except Exception:
        logger.exception("Initial sweep failed (non-fatal — scheduler will retry)")

    # ── Continuous scheduler loop ─────────────────────────────────────
    logger.info("Starting continuous scheduler …")
    await orch.start_scheduler()

    try:
        await stop_event.wait()
    finally:
        logger.info("Stopping scheduler …")
        await orch.stop_scheduler()
        logger.info("GlobalIntel daemon stopped")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GlobalIntel collection daemon")
    parser.add_argument(
        "--persist", dest="persist", action="store_true", default=True,
        help="Persist extraction results to MongoDB (default: on)",
    )
    parser.add_argument(
        "--no-persist", dest="persist", action="store_false",
        help="Disable MongoDB persistence (dry-run mode)",
    )
    parser.add_argument(
        "--only", default="",
        metavar="DOMAIN",
        help="Run only extractors in a specific domain (e.g. 'market', 'environment')",
    )
    args = parser.parse_args()

    sys.exit(asyncio.run(main(persist=args.persist, only_domain=args.only)))
