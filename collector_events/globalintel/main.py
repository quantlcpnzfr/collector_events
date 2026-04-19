# collector_events/globalintel/main.py
"""
GlobalIntel daemon — continuous extraction + optional persistence.

Verified-OK extractors (29 free, no paid API keys required) are grouped
by domain. On startup, an initial full sweep runs immediately. The
APScheduler then drives each extractor on its own interval.

Usage:
    python -m collector_events.globalintel.main
    python -m collector_events.globalintel.main --no-persist
    python -m collector_events.globalintel.main --only market
    python -m collector_events.globalintel.main --json ./intel_dump.jsonl
    python -m collector_events.globalintel.main --no-persist --json ./intel_dump.jsonl
    python -m collector_events.globalintel.main --only environment --json ./env.jsonl

JSON output format (--json):
    One JSON object per line (JSONL).  Each line is the output of
    ``ExtractionResult.to_dict()``:
        {
          "source": "usgs",
          "domain": "environment",
          "count": 12,
          "elapsed_ms": 843.1,
          "fetched_at": "2026-04-19T15:30:00Z",
          "items": [ { "id": "usgs:us7000...", "title": "...", ... }, ... ]
        }
    Append-safe: existing files are not overwritten.  Concurrent writes
    within the same process are serialised by an asyncio.Lock.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from pathlib import Path
from typing import Callable, Awaitable

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

from forex_shared.domain.intel import ExtractionResult
from collector_events.globalintel.orchestrator import IntelOrchestrator
from collector_events.globalintel.intel_store import IntelMongoStore


# ─────────────────────────────────────────────────────────────────────────────
# JSONL writer
# ─────────────────────────────────────────────────────────────────────────────

class JsonlWriter:
    """Appends one ExtractionResult per line to a JSONL file.

    Thread/task-safe: an asyncio.Lock serialises concurrent on_result calls.
    Opens the file in append mode — existing content is preserved.

    Usage::

        writer = JsonlWriter("intel_dump.jsonl")
        # wire as on_result callback:
        orch = IntelOrchestrator(on_result=writer.write)
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._count = 0

    async def write(self, result: ExtractionResult) -> None:
        """Append result as a single JSON line (JSONL)."""
        line = json.dumps(result.to_dict(), ensure_ascii=False)
        async with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
            self._count += 1
        logger.debug("JsonlWriter: wrote %s/%s (%d items) → %s",
                     result.source, result.domain, len(result.items), self._path)

    @property
    def count(self) -> int:
        """Total results written so far."""
        return self._count


def _chain(*callbacks: Callable[[ExtractionResult], Awaitable[None]]) \
        -> Callable[[ExtractionResult], Awaitable[None]]:
    """Combine multiple on_result callbacks into one (called sequentially)."""
    async def _combined(result: ExtractionResult) -> None:
        for cb in callbacks:
            await cb(result)
    return _combined

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

async def main(persist: bool = True, only_domain: str = "", json_path: str = "") -> int:
    """Production daemon: initial sweep + continuous APScheduler loop."""
    sources_filter: set[str] | None = ALL_OK_SOURCES  # default to verified-OK set
    if only_domain:
        sources_filter = set(OK_EXTRACTORS.get(only_domain, []))
        if not sources_filter:
            logger.error("Unknown domain '%s'. Available: %s", only_domain,
                         sorted(OK_EXTRACTORS))
            return 1
        logger.info("Domain filter active: %s (%d sources)",
                    only_domain, len(sources_filter))

    callbacks = []

    store: IntelMongoStore | None = None
    if persist:
        try:
            store = IntelMongoStore()
            await store.ensure_indexes()
            callbacks.append(store.store_result)
            logger.info("MongoDB persistence enabled (intel_items / intel_runs)")
        except Exception as exc:
            logger.warning("MongoDB unavailable — persistence skipped: %s", exc)
            store = None
    else:
        logger.info("Persistence disabled (--no-persist)")

    writer: JsonlWriter | None = None
    if json_path:
        writer = JsonlWriter(json_path)
        callbacks.append(writer.write)
        logger.info("JSONL output enabled → %s", json_path)

    on_result = _chain(*callbacks) if callbacks else None

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
    except asyncio.CancelledError:
        logger.info("Shutdown requested during initial sweep — stopping gracefully")
        stop_event.set()
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
    parser.add_argument(
        "--json", default="",
        metavar="PATH",
        dest="json_path",
        help="Write each ExtractionResult as a JSON line (JSONL) to PATH (append mode)",
    )
    args = parser.parse_args()

    sys.exit(asyncio.run(main(
        persist=args.persist,
        only_domain=args.only,
        json_path=args.json_path,
    )))
