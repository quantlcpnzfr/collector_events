# services/collector_events/main.py
"""
collector_events — bootstrapper principal do serviço de inteligência global.

Inicializa o pipeline completo:
    1. ``EnvConfigManager.startup()`` — carrega variáveis de ambiente
    2. ``IntelOrchestrator`` com ``GlobalIntelConfig``
    3. Conecta MQ (RabbitMQ async) para publicação de ``IntelItem`` e ``GlobalTag``
    4. Inicia APScheduler com todos os extractors configurados
    5. Aguarda SIGINT/SIGTERM para shutdown gracioso

Uso::

    python -m collector_events.main

Flags::

    --no-mq      Executa sem MQ (apenas Redis/cache) — útil para testes offline
    --run-once   Executa um ciclo completo e sai (útil para CI)
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from forex_shared.config import AppConfig
from forex_shared.env_config_manager import EnvConfigManager
from forex_shared.logging.get_logger import get_logger, setup_logging

setup_logging()
log = get_logger(__name__)


async def run(no_mq: bool = False, run_once: bool = False) -> None:
    from collector_events.globalintel.config_env import GlobalIntelConfig
    from collector_events.globalintel.orchestrator import IntelOrchestrator

    try:
        EnvConfigManager.startup()
    except Exception as exc:
        log.warning("EnvConfigManager.startup() failed (continuing with .env): %s", exc)

    strict = AppConfig.REQUIRED_CONFIG_STRICT
    EnvConfigManager.validate_required(["REDIS_URL"], raise_error=strict)
    if not no_mq:
        EnvConfigManager.validate_required(
            ["MQ_HOST", "MQ_PORT", "MQ_USER", "MQ_PASSWORD"],
            raise_error=strict,
        )

    config = GlobalIntelConfig()
    orchestrator = IntelOrchestrator(config=config)

    # Connect MQ unless --no-mq flag is set
    if not no_mq:
        try:
            await orchestrator.connect_mq()
            log.info("MQ connected — publishing enabled")
        except Exception as exc:
            log.warning("MQ connection failed (running in cache-only mode): %s", exc)

    if run_once:
        log.info("Run-once mode: executing all extractors ...")
        results = await orchestrator.run_all()
        ok = sum(1 for r in results if r.ok)
        log.info("Run-once complete: %d/%d extractors succeeded", ok, len(results))
        await orchestrator.disconnect_mq()
        return

    # Scheduler mode — runs indefinitely
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        log.info("Shutdown signal received — stopping scheduler ...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except (NotImplementedError, RuntimeError):
            # Windows: signal handlers on event loops are not always supported
            pass

    await orchestrator.start_scheduler()
    health = await orchestrator.health()
    log.info(
        "IntelOrchestrator running: %d extractors scheduled",
        len([v for v in health.values() if isinstance(v, dict)]),
    )

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        orchestrator.stop_scheduler()
        await orchestrator.disconnect_mq()
        log.info("collector_events shutdown complete")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="collector_events — Global Intelligence Pipeline",
    )
    parser.add_argument(
        "--no-mq",
        action="store_true",
        help="Run without MQ publishing (cache-only mode)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Execute one full extraction cycle and exit (for CI/testing)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(no_mq=args.no_mq, run_once=args.run_once))
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()

