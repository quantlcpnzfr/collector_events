"""Bootstrapper for the worker-api NLP enrichment worker."""

import asyncio
import os

from forex_shared.env_config_manager import EnvConfigManager
from forex_shared.logging.get_logger import get_logger, setup_logging

from .worker import NLPWorker

log = get_logger("NLPBootstrapper")


async def run_worker() -> None:
    setup_logging()
    EnvConfigManager.startup()

    worker_id = os.getenv("NLP_WORKER_ID", "nlp-default")
    worker = NLPWorker(worker_id=worker_id)
    await worker.start()

    if os.getenv("DISABLE_AUTO_SESSION", "false").lower() != "true":
        config = {
            "session_id": "default",
            "input_topic": os.getenv("NLP_INPUT_TOPIC", "intel.translated.#"),
            "output_prefix": os.getenv("NLP_OUTPUT_PREFIX", "intel.enriched"),
            "oracle_threshold": float(os.getenv("NLP_ORACLE_THRESHOLD", "0.68")),
        }
        await worker.create_session({"session_id": "default", "config": config})

    try:
        await worker.run_forever()
    except (asyncio.CancelledError, KeyboardInterrupt):
        log.info("Shutdown requested for NLP worker.")
        await worker.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
