"""Bootstrapper for OSINT Translation Worker."""
import asyncio
import os
import signal
from pathlib import Path
from forex_shared.env_config_manager import EnvConfigManager
from forex_shared.logging.get_logger import get_logger, setup_logging

from collector_events.translation.worker import TranslationWorker

log = get_logger("TranslationBootstrapper")

async def run_worker():
    setup_logging()
    EnvConfigManager.startup()
    
    worker_id = os.getenv("TRANSLATION_WORKER_ID", "translation-default")
    worker = TranslationWorker(worker_id=worker_id)
    
    await worker.start()
    
    # Auto-create default session for convenience if not disabled
    if os.getenv("DISABLE_AUTO_SESSION", "false").lower() != "true":
        # Default config from env
        config = {
            "session_id": "default",
            "input_topic": os.getenv("TRANSLATION_INPUT_TOPIC", "intel.events.#"),
            "output_prefix": os.getenv("TRANSLATION_OUTPUT_PREFIX", "intel.translated"),
            "translation_provider": os.getenv("TRANSLATION_PROVIDER", "nllb"), # default to nllb
            "detector_model_path": os.getenv("GLOTLID_MODEL_PATH", ".models/translation/glotlid/model.bin"),
            "device": os.getenv("TRANSLATION_DEVICE", "auto"),
        }
        await worker.create_session({"session_id": "default", "config": config})
    
    try:
        await worker.run_forever()
    except (asyncio.CancelledError, KeyboardInterrupt):
        await worker.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
