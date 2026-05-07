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
            "detector_model_path": os.getenv("GLOTLID_MODEL_PATH", ".models/lid/lid.176.bin"),
            "translation_model_path": os.getenv("TRANSLATION_MODEL_PATH", ".models/nllb-200-distilled-600M"),
            "device": os.getenv("TRANSLATION_DEVICE", "auto"),
            "max_text_chars": int(os.getenv("TRANSLATION_MAX_TEXT_CHARS", "600")),
            "nllb_max_new_tokens": int(os.getenv("NLLB_MAX_NEW_TOKENS", "96")),
            "nllb_num_beams": int(os.getenv("NLLB_NUM_BEAMS", "1")),
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
