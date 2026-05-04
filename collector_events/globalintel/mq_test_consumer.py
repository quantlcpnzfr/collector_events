"""MQ Test Consumer to verify GlobalTagEmitter payloads over RabbitMQ.

This script boots an MQEventConsumer listening to the `intel.oracle.review` topic.
It serves as the receiver for the `main_test.py` orchestrator run, allowing us to 
verify the exact JSON payloads that `GlobalTagEmitter` produces when running in 
the actual `globalintel` pipeline without dry-run enabled.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict

from forex_shared.env_config_manager import EnvConfigManager
from forex_shared.worker_api.event_consumer import MQEventConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MQTestConsumer")


async def run_consumer() -> None:
    # Ensure environment is loaded
    from dotenv import load_dotenv
    load_dotenv(override=True)
    EnvConfigManager.startup()
    
    received_count = 0
    output_path = Path(__file__).parent / "data" / "received_oracle_requests.jsonl"
    
    # Ensure fresh file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        pass

    async def _message_handler(payload: Dict[str, Any]) -> None:
        nonlocal received_count
        received_count += 1
        
        event_id = payload.get("trigger_event_id", "unknown")
        logger.info(f"Received OracleReviewRequest for event: {event_id}")
        
        # Append to JSONL for inspection
        with open(output_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    # Create consumer bound to the oracle review topic
    consumer = MQEventConsumer(
        topic="intel.oracle.review",
        callback=_message_handler
    )
    
    logger.info("Starting MQTestConsumer...")
    await consumer.start()
    
    logger.info(f"Listening on 'intel.oracle.review'. Saving outputs to {output_path.name}...")
    logger.info("Run `python main_test.py` in another terminal to fire messages.")
    
    try:
        # Keep listening until manually killed or 5 minutes pass
        for _ in range(300):
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info(f"Stopping consumer. Total received: {received_count}")
        await consumer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run_consumer())
    except KeyboardInterrupt:
        logger.info("Exited by user.")
