from __future__ import annotations

import asyncio
import copy
import re
from typing import Any, Dict, Optional, Mapping

from forex_shared.worker_api import BaseSession
from forex_shared.worker_api.contracts import SessionStatus
from forex_shared.logging.get_logger import get_logger
from forex_shared.providers.mq.mq_factory import MQFactory

from .engine import TranslationEngine

log = get_logger(__name__)

class TranslationSession(BaseSession):
    """
    Session that consumes OSINT events, translates them, and republishes.
    """

    def __init__(self, config: Dict[str, Any], engine: Optional[TranslationEngine] = None):
        self.config = config
        self.session_id = config.get("session_id", "default")
        self.session_name = config.get("session_name", self.session_id)
        self.status = SessionStatus.STARTING.value
        self.metadata = config.get("metadata", {})
        self.kind = "translation"
        
        self.input_topic = config.get("input_topic", "intel.events.#")
        self.output_prefix = config.get("output_prefix", "intel.translated")
        self.min_confidence = float(config.get("min_detection_confidence", 0.35))
        
        # Engine can be shared across sessions to save memory
        self.engine = engine or TranslationEngine(config)
        self._mq = None
        self._is_running = False

    async def start(self) -> None:
        """Initialize MQ and start consuming."""
        if self._is_running:
            return
            
        self.status = SessionStatus.STARTING.value
        self.engine.load()

        self._mq = MQFactory.create_async_from_env()
        await self._mq.subscribe_event(self.input_topic, self._on_event)

        self._is_running = True
        self.status = SessionStatus.RUNNING.value
        # Note: start_consuming is usually called by the worker or in a task
        asyncio.create_task(self._mq.start_consuming())
        log.info(f"TranslationSession '{self.session_id}' started. Listening on {self.input_topic}")

    async def stop(self) -> None:
        """Stop MQ consumption and disconnect."""
        self._is_running = False
        self.status = SessionStatus.STOPPING.value
        if self._mq:
            await self._mq.stop_consuming()
            await self._mq.disconnect()
            self._mq = None
        self.status = SessionStatus.STOPPED.value
        log.info(f"TranslationSession '{self.session_id}' stopped.")

    async def _on_event(self, payload: Any) -> None:
        """Process an incoming event."""
        if not isinstance(payload, dict):
            return

        # Avoid loops: don't process already translated items
        if str(payload.get("event_type", "")).upper() == "INTEL_ITEM_TRANSLATED":
            return

        try:
            # Process in thread pool to not block asyncio loop
            enriched = await asyncio.to_thread(self._process_payload, payload)
            
            if enriched:
                # Publish enriched event
                domain = str(enriched.get("domain", "social")).lower()
                output_topic = f"{self.output_prefix}.{domain}"
                await self._mq.publish_event(output_topic, enriched)
                log.debug(f"Published translated event to {output_topic}")
                
        except Exception as e:
            log.error(f"Error processing translation for event {payload.get('id')}: {e}")

    def _process_payload(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Synchronous part of the processing (runs in thread)."""
        enriched = copy.deepcopy(payload)
        
        # Extract text to translate
        title = str(enriched.get("title", "")).strip()
        body = str(enriched.get("body", "")).strip()
        
        sample = (title + "\n" + body).strip()
        if not sample:
            return None

        # Detection
        detection = self.engine.detect_language(sample)

        # Decide if translation is needed
        is_english = detection.language == "eng_Latn" or detection.language in {"en", "eng"}
        should_translate = not is_english and detection.confidence >= self.min_confidence

        # Get raw body for storage
        raw_body = str(payload.get("body", ""))

        if "extra" not in enriched or not isinstance(enriched.get("extra"), dict):
            enriched["extra"] = {}

        if should_translate:
            trans_title, t_status, t_error = self.engine.translate(title, detection.language)
            trans_body, b_status, b_error = self.engine.translate(body, detection.language)
            title_translated = t_status == "translated" and bool(trans_title)
            body_translated = b_status == "translated" and bool(trans_body)

            # Store original body only when translation was actually needed.
            enriched["extra"]["original_body"] = raw_body

            # Overwrite original fields with translated text
            enriched["title"] = self._clean_title(trans_title if title_translated else title)
            enriched["body"] = trans_body if body_translated else body

            translation_source = f"⚡Translation from {detection.language_name} ({detection.language})"
        else:
            if is_english:
                # Clean title even if already English
                enriched["title"] = self._clean_title(title)
                enriched["body"] = body
                translation_source = "➡️No translation needed"
                enriched["extra"]["original_body"] = ""
            else:
                # Not English, but maybe confidence too low?
                # Just clean existing title
                enriched["title"] = self._clean_title(title)
                translation_source = f"⚠️ Detection: {detection.language_name} ({detection.language}) - No translation"
                enriched["extra"]["original_body"] = raw_body
        enriched["extra"]["translation_source"] = translation_source

        return enriched

    def _clean_title(self, text: str, max_length: int = 200) -> str:
        """Process title to be a single compact line."""
        if not text:
            return ""
        # Remove newlines and tabs
        text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        # Truncate
        if len(text) > max_length:
            text = text[:max_length-3] + "..."
        return text

    def snapshot(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "status": self.status,
            "is_running": self._is_running,
            "input_topic": self.input_topic,
            "provider": self.engine.translation_provider,
            "target_language": self.engine.target_language
        }
