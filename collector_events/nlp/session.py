from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from forex_shared.domain.intel import IntelItem
from forex_shared.logging.get_logger import get_logger
from forex_shared.providers.mq.mq_factory import MQFactory
from forex_shared.worker_api import BaseSession
from forex_shared.worker_api.contracts import SessionStatus

from collector_events.processors.event_processor import EventProcessor

log = get_logger(__name__)


class NLPEnrichmentSession(BaseSession):
    """
    MQ-driven NLP enrichment session.

    Default production path:
        intel.translated.# -> NLPEnrichmentSession -> intel.enriched.{domain}

    This complements the legacy globalintel orchestrator path without replacing it.
    """

    def __init__(self, config: Dict[str, Any], processor: Optional[EventProcessor] = None):
        self.config = config
        self.session_id = config.get("session_id", "default")
        self.session_name = config.get("session_name", self.session_id)
        self.status = SessionStatus.STARTING.value
        self.metadata = config.get("metadata", {})
        self.kind = "nlp_enrichment"

        self.input_topic = config.get("input_topic", "intel.translated.#")
        self.output_prefix = config.get("output_prefix", "intel.enriched")
        self.oracle_threshold = float(config.get("oracle_threshold", 0.68))
        self.skip_if_already_enriched = bool(config.get("skip_if_already_enriched", True))

        self.processor = processor or EventProcessor()
        self._mq = None
        self._is_running = False
        self.processed_count = 0
        self.published_count = 0
        self.error_count = 0

    async def start(self) -> None:
        if self._is_running:
            return

        self.status = SessionStatus.STARTING.value
        self._mq = MQFactory.create_async_from_env()
        await self._mq.connect()
        await self._mq.subscribe_event(self.input_topic, self._on_event)

        self._is_running = True
        self.status = SessionStatus.RUNNING.value
        asyncio.create_task(self._mq.start_consuming())
        log.info(
            "NLPEnrichmentSession '%s' started. Listening on %s and publishing to %s.<domain>",
            self.session_id,
            self.input_topic,
            self.output_prefix,
        )

    async def stop(self) -> None:
        self._is_running = False
        self.status = SessionStatus.STOPPING.value
        if self._mq:
            await self._mq.stop_consuming()
            await self._mq.disconnect()
            self._mq = None
        self.status = SessionStatus.STOPPED.value
        log.info("NLPEnrichmentSession '%s' stopped.", self.session_id)

    async def _on_event(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        if self.skip_if_already_enriched and str(payload.get("event_type", "")).upper() == "INTEL_ITEM_ENRICHED":
            return

        try:
            enriched = await asyncio.to_thread(self._process_payload, payload)
            if not enriched:
                return

            domain = str(enriched.get("domain", "social")).lower()
            output_topic = f"{self.output_prefix}.{domain}"
            await self._mq.publish_event(output_topic, enriched)
            self.published_count += 1
            log.debug("Published NLP-enriched event to %s", output_topic)
        except Exception as exc:
            self.error_count += 1
            log.error("Error processing NLP enrichment for event %s: %s", payload.get("id"), exc)

    def _process_payload(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        item = IntelItem.from_dict(payload)
        processed = self.processor.process_item(item)
        now = datetime.now(timezone.utc).isoformat()

        if item.extra is None:
            item.extra = {}

        item.extra["nlp_processed"] = True
        item.extra["nlp_processed_at"] = now
        item.extra["oracle_review_candidate"] = self._should_send_to_oracle(item, processed.danger_score)

        enriched = item.to_mq_payload()
        enriched["event_type"] = "INTEL_ITEM_ENRICHED"
        self.processed_count += 1
        return enriched

    def _should_send_to_oracle(self, item: IntelItem, danger_score: float) -> bool:
        extra = item.extra or {}
        send_to_oracle = bool(extra.get("send_to_oracle", extra.get("sendToOracle", True)))
        source_score = float(extra.get("source_score", 0.5) or 0.5)
        cluster_channels = int(extra.get("cluster_channel_count", 1) or 1)
        verification_required = bool(extra.get("verification_required", False))

        adjusted_threshold = self.oracle_threshold
        if verification_required:
            adjusted_threshold += 0.06
        if source_score >= 0.8:
            adjusted_threshold -= 0.04
        if cluster_channels >= 3:
            adjusted_threshold -= 0.04

        adjusted_threshold = max(0.35, min(0.95, adjusted_threshold))
        return send_to_oracle and danger_score >= adjusted_threshold

    def snapshot(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "status": self.status,
            "is_running": self._is_running,
            "input_topic": self.input_topic,
            "output_prefix": self.output_prefix,
            "processed_count": self.processed_count,
            "published_count": self.published_count,
            "error_count": self.error_count,
            "oracle_threshold": self.oracle_threshold,
        }
