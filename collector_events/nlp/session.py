from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from forex_shared.domain.intel import IntelItem
from forex_shared.domain.oracle import OracleReviewRequest
from forex_shared.logging.get_logger import get_logger
from forex_shared.providers.mq.mq_factory import MQFactory
from forex_shared.providers.mq.topics import IntelTopics
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
        self.oracle_review_topic = config.get("oracle_review_topic", IntelTopics.ORACLE_REVIEW)
        self.oracle_threshold = float(config.get("oracle_threshold", 0.68))
        self.publish_oracle_reviews = self._as_bool(config.get("publish_oracle_reviews", True))
        self.skip_if_already_enriched = bool(config.get("skip_if_already_enriched", True))

        self.processor = processor or EventProcessor()
        self._mq = None
        self._is_running = False
        self.processed_count = 0
        self.published_count = 0
        self.oracle_review_count = 0
        self.oracle_review_error_count = 0
        self.error_count = 0
        self._process_lock = asyncio.Lock()

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
            async with self._process_lock:
                enriched = await asyncio.to_thread(self._process_payload, payload)
            if not enriched:
                return

            domain = str(enriched.get("domain", "social")).lower()
            output_topic = f"{self.output_prefix}.{domain}"
            await self._mq.publish_event(output_topic, enriched)
            self.published_count += 1
            await self._publish_oracle_review_if_needed(enriched)
            log.info(
                "NLP-enriched event id=%s source=%s topic=%s processed=%s published=%s danger_score=%s oracle_review_candidate=%s oracle_review_published=%s",
                enriched.get("id"),
                enriched.get("source"),
                output_topic,
                self.processed_count,
                self.published_count,
                enriched.get("extra", {}).get("danger_score"),
                enriched.get("extra", {}).get("oracle_review_candidate"),
                enriched.get("extra", {}).get("oracle_review_published"),
            )
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

    async def _publish_oracle_review_if_needed(self, enriched: Dict[str, Any]) -> None:
        extra = enriched.get("extra") or {}
        if not self.publish_oracle_reviews or not bool(extra.get("oracle_review_candidate", False)):
            return

        try:
            payload = self._build_oracle_review_payload(enriched)
            ok = await self._mq.publish_event(self.oracle_review_topic, payload)
            extra["oracle_review_published"] = bool(ok)
            extra["oracle_review_topic"] = self.oracle_review_topic
            enriched["extra"] = extra
            if ok:
                self.oracle_review_count += 1
                log.info(
                    "Oracle review requested id=%s topic=%s count=%s danger_score=%s trade_emit_score=%s",
                    enriched.get("id"),
                    self.oracle_review_topic,
                    self.oracle_review_count,
                    extra.get("danger_score"),
                    payload.get("trade_emit_score"),
                )
            else:
                self.oracle_review_error_count += 1
                log.warning("Oracle review publish returned false for event %s", enriched.get("id"))
        except Exception as exc:
            self.oracle_review_error_count += 1
            extra["oracle_review_published"] = False
            extra["oracle_review_error"] = str(exc)
            enriched["extra"] = extra
            log.error("Error publishing Oracle review for event %s: %s", enriched.get("id"), exc)

    def _build_oracle_review_payload(self, enriched: Dict[str, Any]) -> Dict[str, Any]:
        extra = enriched.get("extra") or {}
        danger_score = self._float(extra.get("danger_score"), 0.0)
        attention_score = self._float(extra.get("attention_score"), 0.0)
        trade_emit_score = self._float(extra.get("trade_emit_score"), danger_score)
        scores = self._build_scores(extra, danger_score, attention_score)

        request = OracleReviewRequest(
            trigger_event_id=str(enriched.get("id", "")),
            reason=str(extra.get("oracle_review_reason") or "nlp_oracle_review_candidate"),
            title=str(enriched.get("title", "")),
            body=str(enriched.get("body", "")),
            published_at=str(enriched.get("published_at") or enriched.get("ts") or ""),
            source=str(enriched.get("source", "")),
            domain=str(enriched.get("domain", "social")),
            danger_score_legacy=danger_score,
            risk_bucket=self._risk_bucket(danger_score),
            impact_category=str(extra.get("impact_category", "generic")),
            scores=scores,
            trade_emit_score=trade_emit_score,
            candidate_directives=self._candidate_directives(enriched),
            score_breakdown=self._score_breakdown(enriched),
        )
        return request.to_dict()

    def _build_scores(self, extra: Dict[str, Any], danger_score: float, attention_score: float) -> Dict[str, float]:
        score_keys = [
            "danger_score",
            "attention_score",
            "source_score",
            "source_signal_adjustment",
            "cluster_signal_adjustment",
            "domain_weight",
            "trade_emit_score",
        ]
        scores = {
            key: self._float(extra.get(key), danger_score if key == "danger_score" else 0.0)
            for key in score_keys
            if key in extra or key == "danger_score"
        }
        scores["danger_score_legacy"] = danger_score
        scores["attention_score"] = attention_score
        return scores

    def _candidate_directives(self, enriched: Dict[str, Any]) -> list[Dict[str, Any]]:
        extra = enriched.get("extra") or {}
        directives = extra.get("candidate_directives")
        if isinstance(directives, list):
            return [directive for directive in directives if isinstance(directive, dict)]

        currencies = enriched.get("affected_currencies") or []
        if not isinstance(currencies, list):
            return []

        return [
            {
                "asset": currency,
                "bias": "neutral",
                "confidence": 0.5,
                "reason": "nlp_affected_currency",
            }
            for currency in currencies
            if str(currency).strip()
        ]

    def _score_breakdown(self, enriched: Dict[str, Any]) -> Dict[str, Any]:
        extra = enriched.get("extra") or {}
        cluster_keys = {key: value for key, value in extra.items() if str(key).startswith("cluster_")}
        return {
            "source": {
                "source_score": extra.get("source_score"),
                "source_authenticity_class": extra.get("source_authenticity_class"),
                "source_bias_risk": extra.get("source_bias_risk"),
                "verification_required": extra.get("verification_required"),
            },
            "cluster": cluster_keys,
            "translation": {
                "translation_source": extra.get("translation_source"),
                "has_original_body": bool(extra.get("original_body")),
            },
            "nlp": {
                "features": extra.get("nlp_features", {}),
                "source_signal_adjustment": extra.get("source_signal_adjustment"),
                "cluster_signal_adjustment": extra.get("cluster_signal_adjustment"),
                "attention_score": extra.get("attention_score"),
            },
        }

    def _risk_bucket(self, danger_score: float) -> str:
        if danger_score >= 0.8:
            return "high"
        if danger_score >= 0.6:
            return "elevated"
        return "low"

    def _float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _as_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off", ""}
        return bool(value)

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
            "oracle_review_topic": self.oracle_review_topic,
            "publish_oracle_reviews": self.publish_oracle_reviews,
            "oracle_review_count": self.oracle_review_count,
            "oracle_review_error_count": self.oracle_review_error_count,
            "error_count": self.error_count,
            "oracle_threshold": self.oracle_threshold,
        }
