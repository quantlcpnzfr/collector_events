# globalintel/tagging/tag_manager.py
"""
GlobalTagManager — converts high-severity IntelItems into GlobalTag entries.

Responsibilities:
    1. Receive ``ExtractionResult`` (via ``IntelOrchestrator._run_entry()``)
    2. Filter items by severity threshold
    3. Resolve affected forex asset (via ``AssetResolver``)
    4. Assign bias/risk_score from reference data (optionally refined by ``LLMEnricher``)
    5. Create/update ``GlobalTag`` in Redis with TTL
    6. Publish ``GLOBAL_TAG_UPDATED`` on ``intel.global_tags`` via MQ

Usage::

    from collector_events.globalintel.tagging import GlobalTagManager

    tag_mgr = GlobalTagManager(redis_provider=redis, mq=mq)
    tags = await tag_mgr.process_result(extraction_result)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from forex_shared.domain.intel import (
    ExtractionResult,
    GlobalTag,
    IntelBias,
    IntelItem,
    IntelSeverity,
)
from forex_shared.logging.loggable import Loggable
from forex_shared.providers.cache.redis_provider import RedisProvider
from forex_shared.providers.mq.topics import IntelTopics

from .asset_resolver import AssetResolver
from .llm_enricher import LLMEnricher
from .reference_data import DEFAULT_TTL, DOMAIN_BIAS, DOMAIN_TTL, SEVERITY_SCORE


class GlobalTagManager(Loggable):
    """Converts high-severity intel items into GlobalTag entries (Redis + MQ).

    Called by ``IntelOrchestrator`` after each successful extraction. Items
    meeting the severity threshold are converted into ``GlobalTag`` and stored
    in Redis with a domain-specific TTL.
    """

    def __init__(
        self,
        redis_provider: RedisProvider,
        mq: Any | None = None,
        min_severity: str = IntelSeverity.HIGH,
        llm_enrich: bool = False,
    ) -> None:
        self._r = redis_provider
        self._mq = mq
        self._min_severity = min_severity
        self._llm_enrich = llm_enrich
        self._asset_resolver = AssetResolver()
        self._llm_enricher = LLMEnricher() if llm_enrich else None

    # ── Public API ────────────────────────────────────────────────────

    async def process_result(self, result: ExtractionResult) -> list[GlobalTag]:
        """Process all qualifying items from an ExtractionResult.

        Returns the list of GlobalTag objects that were created/updated.
        """
        if not result.ok or not result.items:
            return []

        tags: list[GlobalTag] = []
        for item in result.items:
            if self._should_tag(item):
                tag = await self._create_tag(item)
                if tag:
                    tags.append(tag)
        return tags

    # ── Internal ──────────────────────────────────────────────────────

    def _should_tag(self, item: IntelItem) -> bool:
        """True if item severity meets the minimum threshold."""
        if self._min_severity == IntelSeverity.HIGH:
            return item.severity == IntelSeverity.HIGH
        if self._min_severity == IntelSeverity.MEDIUM:
            return item.severity in {IntelSeverity.HIGH, IntelSeverity.MEDIUM}
        return True

    async def _create_tag(self, item: IntelItem) -> GlobalTag | None:
        """Create a GlobalTag from an IntelItem, store in Redis, publish to MQ."""
        affected = self._asset_resolver.resolve(item)
        if not affected:
            self.log.debug(
                "GlobalTagManager: no affected assets for %s (%s/%s) — skipping",
                item.id, item.domain, item.source,
            )
            return None

        asset = affected[0]

        bias = str(item.extra.get("bias") or DOMAIN_BIAS.get(item.domain, IntelBias.NEUTRAL))
        risk_score = float(item.extra.get("risk_score") or SEVERITY_SCORE.get(item.severity, 0.5))
        ttl = DOMAIN_TTL.get(item.domain, DEFAULT_TTL)

        # LLM enrichment (optional)
        if self._llm_enricher:
            enriched = await self._llm_enricher.enrich(item, asset, bias, risk_score)
            if enriched:
                bias = enriched.get("bias", bias)
                risk_score = float(enriched.get("risk_score", risk_score))
                if "reasoning" in enriched:
                    item.extra["llm_reasoning"] = enriched["reasoning"]

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl)

        tag = GlobalTag(
            asset=asset,
            bias=bias,
            risk_score=risk_score,
            trigger_event_id=item.id,
            established_at=now.isoformat(),
            expires_at=expires_at.isoformat(),
        )

        redis_key = tag.redis_key()
        await self._r.set_json_raw(redis_key, tag.to_dict(), ttl=ttl)
        self.log.info(
            "GlobalTag stored: %s | bias=%s risk=%.2f ttl=%dh | trigger=%s",
            tag.asset, tag.bias, tag.risk_score, ttl // 3600, item.id,
        )

        if self._mq is not None:
            payload = tag.to_mq_payload("GLOBAL_TAG_UPDATED")
            await self._mq.publish(IntelTopics.GLOBAL_TAGS, payload)
            self.log.debug("GlobalTag published to MQ: %s", tag.asset)

        return tag
