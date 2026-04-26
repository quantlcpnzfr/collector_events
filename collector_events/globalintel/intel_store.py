# collector_events/globalintel/intel_store.py
"""
IntelMongoStore — async MongoDB persistence for globalintel extraction results.

Deduplication:
  - ``_id``          → UUID v4 (auto-generated per document)
  - ``fingerprint``  → UUID5 over source|domain|local_key|country|published_date|title
                       Same real-world event from two extractors → same fingerprint
  - ``hash``         → SHA-256 of (title + body): detects content changes → skip redundant writes

Estágio 1 NLP:
  EventProcessor runs keyword matching on every item before storage.
  Enriches ``item.extra["impact_category"]`` and ``item.extra["danger_score"]``.

Collections:
  - ``intel_items``  → one document per unique event (deduplicated by fingerprint)
  - ``intel_runs``   → one document per extraction run (metadata / audit)
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from forex_shared.domain.intel import ExtractionResult, IntelItem
from forex_shared.logging.loggable import Loggable
from forex_shared.mongo_manager import MongoManager

from collector_events.processors.event_processor import EventProcessor, ProcessedEvent

# UUID5 namespace — reuse URL namespace (RFC 4122)
_FINGERPRINT_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


# ── Helpers ───────────────────────────────────────────────────────────────────

def compute_fingerprint(item: IntelItem) -> str:
    """UUID5 over the canonical identity fields of an event.

    Uses the part after ':' in item.id as the local key, the date portion of
    item.ts for day-level granularity, and the first 200 chars of title
    (lowercased) to tolerate minor wording differences while still catching
    the same event from different extractors.
    """
    local_key = item.id.split(":", 1)[1] if ":" in item.id else item.id
    published_date = (item.ts or "")[:10]  # "YYYY-MM-DD" or ""
    key = "|".join([
        (item.source or "").lower(),
        (item.domain or "").lower(),
        local_key.lower(),
        ",".join(sorted(c.lower() for c in item.country)) if item.country else "",
        published_date,
        (item.title or "").lower().strip()[:200],
    ])
    return str(uuid.uuid5(_FINGERPRINT_NS, key))


def compute_hash(item: IntelItem) -> str:
    """SHA-256 of title + body for content-change detection."""
    content = ((item.title or "") + (item.body or "")).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


# ── Stats dataclass ───────────────────────────────────────────────────────────

@dataclass
class StoreStats:
    source: str
    domain: str
    total: int = 0
    new: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0


# ── IntelMongoStore ───────────────────────────────────────────────────────────

class IntelMongoStore(Loggable):
    """Async MongoDB persistence layer for globalintel ExtractionResults.

    Usage::

        store = IntelMongoStore()
        await store.ensure_indexes()          # once at startup
        await store.store_result(result)      # per ExtractionResult

    Or wire as on_result callback::

        orch = IntelOrchestrator(on_result=store.store_result)
    """

    COLLECTION_ITEMS = "intel_items"
    COLLECTION_RUNS  = "intel_runs"

    def __init__(
        self,
        processor: EventProcessor | None = None,
    ) -> None:
        self._mongo = MongoManager()
        self._processor = processor or EventProcessor()

    # ── Index setup ───────────────────────────────────────────────────

    async def ensure_indexes(self) -> None:
        """Idempotent index creation. Call once at service startup."""
        # Regular indexes via async_ensure_indexes
        await self._mongo.async_ensure_indexes(self.COLLECTION_ITEMS, [
            [("domain", 1), ("severity", 1)],
            [("source", 1), ("ts", -1)],
            [("danger_score", -1)],
            [("fetched_at", -1)],
        ])
        # Unique index on fingerprint — wrapped to match MongoManager's WARNING-level behaviour
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self._mongo.get_collection(self.COLLECTION_ITEMS).create_index(
                    [("fingerprint", 1)], unique=True
                ),
            )
        except Exception as exc:
            self.log.warning("IntelMongoStore: create_index(fingerprint) failed: %s", exc)
        await self._mongo.async_ensure_indexes(self.COLLECTION_RUNS, [
            [("source", 1), ("fetched_at", -1)],
        ])
        self.log.debug("IntelMongoStore: indexes ensured")

    # ── Public API ────────────────────────────────────────────────────

    async def store_result(self, result: ExtractionResult) -> StoreStats:
        """Persist all items in an ExtractionResult to MongoDB.

        Skips results with no items (but still logs the run).
        Items are written concurrently via asyncio.gather.
        Returns a StoreStats summary.
        """
        stats = StoreStats(source=result.source, domain=result.domain, total=len(result.items))

        if result.items:
            # Run EventProcessor (Estágio 1 NLP) — process_items returns sorted by danger_score
            processed_events = self._processor.process_items(result.items)
            pe_by_id = {pe.item.id: pe for pe in processed_events}

            async def _upsert(item: IntelItem) -> str:
                pe = pe_by_id.get(item.id) or self._processor.process_item(item)
                return await self._upsert_item(item, pe)

            outcomes = await asyncio.gather(
                *[_upsert(i) for i in result.items],
                return_exceptions=True,
            )
            for outcome in outcomes:
                if isinstance(outcome, Exception):
                    stats.errors += 1
                    self.log.warning("IntelStore upsert error: %s", outcome)
                elif outcome == "new":
                    stats.new += 1
                elif outcome == "updated":
                    stats.updated += 1
                else:
                    stats.skipped += 1

            self.log.info(
                "IntelStore %s/%s: %d new, %d updated, %d skipped, %d errors",
                result.domain, result.source,
                stats.new, stats.updated, stats.skipped, stats.errors,
            )

        await self._log_run(result, stats)
        return stats

    # ── Internal helpers ──────────────────────────────────────────────

    async def _upsert_item(self, item: IntelItem, processed: ProcessedEvent) -> str:
        """Insert or update a single item. Returns 'new', 'updated', or 'skipped'."""
        fingerprint = compute_fingerprint(item)
        new_hash = compute_hash(item)

        existing = await self._mongo.async_find_many(
            self.COLLECTION_ITEMS,
            {"fingerprint": fingerprint},
            {"_id": 1, "hash": 1},
            limit=1,
        )

        now = datetime.now(timezone.utc).isoformat()

        # Existing + same content => nothing to update
        if existing and existing[0].get("hash") == new_hash:
            return "skipped"

        # Existing + changed content => update mutable fields only
        if existing:
            await self._mongo.async_update_one(
                self.COLLECTION_ITEMS,
                {"fingerprint": fingerprint},
                {
                    "$set": self._build_update_fields(item, processed, new_hash, now),
                    "$inc": {"update_count": 1},
                },
            )
            return "updated"

        # New document => atomic upsert.
        # Do NOT send _id. Let Mongo/Cosmos generate native _id.
        insert_doc = self._build_insert_doc(item, processed, fingerprint, new_hash, now)

        try:
            await self._mongo.async_update_one(
                self.COLLECTION_ITEMS,
                {"fingerprint": fingerprint},
                {
                    "$setOnInsert": insert_doc,
                },
                upsert=True,
            )
            return "new"

        except Exception as exc:
            # Race condition guard:
            # if another async task inserted the same fingerprint between find and upsert,
            # retry as skip/update instead of blowing up.
            msg = str(exc).lower()
            if "duplicate key" in msg or "11000" in msg:
                existing_after_race = await self._mongo.async_find_many(
                    self.COLLECTION_ITEMS,
                    {"fingerprint": fingerprint},
                    {"_id": 1, "hash": 1},
                    limit=1,
                )

                if existing_after_race and existing_after_race[0].get("hash") == new_hash:
                    return "skipped"

                await self._mongo.async_update_one(
                    self.COLLECTION_ITEMS,
                    {"fingerprint": fingerprint},
                    {
                        "$set": self._build_update_fields(item, processed, new_hash, now),
                        "$inc": {"update_count": 1},
                    },
                )
                return "updated"

            raise

    @staticmethod
    def _build_doc(
        item: IntelItem,
        processed: ProcessedEvent,
        fingerprint: str,
        hash_val: str,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "_id": str(uuid.uuid4()),
            "item_id": item.id,
            "fingerprint": fingerprint,
            "hash": hash_val,
            "source": item.source,
            "domain": item.domain,
            "title": item.title,
            "url": item.url,
            "body": item.body,
            "ts": item.ts,
            "fetched_at": item.fetched_at,
            "country": item.country,
            "severity": item.severity,
            "tags": item.tags,
            "lat": getattr(item, "lat", None),
            "lon": getattr(item, "lon", None),
            "extra": item.extra,
            "impact_category": processed.impact_category,
            "danger_score": processed.danger_score,
            "matched_keywords": processed.matched_keywords,
            "created_at": now,
            "updated_at": now,
            "update_count": 0,
        }

    async def _log_run(self, result: ExtractionResult, stats: StoreStats) -> None:
        """Append a run-level document to intel_runs for audit/monitoring."""
        now = datetime.now(timezone.utc).isoformat()
        doc = {
            "_id": str(uuid.uuid4()),
            "source": result.source,
            "domain": result.domain,
            "fetched_at": result.fetched_at or now,
            "item_count": len(result.items),
            "new_items": stats.new,
            "updated_items": stats.updated,
            "skipped_items": stats.skipped,
            "error_items": stats.errors,
            "elapsed_ms": round(result.elapsed_ms, 1) if result.elapsed_ms else None,
            "error": result.error,
            "logged_at": now,
        }
        await self._mongo.async_replace_one(
            self.COLLECTION_RUNS, {"_id": doc["_id"]}, doc, upsert=True
        )
