from datetime import datetime, timezone
from typing import Any, Dict, Optional
from forex_shared.worker_api import BaseStore
from forex_shared.mongo_manager import MongoManager

class OsintTelegramStore(BaseStore):
    """MongoDB store for Telegram sessions and cursors."""
    
    def __init__(self, mongo_manager: Optional[MongoManager] = None):
        super().__init__(mongo_manager)
        self.cursor_collection = "telegram_osint"
        self.session_collection = "telegram"
        self.entity_cache_collection = "telegram_entity_cache"
        self.channel_state_collection = "telegram_channel_state"
        self.fingerprint_collection = "telegram_content_fingerprint"
        self.cluster_collection = "telegram_content_cluster"

    async def ensure_indexes(self) -> None:
        """Create indexes for performance and uniqueness."""
        # Index for cursors
        await self._mongo.async_ensure_indexes(
            self.cursor_collection, 
            [[("key", 1)]]
        )
        # Index for sessions
        await self._mongo.async_ensure_indexes(
            self.session_collection,
            [[("_id", 1)]]
        )
        await self._mongo.async_ensure_indexes(
            self.entity_cache_collection,
            [[("handle", 1)]]
        )
        await self._mongo.async_ensure_indexes(
            self.channel_state_collection,
            [[("handle", 1)]]
        )
        await self._mongo.async_ensure_indexes(
            self.fingerprint_collection,
            [[("_id", 1)]]
        )
        await self._mongo.async_ensure_indexes(
            self.cluster_collection,
            [[("_id", 1)], [("domain", 1), ("last_seen_at", -1)]]
        )

    async def store_item(self, collection: str, item: Any, filter_: Dict[str, Any]) -> Any:
        """Helper to persist items with custom filter."""
        return await self._mongo.async_replace_one(
            collection,
            filter_,
            item,
            upsert=True
        )

    async def get_item(self, collection: str, query: Dict[str, Any]) -> Optional[Any]:
        """Retrieve an item by query."""
        results = await self._mongo.async_find_many(
            collection,
            filter_=query,
            limit=1
        )
        return results[0] if results else None

    async def get_session(self, session_id: str) -> Optional[str]:
        """Load StringSession from 'telegram' collection using _id."""
        doc = await self.get_item(self.session_collection, {"_id": session_id})
        return doc.get("session_string") if doc else None

    async def save_session(self, session_id: str, session_string: str) -> None:
        """Save StringSession to 'telegram' collection using _id."""
        await self.store_item(
            self.session_collection,
            {
                "_id": session_id,
                "session_string": session_string,
                "updated_at": datetime.now(timezone.utc)
            },
            {"_id": session_id}
        )

    async def get_cursor(self, handle: str) -> int:
        """Load last processed message ID for a handle from 'telegram_osint'."""
        doc = await self.get_item(self.cursor_collection, {"key": f"cursor:{handle.lower()}"})
        return doc.get("last_id", 0) if doc else 0

    async def save_cursor(self, handle: str, last_id: int) -> None:
        """Save last processed message ID for a handle to 'telegram_osint'."""
        await self.store_item(
            self.cursor_collection,
            {
                "key": f"cursor:{handle.lower()}",
                "last_id": last_id,
                "updated_at": datetime.now(timezone.utc)
            },
            {"key": f"cursor:{handle.lower()}"}
        )

    async def save_intel_item(self, item: Dict[str, Any]) -> None:
        """Save normalized IntelItem to 'globalintel' collection."""
        await self.store_item(
            "globalintel",
            item,
            {"id": item.get("id")}
        )

    async def get_entity_cache(self, handle: str) -> Optional[Dict[str, Any]]:
        """Load cached Telethon input-entity reference for a handle."""
        return await self.get_item(self.entity_cache_collection, {"handle": handle.lower()})

    async def save_entity_cache(self, handle: str, payload: Dict[str, Any]) -> None:
        """Persist Telethon input-entity reference for a handle."""
        doc = dict(payload)
        doc["handle"] = handle.lower()
        doc["updated_at"] = datetime.now(timezone.utc)
        await self.store_item(
            self.entity_cache_collection,
            doc,
            {"handle": handle.lower()}
        )

    async def get_channel_state(self, handle: str) -> Dict[str, Any]:
        """Load runtime state for a handle."""
        return await self.get_item(self.channel_state_collection, {"handle": handle.lower()}) or {}

    async def save_channel_state(self, handle: str, updates: Dict[str, Any]) -> None:
        """Upsert runtime state for a handle."""
        current = await self.get_channel_state(handle)
        current.update(updates)
        current["handle"] = handle.lower()
        current["updated_at"] = datetime.now(timezone.utc)
        await self.store_item(
            self.channel_state_collection,
            current,
            {"handle": handle.lower()}
        )

    async def get_content_fingerprint(self, fingerprint_key: str) -> Optional[Dict[str, Any]]:
        """Load a stored content fingerprint entry."""
        return await self.get_item(self.fingerprint_collection, {"_id": fingerprint_key})

    async def save_content_fingerprint(self, fingerprint_key: str, updates: Dict[str, Any]) -> None:
        """Upsert a content fingerprint entry."""
        current = await self.get_content_fingerprint(fingerprint_key) or {}
        current.update(updates)
        current["_id"] = fingerprint_key
        current["updated_at"] = datetime.now(timezone.utc)
        await self.store_item(
            self.fingerprint_collection,
            current,
            {"_id": fingerprint_key}
        )

    async def get_content_cluster(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """Load a stored content cluster entry."""
        return await self.get_item(self.cluster_collection, {"_id": cluster_id})

    async def save_content_cluster(self, cluster_id: str, updates: Dict[str, Any]) -> None:
        """Upsert a content cluster entry."""
        current = await self.get_content_cluster(cluster_id) or {}
        current.update(updates)
        current["_id"] = cluster_id
        current["updated_at"] = datetime.now(timezone.utc)
        await self.store_item(
            self.cluster_collection,
            current,
            {"_id": cluster_id}
        )

    async def find_cluster_candidates(
        self,
        domain: str,
        *,
        since_iso: str,
        topic: str,
        routing_family: str,
        limit: int = 40,
    ) -> list[Dict[str, Any]]:
        """Load recent cluster candidates for similarity matching."""
        return await self._mongo.async_find_many(
            self.cluster_collection,
            filter_={
                "domain": domain,
                "last_seen_at": {"$gte": since_iso},
                "$or": [
                    {"topic": topic},
                    {"routing_family": routing_family},
                ],
            },
            sort=[("last_seen_at", -1)],
            limit=limit,
        )
